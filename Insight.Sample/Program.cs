using System;
using System.Collections.Generic;
using System.Linq;
using System.Data.SqlClient;
using System.Data;
using System.Threading.Tasks;
using System.Data.Common;
using System.Reflection;
using System.Reflection.Emit;
using System.Runtime.CompilerServices;

#pragma warning disable 0649

[assembly: InternalsVisibleTo("Insight.Database.DynamicAssembly")]

namespace Insight.Database.Sample
{
    public interface IRepository
    {
    }

    public abstract class Repository
    {
        public const string Version = "_2";
    }

    public interface IBeerRepository : IRepository
    {
        IList<Beer> FindBeers(string name);
    }

    public static class ConnectionExtensions
    {
        public static T AsVersionedRepo<T>(this IDbConnection conn)
        {
            // var type = Program.NewTypes[$"{typeof(T).Name.TrimStart('I')}_Proxy"];
            var type = Program.NewTypes[$"{typeof(T).Name}_Proxy"];
            dynamic repo = conn.As(type);
            return (T) repo;
        }
    }

    public class Program
    {
        public static readonly string connectionString =
            "Data Source = .; Initial Catalog = InsightDbSample; Integrated Security = true";

        private static SqlConnectionStringBuilder Database = new SqlConnectionStringBuilder(connectionString);


        static void Main(string[] args)
        {
            // Registering a provider is usually not necessary
            // SqlInsightDbProvider.RegisterProvider();

            BuildAssembly();

            using (IDbConnection connection = Database.Open())
            {
                var results = connection.AsVersionedRepo<IBeerRepository>().FindBeers("HopDevil");
                foreach (var beer in results)
                {
                    Console.WriteLine($"{beer.Id} {beer.Name} {beer.Details}");
                }
            }
        }


        public static readonly Dictionary<string, Type> NewTypes = new Dictionary<string, Type>();

        private static void BuildAssembly()
        {
            // Build a namespace and assembly to put proxy implementations
            var dynamicNamespace = new AssemblyName("VersionedRepositories");
            var assemblyBuilder =
                AppDomain.CurrentDomain.DefineDynamicAssembly(dynamicNamespace, AssemblyBuilderAccess.Run);
            var moduleBuilder = assemblyBuilder.DefineDynamicModule(dynamicNamespace.Name);


            var types = Assembly.GetExecutingAssembly().GetTypes();

            // Find all IRepository implementations
            var interfaces = types
                .Where(t => t.IsInterface && typeof(IRepository).IsAssignableFrom(t) && t != typeof(IRepository))
                .ToList();

            // For each abstract repository create a proxy with appropriate SqlAttributes
            foreach (var repository in interfaces)
            {
                var name = $"{repository.Name}_Proxy";
                var typeBuilder = moduleBuilder.DefineType(name,
                    TypeAttributes.Public | TypeAttributes.Interface | TypeAttributes.Abstract);

                // Attach the matching interface for more type safety. Without this we have to fall back to dynamic.
                typeBuilder.AddInterfaceImplementation(interfaces.First(i => i.Name.EndsWith(repository.Name)));
                foreach (var method in repository.GetMethods())
                {
                    CreateMethod(method, typeBuilder);
                }

                // Register the type
                var newType = typeBuilder.CreateType();
                NewTypes[name] = newType;
            }
        }

        private static void CreateMethod(MethodInfo method, TypeBuilder typeBuilder)
        {
            // Create the attribute and concatenate the version identifier
            var attrCtorParams = new[] {typeof(string)};
            var attrCtorInfo = typeof(SqlAttribute).GetConstructor(attrCtorParams);
            var attrBuilder =
                new CustomAttributeBuilder(attrCtorInfo, new object[] {$"{method.Name}{Repository.Version}"});

            // Create a copy of the method from the original class and add the attribute
            var methodBuilder = typeBuilder.DefineMethod(method.Name,
                 MethodAttributes.Public | MethodAttributes.Abstract | MethodAttributes.Virtual,
                method.ReturnType, method.GetParameters().Select(i => i.ParameterType).ToArray());
            methodBuilder.SetCustomAttribute(attrBuilder);

            // Copy the parameters over, including their names so that Insight can still do its mapping
            var parameters = method.GetParameters();
            for (var index = 0; index < parameters.Length; index++)
            {
                var parameter = parameters[index];
                // parameter at index 0 is the return value...
                methodBuilder.DefineParameter(index + 1, parameter.Attributes, parameter.Name);
            }
        }

        class Glass
        {
            public int Id;
            public string Name;
            public int Ounces;
        }

        class Serving
        {
            public int ID;
            public DateTime When;

            public int BeerID
            {
                get { return Beer.Id; }
            }

            public Beer Beer;

            public int GlassesID
            {
                get { return Glass.Id; }
            }

            public Glass Glass;
        }

        #region Opening Connections

        static void IDBConnection_OpenConnection()
        {
            // open the connection and return it
            using (SqlConnection c = new SqlConnection(connectionString).OpenConnection())
            {
                c.QuerySql("SELECT * FROM Beer", Parameters.Empty);
            }
        }

        static void SqlConnectionStringBuilder_Connection()
        {
            SqlConnectionStringBuilder database = new SqlConnectionStringBuilder(connectionString);
            // make other changes here

            // run a query right off the connection (this performs an auto-open/close)
            database.Connection().QuerySql("SELECT * FROM Beer", Parameters.Empty);
        }

        static void SqlConnectionStringBuilder_Open()
        {
            SqlConnectionStringBuilder database = new SqlConnectionStringBuilder(connectionString);
            // make other changes here

            // manage the lifetime ourselves
            using (IDbConnection c = database.Open())
            {
                c.QuerySql("SELECT * FROM Beer", Parameters.Empty);
            }
        }

        static void SqlConnectionStringBuilder_Open2()
        {
            SqlConnection database = new SqlConnection(connectionString);
            // make other changes here

            // manage the lifetime ourselves
            using (IDbConnection c = database.OpenWithTransaction("test"))
            {
                c.QuerySql("SELECT * FROM Beer", Parameters.Empty);
            }
        }

        #endregion

        #region Executing Database Commands

        static void Execute()
        {
            Beer beer = new Beer() {Name = "IPA"};

            // map a beer the stored procedure parameters
            Database.Connection().Insert("InsertBeer", beer);

            // map an anonymous object to the stored procedure parameters
            Database.Connection().Execute("DeleteBeer", beer);
        }

        static void ExecuteSql()
        {
            Beer beer = new Beer() {Name = "IPA"};

            // map a beer the stored procedure parameters
            Database.Connection().ExecuteSql("INSERT INTO Beer (Name) VALUES (@Name)", beer);

            // map an anonymous object to the stored procedure parameters
            Database.Connection().ExecuteSql("DELETE FROM Beer WHERE Name = @Name", new {Name = "IPA"});
        }

        static void ExecuteScalar()
        {
            int count = Database.Connection().ExecuteScalar<int>("CountBeer", new {Name = "IPA"});

            int count2 = Database.Connection()
                .ExecuteScalarSql<int>("SELECT COUNT(*) FROM Beer WHERE Name LIKE @Name", new {Name = "IPA"});
        }

        #endregion

        #region Creating Commands

        static void IDbConnection_CreateCommand()
        {
            using (IDbConnection connection = Database.Open())
            {
                // IDbCommand command = connection.CreateCommand("FindBeers", new { Name = "IPA" });
                dynamic repo = connection.As(NewTypes["BeerRepository_Proxy"]);
                var results = repo.FindBeers("HopDevil");
                foreach (var beer in results)
                {
                    Console.WriteLine($"{beer.Id} {beer.Name} {beer.Details}");
                }
            }
        }

        static void IDbConnection_CreateCommandSql()
        {
            using (IDbConnection connection = Database.Open())
            {
                IDbCommand command =
                    connection.CreateCommandSql("SELECT * FROM Beer WHERE Name = @Name", new {Name = "IPA"});
            }
        }

        #endregion

        #region Common Method Parameters

        static void CommonParameter_Transaction()
        {
            using (var connection = Database.Connection().OpenWithTransaction())
            {
                Beer beer = new Beer("Sly Fox IPA");
                connection.Execute("InsertBeer", beer);

                // without a commit this rolls back
            }
        }

        #endregion

        #region Manual Transformations

        static void ManualTransform()
        {
            List<Beer> beer = Database.Connection().Query(
                "FindBeers",
                new {Name = "IPA"},
                reader =>
                {
                    List<Beer> b = new List<Beer>();

                    while (reader.Read())
                    {
                        b.Add(new Beer(reader["Name"].ToString()));
                    }

                    return b;
                });
        }

        static void ManualTransform_Sum()
        {
            int totalNameLength = Database.Connection().Query(
                "FindBeers",
                new {Name = "IPA"},
                reader =>
                {
                    int total = 0;
                    while (reader.Read())
                    {
                        total += reader["Name"].ToString().Length;
                    }

                    return total;
                });

            Console.WriteLine(totalNameLength);
        }

        static void ManualTransform_GetReader()
        {
            using (IDbConnection connection = Database.Open())
            using (IDataReader reader = connection.GetReader("FindBeers", new {Name = "IPA"}))
            {
                while (reader.Read())
                {
                    // do stuff with the reader here
                }
            }
        }

        static void Async_ManualTransform_GetReader()
        {
            using (IDbConnection connection = Database.Open())
            using (DbDataReader reader = connection.GetReaderAsync("FindBeers", new {Name = "IPA"}).Result)
            {
                while (reader.ReadAsync().Result)
                {
                }
            }
        }

        #endregion

        #region Querying for Objects

        static void Query_Query()
        {
            IList<Beer> beer = Database.Connection().Query<Beer>("FindBeers", new {Name = "IPA"});
        }

        static void Query_QuerySql()
        {
            IList<Beer> beer = Database.Connection()
                .QuerySql<Beer>("SELECT * FROM Beer WHERE Name = @Name", new {Name = "IPA"});
        }

        static void Query_ToList()
        {
            using (IDbConnection connection = Database.Open())
            using (IDataReader reader = connection.GetReader("FindBeers", new {Name = "IPA"}))
            {
                IList<Beer> beer = reader.ToList<Beer>();
            }
        }

        static void Query_AsEnumerable()
        {
            using (IDbConnection connection = Database.Open())
            using (IDataReader reader = connection.GetReader("FindBeers", new {Name = "IPA"}))
            {
                foreach (Beer beer in reader.AsEnumerable<Beer>())
                {
                    // drink?
                }
            }
        }

        #endregion

        #region Insert

        static void Insert_Sql()
        {
            Beer beer = new Beer()
            {
                Name = "HopDevil",
                Style = "Hoppy"
            };

            var insertedBeer = Database.Connection()
                .InsertSql("INSERT INTO Beer (Name, Style) VALUES (@Name, @Style) SELECT SCOPE_IDENTITY() AS [Id]",
                    beer);
        }

        #endregion

        #region Dynamic Objects

        static void Dynamic_Query()
        {
            Beer ipa = new Beer() {Name = "IPA"};
            Database.Connection().ExecuteSql("INSERT INTO Beer (Name) VALUES (@Name)", ipa);

            foreach (dynamic beer in Database.Connection()
                .QuerySql("SELECT * FROM Beer WHERE Name = @Name", new {Name = "IPA"}))
            {
                beer.Style = "crisp";

                // extension methods cannot be dynamically dispatched, so we have to cast it to object
                // good news: it still works
                Database.Connection().ExecuteSql("UPDATE Beer Set Style = @Style WHERE Name = @Name", (object) beer);
            }

            Database.Connection().ExecuteSql("DELETE FROM Beer WHERE Name = @Name", ipa);
        }

        static void Dynamic_ForEach()
        {
            Beer ipa = new Beer() {Name = "IPA"};
            Database.Connection().ExecuteSql("INSERT INTO Beer (Name) VALUES (@Name)", ipa);

            Database.Connection().ForEachDynamicSql("SELECT * FROM Beer WHERE Name = @Name", new {Name = "IPA"},
                beer =>
                {
                    beer.Style = "crisp";

                    // extension methods cannot be dynamically dispatched, so we have to cast it to object
                    // good news: it still works
                    Database.Connection()
                        .ExecuteSql("UPDATE Beer Set Style = @Style WHERE Name = @Name", (object) beer);
                });

            Database.Connection().ExecuteSql("DELETE FROM Beer WHERE Name = @Name", ipa);
        }

        #endregion

        #region Dynamic Database Calls

        static void DynamicCall_Named()
        {
            IList<Beer> beer = Database.Connection().Dynamic<Beer>().FindBeers(name: "IPA");
        }

        static void DynamicCall_Transaction()
        {
            using (var connection = Database.Connection().OpenWithTransaction())
            {
                IList<Beer> beer = connection.Dynamic<Beer>().FindBeers(name: "IPA");
            }
        }

        #endregion

        #region Lists of Objects

        static void List_ValueTypeSql()
        {
            IEnumerable<String> names = new List<String>() {"Sly Fox IPA", "Hoppapotamus"};
            var beer = Database.Connection().QuerySql("SELECT * FROM Beer WHERE Name IN (@Name)", new {Name = names});

            names = new string[] {"Sly Fox IPA", "Hoppapotamus"};
            beer = Database.Connection().QuerySql("SELECT * FROM Beer WHERE Name IN (@Name)", new {Name = names});
        }

        static void List_ClassSql()
        {
            List<Beer> beer = new List<Beer>();
            beer.Add(new Beer() {Name = "Sly Fox IPA", Style = "yummy"});
            beer.Add(new Beer() {Name = "Hoppopotamus", Style = "hoppy"});

            Database.Connection().ExecuteSql("INSERT INTO Beer (Name, Style) SELECT Name, Style FROM @Beer",
                new {Beer = beer});
        }

        #endregion

        #region Async

        static void Async_Execute()
        {
            Beer beer = new Beer() {Name = "Sly Fox IPA"};

            Task task = Database.Connection().ExecuteAsync("InsertBeer", beer);

            // do stuff

            task.Wait();
        }

        static void Async_Query()
        {
            Task<IList<Beer>> task = Database.Connection().QueryAsync<Beer>("FindBeers", new {Name = "IPA"});

            // do stuff

            var results = task.Result;

            foreach (Beer b in results)
                Console.WriteLine(b.Name);
        }

        #endregion

        #region Bulk Copy

        static void BulkCopy()
        {
            List<Beer> beer = new List<Beer>();
            beer.Add(new Beer() {Name = "Sly Fox IPA", Flavor = "yummy", OriginalGravity = 4.2m});
            beer.Add(new Beer() {Name = "Hoppopotamus", Flavor = "hoppy", OriginalGravity = 3.0m});

            Database.Connection().BulkCopy("Beer", beer);
        }

        #endregion

        #region Bulk Copy Async

        static void Async_BulkCopy()
        {
            List<Beer> beer = new List<Beer>();
            beer.Add(new Beer() {Name = "Sly Fox IPA", Flavor = "yummy", OriginalGravity = 4.2m});
            beer.Add(new Beer() {Name = "Hoppopotamus", Flavor = "hoppy", OriginalGravity = 3.0m});

            Database.Connection().BulkCopyAsync("Beer", beer);
        }

        #endregion

        #region Expando Expansion

        static void Expando_Expand()
        {
            Beer beer = new Beer() {Name = "Sly Fox IPA"};
            Glass glass = new Glass() {Ounces = 32};

            // create an expando and combine the objects
            FastExpando x = beer.Expand();
            x.Expand(glass);

            // look! a dynamic object
            dynamic d = x;
            Console.WriteLine("{0}", d.Name);
            Console.WriteLine("{0}", d.Ounces);
        }

        #endregion

        #region Expando Mutations

        static void Expando_Mutate()
        {
            Beer ipa = new Beer() {Name = "IPA"};
            Database.Connection().ExecuteSql("INSERT INTO Beer (Name) VALUES (@Name)", ipa);

            var mapping = new Dictionary<string, string>() {{"Name", "TheName"}};
            dynamic beer = Database.Connection().QuerySql("SELECT * FROM Beer WHERE Name = @Name", new {Name = "IPA"})
                .First();
            beer.Mutate(mapping);
            Console.WriteLine(beer.TheName);

            Database.Connection().ExecuteSql("DELETE FROM Beer WHERE Name = @Name", ipa);
        }

        static void Expando_Transform()
        {
            Beer ipa = new Beer() {Name = "IPA"};
            Database.Connection().ExecuteSql("INSERT INTO Beer (Name) VALUES (@Name)", ipa);

            var mapping = new Dictionary<string, string>() {{"Name", "TheName"}};
            dynamic beer = Database.Connection().QuerySql("SELECT * FROM Beer WHERE Name = @Name", new {Name = "IPA"})
                .First().Transform(mapping);
            Console.WriteLine(beer.TheName);

            Database.Connection().ExecuteSql("DELETE FROM Beer WHERE Name = @Name", ipa);
        }

        static void Expando_TransformList()
        {
            Beer ipa = new Beer() {Name = "IPA"};
            Database.Connection().ExecuteSql("INSERT INTO Beer (Name) VALUES (@Name)", ipa);

            var mapping = new Dictionary<string, string>() {{"Name", "TheName"}};
            foreach (dynamic beer in Database.Connection()
                .QuerySql("SELECT * FROM Beer WHERE Name = @Name", new {Name = "IPA"}).Transform(mapping))
                Console.WriteLine(beer.TheName);

            Database.Connection().ExecuteSql("DELETE FROM Beer WHERE Name = @Name", ipa);
        }

        #endregion

        #region ForEach

        static void ForEach()
        {
            Database.Connection().ForEach<Beer>(
                "FindBeers",
                new {Name = "IPA"},
                beer => Drink(beer));
        }

        static void AsEnumerable()
        {
            using (IDbConnection connection = Database.Open())
            using (var reader = connection.GetReaderSql("SELECT * FROM Beer", Parameters.Empty))
            {
                foreach (Beer beer in reader.AsEnumerable<Beer>())
                {
                    Drink(beer);
                }
            }
        }

        static void Drink(Beer b)
        {
            Console.WriteLine("YUM");
        }

        #endregion
    }
}