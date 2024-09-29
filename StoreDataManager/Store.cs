using Entities;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;

namespace StoreDataManager
{
    public sealed class Store
    {
        private static Store? instance = null;
        private static readonly object _lock = new object();
               
        public static Store GetInstance()
        {
            lock(_lock)
            {
                if (instance == null) 
                {
                    instance = new Store();
                }
                return instance;
            }
        }
        private static readonly string DatabaseBasePath = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), "TinySQL");
        private static readonly string DataPath = Path.Combine(DatabaseBasePath, "Data");
        private static readonly string SystemCatalogPath = Path.Combine(DataPath, "SystemCatalog");
        private static readonly string SystemDatabasesFile = Path.Combine(SystemCatalogPath, "SystemDatabases.table");
        private static readonly string SystemTablesFile = Path.Combine(SystemCatalogPath, "SystemTables.table");
        public Store()
        {
            this.InitializeSystemCatalog();
            
        }

        private void InitializeSystemCatalog()
        {
            // Always make sure that the system catalog and above folder
            // exist when initializing
            Directory.CreateDirectory(SystemCatalogPath);
        }

        public OperationStatus CreateTable()
        {
            // Creates a default DB called TESTDB
            Directory.CreateDirectory($@"{DataPath}/TESTDB");

            // Creates a default Table called ESTUDIANTES
            var tablePath = $@"{DataPath}/TESTDB/ESTUDIANTES.Table";

            using (FileStream stream = File.Open(tablePath, FileMode.OpenOrCreate))
            using (BinaryWriter writer = new (stream))
            {
                // Create an object with a hardcoded.
                // First field is an int, second field is a string of size 30,
                // third is a string of 50
                int id = 1;
                string nombre = "Isaac".PadRight(30); // Pad to make the size of the string fixed
                string apellido = "Ramirez".PadRight(50);

                writer.Write(id);
                writer.Write(nombre);
                writer.Write(apellido);
            }
            return OperationStatus.Success;
        }

        public OperationStatus Select()
        {
            // Creates a default Table called ESTUDIANTES
            var tablePath = $@"{DataPath}/TESTDB/ESTUDIANTES.Table";
            using (FileStream stream = File.Open(tablePath, FileMode.OpenOrCreate))
            using (BinaryReader reader = new (stream))
            {
                // Print the values as a I know exactly the types, but this needs to be done right
                Console.WriteLine(reader.ReadInt32());
                Console.WriteLine(reader.ReadString());
                Console.WriteLine(reader.ReadString());
                return OperationStatus.Success;
            }
        }
    }
}
