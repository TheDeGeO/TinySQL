using Entities;
using System;
using System.IO;

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
        private static string DataPath = Path.Combine(DatabaseBasePath, "Data");
        private static readonly string SystemCatalogPath = Path.Combine(DataPath, "SystemCatalog");
        private static readonly string SystemDatabasesFile = Path.Combine(SystemCatalogPath, "SystemDatabases.table");
        private static readonly string SystemTablesFile = Path.Combine(SystemCatalogPath, "SystemTables.table");

        private Store()
        {
            InitializeSystemCatalog();
        }

        private void InitializeSystemCatalog()
        {
            Directory.CreateDirectory(SystemCatalogPath);
            if (!File.Exists(SystemDatabasesFile))
            {
                File.Create(SystemDatabasesFile).Close();
            }
            if (!File.Exists(SystemTablesFile))
            {
                File.Create(SystemTablesFile).Close();
            }
        }

        public OperationStatus SetDatabase(string databaseName)
        {
            if (string.IsNullOrWhiteSpace(databaseName))
            {
                return OperationStatus.Error;
            }

            string databasePath = Path.Combine(DatabaseBasePath, databaseName);
            if (!Directory.Exists(databasePath))
            {
                return OperationStatus.Error;
            }

            DataPath = databasePath;
            return OperationStatus.Success;
        }

        public OperationStatus CreateDatabase(string databaseName)
        {
            if (string.IsNullOrWhiteSpace(databaseName))
            {
                return OperationStatus.Error;
            }

            string databasePath = Path.Combine(DatabaseBasePath, databaseName);
            if (Directory.Exists(databasePath))
            {
                return OperationStatus.Error;
            }

            Directory.CreateDirectory(databasePath);
            
            // Add the new database to SystemDatabases.table
            using (StreamWriter writer = File.AppendText(SystemDatabasesFile))
            {
                writer.WriteLine(databaseName);
            }

            return OperationStatus.Success;
        }

        public OperationStatus CreateTable(string tableName, List<(string Name, Type Type)> columns)
        {
            if (string.IsNullOrWhiteSpace(tableName) || columns == null || columns.Count == 0)
            {
                return OperationStatus.Error;
            }

            string tablePath = Path.Combine(DataPath, $"{tableName}.table");
            if (File.Exists(tablePath))
            {
                return OperationStatus.Error;
            }

            using (StreamWriter writer = new StreamWriter(tablePath))
            {
                foreach (var column in columns)
                {
                    writer.WriteLine($"{column.Name},{column.Type}");
                }
            }

            // Add the new table to SystemTables.table
            using (StreamWriter writer = File.AppendText(SystemTablesFile))
            {
                writer.WriteLine($"{Path.GetFileName(DataPath)},{tableName}");
            }

            return OperationStatus.Success;
        }

        public OperationStatus Select(string tableName)
        {
            string tablePath = Path.Combine(DataPath, $"{tableName}.table");
            if (!File.Exists(tablePath))
            {
                return OperationStatus.Error;
            }

            using (StreamReader reader = new StreamReader(tablePath))
            {
                string? line;
                while ((line = reader.ReadLine()) != null)
                {
                    Console.WriteLine(line);
                }
            }

            return OperationStatus.Success;
        }

        public OperationStatus ShowDatabases()
        {
            if (!Directory.Exists(DatabaseBasePath))
            {
                return OperationStatus.Error;
            }

            string[] databases = Directory.GetDirectories(DatabaseBasePath);
            foreach (var database in databases)
            {
                Console.WriteLine(Path.GetFileName(database));
            }

            return OperationStatus.Success;
        }
    }
}
