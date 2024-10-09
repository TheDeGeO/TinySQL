using Entities;
using System;
using System.IO;
using System.Collections.Generic;

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

        private static readonly string DatabaseBasePath = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), "TinySQL/Data");
        private static string DataPath = Path.Combine(DatabaseBasePath, "TESTDB");
        private static string SystemCatalogPath = Path.Combine(DataPath, "SystemCatalog");
        private static string SystemDatabasesFile = Path.Combine(SystemCatalogPath, "SystemDatabases.table");
        private static string SystemTablesFile = Path.Combine(SystemCatalogPath, "SystemTables.table");

        private Dictionary<string, Dictionary<string, Index>> tableIndices = new Dictionary<string, Dictionary<string, Index>>();

        private Store()
        {
            InitializeSystemCatalog();
            LoadIndices();
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
            SystemCatalogPath = Path.Combine(DataPath, "SystemCatalog");
            SystemDatabasesFile = Path.Combine(SystemCatalogPath, "SystemDatabases.table");
            SystemTablesFile = Path.Combine(SystemCatalogPath, "SystemTables.table");
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
            try
            {
                if (string.IsNullOrWhiteSpace(tableName) || columns == null || columns.Count == 0)
                {
                    Console.WriteLine("Invalid table name or columns");
                    return OperationStatus.Error;
                }

                string tablePath = Path.Combine(DataPath, $"{tableName}.table");
                if (File.Exists(tablePath))
                {
                    Console.WriteLine($"Table {tableName} already exists");
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

                Console.WriteLine($"Table {tableName} created successfully");

                // After creating the table, check if any columns have indices and update them
                if (tableIndices.ContainsKey(tableName))
                {
                    foreach (var indexedColumn in tableIndices[tableName])
                    {
                        string columnName = indexedColumn.Key;
                        Index index = indexedColumn.Value;

                        // Read the table data and update the index
                        using (StreamReader reader = new StreamReader(tablePath))
                        {
                            string? headerLine = reader.ReadLine();
                            if (headerLine != null)
                            {
                                string[] headers = headerLine.Split(',');
                                int columnIndex = Array.IndexOf(headers, columnName);
                                if (columnIndex != -1)
                                {
                                    int rowIndex = 0;
                                    string? dataLine;
                                    while ((dataLine = reader.ReadLine()) != null)
                                    {
                                        string[] values = dataLine.Split(',');
                                        if (columnIndex < values.Length)
                                        {
                                            string value = values[columnIndex];
                                            if (index.Search(value) != -1)
                                            {
                                                Console.WriteLine($"Duplicate value found in indexed column {columnName}. Cannot insert row.");
                                                continue;
                                            }
                                            index.Insert(value, rowIndex);
                                        }
                                        rowIndex++;
                                    }
                                }
                            }
                        }
                    }
                }

                return OperationStatus.Success;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error creating table: {ex.Message}");
                return OperationStatus.Error;
            }
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
                Console.WriteLine("No databases found.");
                return OperationStatus.Success;
            }

            string[] databases = Directory.GetDirectories(DatabaseBasePath);
            foreach (var database in databases)
            {
                string databaseName = Path.GetFileName(database);
                if (databaseName != "SystemCatalog")
                {
                    Console.WriteLine(databaseName);
                }
            }

            return OperationStatus.Success;
        }

        public OperationStatus ShowTables()
        {
            if (!File.Exists(SystemTablesFile))
            {
                Console.WriteLine("No tables found in the current database.");
                return OperationStatus.Success;
            }

            bool tablesFound = false;
            using (StreamReader reader = new StreamReader(SystemTablesFile))
            {
                string? line;
                while ((line = reader.ReadLine()) != null)
                {
                    Console.WriteLine(line);
                    tablesFound = true;
                }
            }

            if (!tablesFound)
            {
                Console.WriteLine("No tables found in the current database.");
            }

            return OperationStatus.Success;
        }

        public OperationStatus DropTable(string tableName)
        {
            string tablePath = Path.Combine(DataPath, $"{tableName}.table");
            if (!File.Exists(tablePath))
            {
                return OperationStatus.Error;
            }

            File.Delete(tablePath);

            // Remove the table from SystemTables.table
            string[] lines = File.ReadAllLines(SystemTablesFile);
            using (StreamWriter writer = new StreamWriter(SystemTablesFile))
            {
                foreach (var line in lines)
                {
                    if (!line.Contains(tableName))
                    {
                        writer.WriteLine(line);
                    }
                }
            }

            return OperationStatus.Success;
        }

        public OperationStatus DescribeTable(string tableName)
        {
            string tablePath = Path.Combine(DataPath, $"{tableName}.table");
            if (!File.Exists(tablePath))
            {
                Console.WriteLine($"Table {tableName} does not exist");
                return OperationStatus.Error;
            }

            using (StreamReader reader = new StreamReader(tablePath))
            {
                Console.WriteLine($"Columns in table {tableName}:");
                string? line;
                while ((line = reader.ReadLine()) != null)
                {
                    string[] columnInfo = line.Split(',');
                    if (columnInfo.Length == 2)
                    {
                        Console.WriteLine($"Column Name: {columnInfo[0].Trim()}, Type: {columnInfo[1].Trim()}");
                    }
                    else
                    {
                        // We've reached the data part of the file
                        break;
                    }
                }
            }

            return OperationStatus.Success;
        }

        // Add this method to create an index
        public OperationStatus CreateIndex(string indexName, string tableName, string columnName, string indexType)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(indexName) || string.IsNullOrWhiteSpace(tableName) || 
                    string.IsNullOrWhiteSpace(columnName) || string.IsNullOrWhiteSpace(indexType))
                {
                    Console.WriteLine("Invalid index parameters");
                    return OperationStatus.Error;
                }

                string tablePath = Path.Combine(DataPath, $"{tableName}.table");
                if (!File.Exists(tablePath))
                {
                    Console.WriteLine($"Table {tableName} does not exist");
                    return OperationStatus.Error;
                }

                // Check if an index already exists for this column
                if (tableIndices.ContainsKey(tableName) && tableIndices[tableName].ContainsKey(columnName))
                {
                    Console.WriteLine($"An index already exists for column {columnName} in table {tableName}");
                    return OperationStatus.Error;
                }

                // Read table data and check for duplicate values
                List<string> columnValues = new List<string>();
                int columnIndex = -1;
                using (StreamReader reader = new StreamReader(tablePath))
                {
                    string? line;
                    bool isHeaderSection = true;
                    while ((line = reader.ReadLine()) != null)
                    {
                        if (isHeaderSection)
                        {
                            string[] parts = line.Split(',');
                            if (parts[0] == columnName)
                            {
                                columnIndex = Array.IndexOf(parts, columnName);
                                isHeaderSection = false;
                            }
                        }
                        else
                        {
                            // This is a data line
                            if (line.StartsWith("(") && line.EndsWith(")"))
                            {
                                string[] parts = line.Trim('(', ')').Split(",,");
                                if (columnIndex < parts.Length)
                                {
                                    string value = parts[columnIndex].Trim('"');
                                    if (columnValues.Contains(value))
                                    {
                                        Console.WriteLine($"Duplicate value found in column {columnName}. Cannot create index.");
                                        return OperationStatus.Error;
                                    }
                                    columnValues.Add(value);
                                }
                            }
                        }
                    }
                }

                if (columnIndex == -1)
                {
                    Console.WriteLine($"Column {columnName} not found in table {tableName}");
                    return OperationStatus.Error;
                }

                // Create the index
                Index index;
                if (indexType.ToUpper() == "BTREE")
                {
                    index = new BTreeIndex();
                }
                else if (indexType.ToUpper() == "BST")
                {
                    index = new BSTIndex();
                }
                else
                {
                    Console.WriteLine("Invalid index type. Use BTREE or BST.");
                    return OperationStatus.Error;
                }

                // Add values to the index
                for (int i = 0; i < columnValues.Count; i++)
                {
                    index.Insert(columnValues[i], i);
                }

                // Add the index to the tableIndices dictionary
                if (!tableIndices.ContainsKey(tableName))
                {
                    tableIndices[tableName] = new Dictionary<string, Index>();
                }
                tableIndices[tableName][columnName] = index;

                // Add the index to the system catalog
                string indexCatalogPath = Path.Combine(SystemCatalogPath, "SystemIndices.table");
                using (StreamWriter writer = File.AppendText(indexCatalogPath))
                {
                    writer.WriteLine($"{tableName},{columnName},{indexName},{indexType}");
                }

                Console.WriteLine($"Index {indexName} created successfully on {tableName}.{columnName} of type {indexType}");
                return OperationStatus.Success;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error creating index: {ex.Message}");
                return OperationStatus.Error;
            }
        }

        // Add this method to load indices when the server starts
        private void LoadIndices()
        {
            string indexCatalogPath = Path.Combine(SystemCatalogPath, "SystemIndices.table");
            if (!File.Exists(indexCatalogPath))
            {
                return;
            }

            using (StreamReader reader = new StreamReader(indexCatalogPath))
            {
                string? line;
                while ((line = reader.ReadLine()) != null)
                {
                    string[] parts = line.Split(',');
                    if (parts.Length == 4)
                    {
                        string tableName = parts[0];
                        string columnName = parts[1];
                        string indexName = parts[2];
                        string indexType = parts[3];

                        CreateIndex(indexName, tableName, columnName, indexType);
                    }
                }
            }
        }

        // Add abstract Index class and its implementations
        private abstract class Index
        {
            public abstract void Insert(string key, int value);
            public abstract int Search(string key);
        }

        private class BTreeIndex : Index
        {
            private const int T = 2; // Minimum degree of B-Tree
            private BTreeNode root;

            private class BTreeNode
            {
                public int[] keys;
                public int[] values;
                public BTreeNode[] children;
                public int n;
                public bool leaf;

                public BTreeNode(bool leaf)
                {
                    this.leaf = leaf;
                    keys = new int[2 * T - 1];
                    values = new int[2 * T - 1];
                    children = new BTreeNode[2 * T];
                    n = 0;
                }
            }

            public BTreeIndex()
            {
                root = new BTreeNode(true);
            }

            public override void Insert(string key, int value)
            {
                int keyHash = key.GetHashCode();
                if (root.n == 2 * T - 1)
                {
                    BTreeNode s = new BTreeNode(false);
                    s.children[0] = root;
                    SplitChild(s, 0, root);
                    int i = 0;
                    if (s.keys[0] < keyHash)
                        i++;
                    InsertNonFull(s.children[i], keyHash, value);
                    root = s;
                }
                else
                    InsertNonFull(root, keyHash, value);
            }

            private void InsertNonFull(BTreeNode x, int k, int v)
            {
                int i = x.n - 1;
                if (x.leaf)
                {
                    while (i >= 0 && x.keys[i] > k)
                    {
                        x.keys[i + 1] = x.keys[i];
                        x.values[i + 1] = x.values[i];
                        i--;
                    }
                    x.keys[i + 1] = k;
                    x.values[i + 1] = v;
                    x.n = x.n + 1;
                }
                else
                {
                    while (i >= 0 && x.keys[i] > k)
                        i--;
                    i++;
                    if (x.children[i].n == 2 * T - 1)
                    {
                        SplitChild(x, i, x.children[i]);
                        if (x.keys[i] < k)
                            i++;
                    }
                    InsertNonFull(x.children[i], k, v);
                }
            }

            private void SplitChild(BTreeNode x, int i, BTreeNode y)
            {
                BTreeNode z = new BTreeNode(y.leaf);
                z.n = T - 1;
                for (int j = 0; j < T - 1; j++)
                {
                    z.keys[j] = y.keys[j + T];
                    z.values[j] = y.values[j + T];
                }
                if (!y.leaf)
                {
                    for (int j = 0; j < T; j++)
                        z.children[j] = y.children[j + T];
                }
                y.n = T - 1;
                for (int j = x.n; j >= i + 1; j--)
                    x.children[j + 1] = x.children[j];
                x.children[i + 1] = z;
                for (int j = x.n - 1; j >= i; j--)
                {
                    x.keys[j + 1] = x.keys[j];
                    x.values[j + 1] = x.values[j];
                }
                x.keys[i] = y.keys[T - 1];
                x.values[i] = y.values[T - 1];
                x.n = x.n + 1;
            }

            public override int Search(string key)
            {
                return Search(root, key.GetHashCode());
            }

            private int Search(BTreeNode x, int k)
            {
                int i = 0;
                while (i < x.n && k > x.keys[i])
                    i++;
                if (i < x.n && k == x.keys[i])
                    return x.values[i];
                if (x.leaf)
                    return -1;
                return Search(x.children[i], k);
            }
        }

        private class BSTIndex : Index
        {
            private BSTNode root;

            private class BSTNode
            {
                public string key;
                public int value;
                public BSTNode left, right;

                public BSTNode(string key, int value)
                {
                    this.key = key;
                    this.value = value;
                    left = right = null;
                }
            }

            public override void Insert(string key, int value)
            {
                root = InsertRec(root, key, value);
            }

            private BSTNode InsertRec(BSTNode root, string key, int value)
            {
                if (root == null)
                {
                    root = new BSTNode(key, value);
                    return root;
                }

                if (string.Compare(key, root.key) < 0)
                    root.left = InsertRec(root.left, key, value);
                else if (string.Compare(key, root.key) > 0)
                    root.right = InsertRec(root.right, key, value);

                return root;
            }

            public override int Search(string key)
            {
                return SearchRec(root, key);
            }

            private int SearchRec(BSTNode root, string key)
            {
                if (root == null || root.key == key)
                    return (root != null) ? root.value : -1;

                if (string.Compare(key, root.key) < 0)
                    return SearchRec(root.left, key);

                return SearchRec(root.right, key);
            }
        }

        public OperationStatus InsertIntoTable(string tableName, List<string> values)
        {
            try
            {
                string tablePath = Path.Combine(DataPath, $"{tableName}.table");
                if (!File.Exists(tablePath))
                {
                    Console.WriteLine($"Table {tableName} does not exist");
                    return OperationStatus.Error;
                }

                // Check for duplicates in indexed columns
                if (tableIndices.ContainsKey(tableName))
                {
                    using (StreamReader reader = new StreamReader(tablePath))
                    {
                        string? headerLine = reader.ReadLine();
                        if (headerLine != null)
                        {
                            string[] headers = headerLine.Split(',');
                            foreach (var indexedColumn in tableIndices[tableName])
                            {
                                string columnName = indexedColumn.Key;
                                Index index = indexedColumn.Value;
                                int columnIndex = Array.IndexOf(headers, columnName);
                                if (columnIndex != -1 && columnIndex < values.Count)
                                {
                                    string value = values[columnIndex];
                                    if (index.Search(value) != -1)
                                    {
                                        Console.WriteLine($"Duplicate value found in indexed column {columnName}. Cannot insert row.");
                                        return OperationStatus.Error;
                                    }
                                }
                            }
                        }
                    }
                }

                // Insert the new row
                using (StreamWriter writer = File.AppendText(tablePath))
                {
                    writer.WriteLine(string.Join(",", values));
                }

                // Update indices
                if (tableIndices.ContainsKey(tableName))
                {
                    int rowIndex = File.ReadLines(tablePath).Count() - 1;
                    foreach (var indexedColumn in tableIndices[tableName])
                    {
                        string columnName = indexedColumn.Key;
                        Index index = indexedColumn.Value;
                        int columnIndex = -1;
                        using (StreamReader reader = new StreamReader(tablePath))
                        {
                            string? headerLine = reader.ReadLine();
                            if (headerLine != null)
                            {
                                string[] headers = headerLine.Split(',');
                                columnIndex = Array.IndexOf(headers, columnName);
                            }
                        }
                        if (columnIndex != -1 && columnIndex < values.Count)
                        {
                            index.Insert(values[columnIndex], rowIndex);
                        }
                    }
                }

                Console.WriteLine("Row inserted successfully");
                return OperationStatus.Success;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error inserting row: {ex.Message}");
                return OperationStatus.Error;
            }
        }
    }
}