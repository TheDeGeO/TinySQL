using Entities;
using System.Text.RegularExpressions;
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

        public OperationStatus Select(string tableName, List<string> columns, string whereClause, string orderByClause)
        {
            string tablePath = Path.Combine(DataPath, $"{tableName}.table");
            if (!File.Exists(tablePath))
            {
                Console.WriteLine($"Table {tableName} does not exist.");
                return OperationStatus.Error;
            }

            List<string[]> results = new List<string[]>();
            List<string> headers = new List<string>();
            List<string> types = new List<string>();
            List<int> columnIndices = new List<int>();

            using (StreamReader reader = new StreamReader(tablePath))
            {
                // Read headers and types
                string? headerLine = reader.ReadLine();
                string? typeLine = reader.ReadLine();
                
                if (headerLine != null && typeLine != null)
                {
                    headers = headerLine.Split(',').Select(h => h.Trim('"')).ToList();
                    types = typeLine.Split(',').ToList();

                    // Handle SELECT * case
                    if (columns.Count == 1 && columns[0] == "*")
                    {
                        columns = headers;
                    }

                    // Find indices of requested columns
                    foreach (string column in columns)
                    {
                        int index = headers.IndexOf(column);
                        if (index != -1)
                        {
                            columnIndices.Add(index);
                        }
                        else
                        {
                            Console.WriteLine($"Column {column} does not exist in table {tableName}");
                            return OperationStatus.Error;
                        }
                    }
                }
                else
                {
                    Console.WriteLine($"Table {tableName} is empty or corrupted.");
                    return OperationStatus.Error;
                }

                // Process data rows
                string? line;
                while ((line = reader.ReadLine()) != null)
                {
                    ProcessDataLine(line, headers, columnIndices, whereClause, results);
                }
            }

            if (!string.IsNullOrEmpty(orderByClause))
            {
                ApplyOrderBy(results, headers.ToArray(), orderByClause, columns);
            }

            // Print results
            foreach (var row in results)
            {
                // Only print the columns that were originally requested
                var selectedValues = columns.Select(col => row[columns.IndexOf(col)]);
                Console.WriteLine(string.Join(",", selectedValues.Select(v => string.IsNullOrEmpty(v) ? "NULL" : v)));
            }

            return OperationStatus.Success;
        }

        private void ProcessDataLine(string line, List<string> headers, List<int> columnIndices, string whereClause, List<string[]> results)
        {
            string[] values = line.Trim('(', ')').Split(new[] { ",," }, StringSplitOptions.None);
            
            if (EvaluateWhereClause(headers, values, whereClause))
            {
                string[] selectedValues = columnIndices.Select(i => i < values.Length ? values[i].Trim('"', ' ', ',') : "NULL").ToArray();
                results.Add(selectedValues);
            }
        }

        private bool EvaluateWhereClause(List<string> headers, string[] values, string whereClause)
        {
            if (string.IsNullOrEmpty(whereClause))
            {
                return true;
            }

            // Trim any trailing commas from the where clause
            whereClause = whereClause.TrimEnd(',');

            string[] whereClauseParts = whereClause.Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
            if (whereClauseParts.Length < 3)
            {
                Console.WriteLine("Invalid WHERE clause format.");
                return false;
            }

            string columnName = whereClauseParts[0];
            string operation = whereClauseParts[1].ToUpper();
            string value = string.Join(" ", whereClauseParts.Skip(2)); // Join the rest of the parts in case the value contains spaces

            int columnIndex = headers.IndexOf(columnName);
            if (columnIndex == -1)
            {
                Console.WriteLine($"Column {columnName} does not exist.");
                return false;
            }

            string columnValue = values[columnIndex];

            // Trim parentheses and split by double commas
            columnValue = columnValue.Trim('(', ')');
            string[] columnValueParts = columnValue.Split(new[] { ",," }, StringSplitOptions.None);

            // Use the first non-empty part as the actual value
            columnValue = columnValueParts.FirstOrDefault(v => !string.IsNullOrWhiteSpace(v)) ?? string.Empty;

            // Trim quotes, whitespace, and any remaining single commas from both the column value and the comparison value
            columnValue = columnValue.Trim().Trim('"').Trim(',');
            value = value.Trim().Trim('"');

            switch (operation)
            {
                case "=":
                    return string.Equals(columnValue, value, StringComparison.OrdinalIgnoreCase);
                case "<>":
                    return !string.Equals(columnValue, value, StringComparison.OrdinalIgnoreCase);
                case ">":
                case "<":
                case ">=":
                case "<=":
                    if (double.TryParse(columnValue, out double numericColumnValue) && double.TryParse(value, out double numericValue))
                    {
                        return operation switch
                        {
                            ">" => numericColumnValue > numericValue,
                            "<" => numericColumnValue < numericValue,
                            ">=" => numericColumnValue >= numericValue,
                            "<=" => numericColumnValue <= numericValue,
                            _ => false // This should never happen
                        };
                    }
                    else
                    {
                        return operation switch
                        {
                            ">" => string.Compare(columnValue, value, StringComparison.OrdinalIgnoreCase) > 0,
                            "<" => string.Compare(columnValue, value, StringComparison.OrdinalIgnoreCase) < 0,
                            ">=" => string.Compare(columnValue, value, StringComparison.OrdinalIgnoreCase) >= 0,
                            "<=" => string.Compare(columnValue, value, StringComparison.OrdinalIgnoreCase) <= 0,
                            _ => false
                        };
                    }
                case "LIKE":
                    return IsLikeMatch(columnValue, value);
                case "NOT":
                    if (whereClauseParts.Length < 4 || whereClauseParts[2].ToUpper() != "LIKE")
                    {
                        Console.WriteLine("Invalid NOT LIKE clause format.");
                        return false;
                    }
                    return !IsLikeMatch(columnValue, whereClauseParts[3]);
                default:
                    Console.WriteLine($"Unsupported operation {operation}.");
                    return false;
            }
        }

        private bool IsLikeMatch(string value, string pattern)
        {
            string regexPattern = "^" + Regex.Escape(pattern).Replace("%", ".*").Replace("_", ".") + "$";
            Console.WriteLine(regexPattern);
            return Regex.IsMatch(value, regexPattern, RegexOptions.IgnoreCase);
        }

        private static void ApplyOrderBy(List<string[]> results, string[] headers, string orderByClause, List<string> selectedColumns)
        {
            if (string.IsNullOrWhiteSpace(orderByClause))
            {
                return; // No order by clause, do nothing
            }
        
            string[] parts = orderByClause.Split(' ');
            if (parts.Length < 2)
            {
                return; // Invalid order by clause, do nothing
            }
        
            string columnName = parts[0];
            string direction = parts.Length > 1 ? parts[1].ToUpper() : "ASC";
        
            if (direction != "ASC" && direction != "DESC")
            {
                return; // Invalid direction, do nothing
            }
        
            int columnIndex = Array.IndexOf(headers, columnName);
            if (columnIndex == -1)
            {
                return; // Column not found, do nothing
            }
        
            // Store the original indices, full row data, and selected column data before sorting
            var tempResults = results.Select((row, index) => new
            {
                Row = row,
                Index = index,
                SortValue = row[columnIndex],
                SelectedValues = selectedColumns.Select(col => row[Array.IndexOf(headers, col)]).ToArray()
            }).ToList();
        
            tempResults.Sort((a, b) =>
            {
                int comparison;
                if (double.TryParse(a.SortValue, out double doubleA) && double.TryParse(b.SortValue, out double doubleB))
                {
                    comparison = doubleA.CompareTo(doubleB);
                }
                else
                {
                    comparison = string.Compare(a.SortValue, b.SortValue, StringComparison.OrdinalIgnoreCase);
                }
        
                // If values are equal, use the original index for stable sorting
                if (comparison == 0)
                {
                    return a.Index.CompareTo(b.Index);
                }
        
                // Fix for >= operator
                if (direction == "ASC" && comparison > 0)
                {
                    return 1;
                }
                else if (direction == "DESC" && comparison < 0)
                {
                    return -1;
                }
        
                return direction == "DESC" ? -comparison : comparison;
            });
        
            // Update the results list with the sorted data, but only include selected columns
            results.Clear();
            results.AddRange(tempResults.Select(item => item.SelectedValues));
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

        public OperationStatus UpdateTable(string tableName, Dictionary<string, string> setValues, string whereCondition)
        {
            string tablePath = Path.Combine(DataPath, $"{tableName}.table");
            if (!File.Exists(tablePath))
            {
                Console.WriteLine($"Table {tableName} does not exist");
                return OperationStatus.Error;
            }

            List<string> lines = File.ReadAllLines(tablePath).ToList();
            List<string> columnNames = new List<string>();
            List<string> columnTypes = new List<string>();
            int dataStartIndex = 0;

            // Parse column information
            for (int i = 0; i < lines.Count; i++)
            {
                string[] parts = lines[i].Split(',');
                if (parts.Length == 2)
                {
                    columnNames.Add(parts[0]);
                    columnTypes.Add(parts[1]);
                }
                else
                {
                    dataStartIndex = i;
                    break;
                }
            }

            // Parse WHERE condition
            string[] whereparts = whereCondition.Split('=');
            string whereColumn = whereparts[0].Trim();
            string whereValue = whereparts[1].Trim();
            int whereColumnIndex = columnNames.IndexOf(whereColumn);

            if (whereColumnIndex == -1)
            {
                Console.WriteLine($"Column {whereColumn} not found in table {tableName}");
                return OperationStatus.Error;
            }

            // Update matching rows
            for (int i = dataStartIndex; i < lines.Count; i++)
            {
                string line = lines[i];
                if (line.StartsWith("(") && line.EndsWith(")"))
                {
                    string[] values = line.Substring(1, line.Length - 2).Split(",,");
                    if (values[whereColumnIndex] == whereValue)
                    {
                        foreach (var setValue in setValues)
                        {
                            int setColumnIndex = columnNames.IndexOf(setValue.Key);
                            if (setColumnIndex != -1)
                            {
                                values[setColumnIndex] = setValue.Value;
                            }
                        }
                        lines[i] = "(" + string.Join(",,", values) + ")";
                    }
                }
            }

            // Write updated content back to file
            File.WriteAllLines(tablePath, lines);

            return OperationStatus.Success;
        }

        public OperationStatus Delete(string tableName, string whereClause)
        {
            string tablePath = Path.Combine(DataPath, $"{tableName}.table");
            if (!File.Exists(tablePath))
            {
                Console.WriteLine($"Table {tableName} does not exist");
                return OperationStatus.Error;
            }

            List<string> lines = File.ReadAllLines(tablePath).ToList();
            List<string> columnNames = new List<string>();
            List<string> columnTypes = new List<string>();
            int dataStartIndex = 0;

            // Parse column information
            for (int i = 0; i < lines.Count; i++)
            {
                string[] parts = lines[i].Split(',');
                if (parts.Length == 2)
                {
                    columnNames.Add(parts[0]);
                    columnTypes.Add(parts[1]);
                }
                else
                {
                    dataStartIndex = i;
                    break;
                }
            }

            // Parse WHERE condition
            string[] whereParts = whereClause.Split('=');
            string whereColumn = whereParts[0].Trim();
            string whereValue = whereParts[1].Trim();
            int whereColumnIndex = columnNames.IndexOf(whereColumn);

            if (whereColumnIndex == -1)
            {
                Console.WriteLine($"Column {whereColumn} not found in table {tableName}");
                return OperationStatus.Error;
            }

            // Remove matching rows
            for (int i = dataStartIndex; i < lines.Count; i++)
            {
                string line = lines[i];
                if (line.StartsWith("(") && line.EndsWith(")"))
                {
                    string[] values = line.Substring(1, line.Length - 2).Split(",,");
                    if (values[whereColumnIndex] == whereValue)
                    {
                        lines.RemoveAt(i);
                        i--; // Adjust the loop index after removing a line
                    }
                }
            }

            // Write updated content back to file
            File.WriteAllLines(tablePath, lines);

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
            public abstract IEnumerable<int> SearchLessThan(string key);
            public abstract IEnumerable<int> SearchGreaterThan(string key);
            public abstract IEnumerable<int> SearchLessThanOrEqual(string key);
            public abstract IEnumerable<int> SearchGreaterThanOrEqual(string key);
            public virtual IEnumerable<int> SearchLike(string key) => throw new NotImplementedException();
            public virtual IEnumerable<int> SearchNotLike(string key) => throw new NotImplementedException();
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

            public override IEnumerable<int> SearchLessThan(string key)
            {
            return SearchLessThanRec(root, key.GetHashCode());
            }

            private IEnumerable<int> SearchLessThanRec(BTreeNode x, int k)
            {
            List<int> result = new List<int>();
            for (int i = 0; i < x.n; i++)
            {
                if (x.keys[i] >= k)
                break;
                result.Add(x.values[i]);
                if (!x.leaf)
                result.AddRange(SearchLessThanRec(x.children[i], k));
            }
            if (!x.leaf && x.keys[x.n - 1] < k)
                result.AddRange(SearchLessThanRec(x.children[x.n], k));
            return result;
            }

            public override IEnumerable<int> SearchGreaterThan(string key)
            {
            return SearchGreaterThanRec(root, key.GetHashCode());
            }

            private IEnumerable<int> SearchGreaterThanRec(BTreeNode x, int k)
            {
            List<int> result = new List<int>();
            int i;
            for (i = 0; i < x.n; i++)
            {
                if (x.keys[i] > k)
                {
                result.Add(x.values[i]);
                if (!x.leaf)
                    result.AddRange(SearchGreaterThanRec(x.children[i], k));
                }
                else if (!x.leaf)
                result.AddRange(SearchGreaterThanRec(x.children[i], k));
            }
            if (!x.leaf)
                result.AddRange(SearchGreaterThanRec(x.children[i], k));
            return result;
            }

            public override IEnumerable<int> SearchLessThanOrEqual(string key)
            {
            return SearchLessThanOrEqualRec(root, key.GetHashCode());
            }

            private IEnumerable<int> SearchLessThanOrEqualRec(BTreeNode x, int k)
            {
            List<int> result = new List<int>();
            for (int i = 0; i < x.n; i++)
            {
                if (x.keys[i] > k)
                break;
                result.Add(x.values[i]);
                if (!x.leaf)
                result.AddRange(SearchLessThanOrEqualRec(x.children[i], k));
            }
            if (!x.leaf && x.keys[x.n - 1] <= k)
                result.AddRange(SearchLessThanOrEqualRec(x.children[x.n], k));
            return result;
            }

            public override IEnumerable<int> SearchGreaterThanOrEqual(string key)
            {
            return SearchGreaterThanOrEqualRec(root, key.GetHashCode());
            }

            private IEnumerable<int> SearchGreaterThanOrEqualRec(BTreeNode x, int k)
            {
            List<int> result = new List<int>();
            int i;
            for (i = 0; i < x.n; i++)
            {
                if (x.keys[i] >= k)
                {
                result.Add(x.values[i]);
                if (!x.leaf)
                    result.AddRange(SearchGreaterThanOrEqualRec(x.children[i], k));
                }
                else if (!x.leaf)
                result.AddRange(SearchGreaterThanOrEqualRec(x.children[i], k));
            }
            if (!x.leaf)
                result.AddRange(SearchGreaterThanOrEqualRec(x.children[i], k));
            return result;
            }

        public override IEnumerable<int> SearchLike(string pattern)
        {
            return SearchLikeRec(root, pattern);
        }

        private IEnumerable<int> SearchLikeRec(BTreeNode x, string pattern)
        {
            List<int> result = new List<int>();
            for (int i = 0; i < x.n; i++)
            {
                if (IsLikeMatch(x.keys[i].ToString(), pattern))
                {
                    result.Add(x.values[i]);
                }
                if (!x.leaf)
                {
                    result.AddRange(SearchLikeRec(x.children[i], pattern));
                }
            }
            if (!x.leaf)
            {
                result.AddRange(SearchLikeRec(x.children[x.n], pattern));
            }
            return result;
        }

        public override IEnumerable<int> SearchNotLike(string pattern)
        {
            return SearchNotLikeRec(root, pattern);
        }

        private IEnumerable<int> SearchNotLikeRec(BTreeNode x, string pattern)
        {
            List<int> result = new List<int>();
            for (int i = 0; i < x.n; i++)
            {
                if (!IsLikeMatch(x.keys[i].ToString(), pattern))
                {
                    result.Add(x.values[i]);
                }
                if (!x.leaf)
                {
                    result.AddRange(SearchNotLikeRec(x.children[i], pattern));
                }
            }
            if (!x.leaf)
            {
                result.AddRange(SearchNotLikeRec(x.children[x.n], pattern));
            }
            return result;
        }

        private bool IsLikeMatch(string value, string pattern)
        {
            string regexPattern = "^" + Regex.Escape(pattern).Replace("%", ".*").Replace("_", ".") + "$";
            return Regex.IsMatch(value, regexPattern, RegexOptions.IgnoreCase);
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

            public override IEnumerable<int> SearchLessThan(string key)
            {
            List<int> result = new List<int>();
            SearchLessThanRec(root, key, result);
            return result;
            }

            private void SearchLessThanRec(BSTNode node, string key, List<int> result)
            {
            if (node == null)
                return;

            if (string.Compare(key, node.key) <= 0)
                SearchLessThanRec(node.left, key, result);
            else
            {
                SearchLessThanRec(node.left, key, result);
                result.Add(node.value);
                SearchLessThanRec(node.right, key, result);
            }
            }

            public override IEnumerable<int> SearchGreaterThan(string key)
            {
            List<int> result = new List<int>();
            SearchGreaterThanRec(root, key, result);
            return result;
            }

            private void SearchGreaterThanRec(BSTNode node, string key, List<int> result)
            {
            if (node == null)
                return;

            if (string.Compare(key, node.key) >= 0)
                SearchGreaterThanRec(node.right, key, result);
            else
            {
                SearchGreaterThanRec(node.left, key, result);
                result.Add(node.value);
                SearchGreaterThanRec(node.right, key, result);
            }
            }

            public override IEnumerable<int> SearchLessThanOrEqual(string key)
            {
            List<int> result = new List<int>();
            SearchLessThanOrEqualRec(root, key, result);
            return result;
            }

            private void SearchLessThanOrEqualRec(BSTNode node, string key, List<int> result)
            {
            if (node == null)
                return;

            if (string.Compare(key, node.key) < 0)
                SearchLessThanOrEqualRec(node.left, key, result);
            else
            {
                SearchLessThanOrEqualRec(node.left, key, result);
                result.Add(node.value);
                SearchLessThanOrEqualRec(node.right, key, result);
            }
            }

            public override IEnumerable<int> SearchGreaterThanOrEqual(string key)
            {
            List<int> result = new List<int>();
            SearchGreaterThanOrEqualRec(root, key, result);
            return result;
            }

            private void SearchGreaterThanOrEqualRec(BSTNode node, string key, List<int> result)
            {
            if (node == null)
                return;

            if (string.Compare(key, node.key) > 0)
                SearchGreaterThanOrEqualRec(node.right, key, result);
            else
            {
                SearchGreaterThanOrEqualRec(node.left, key, result);
                result.Add(node.value);
                SearchGreaterThanOrEqualRec(node.right, key, result);
            }
            }

        public override IEnumerable<int> SearchLike(string pattern)
        {
            List<int> result = new List<int>();
            SearchLikeRec(root, pattern, result);
            return result;
        }

        private void SearchLikeRec(BSTNode node, string pattern, List<int> result)
        {
            if (node == null)
                return;

            string regexPattern = "^" + Regex.Escape(pattern).Replace("%", ".*").Replace("_", ".") + "$";
            if (Regex.IsMatch(node.key.ToString(), regexPattern, RegexOptions.IgnoreCase))
            {
                result.Add(node.value);
            }

            SearchLikeRec(node.left, pattern, result);
            SearchLikeRec(node.right, pattern, result);
        }

        public override IEnumerable<int> SearchNotLike(string pattern)
        {
            List<int> result = new List<int>();
            SearchNotLikeRec(root, pattern, result);
            return result;
        }

        private void SearchNotLikeRec(BSTNode node, string pattern, List<int> result)
        {
            if (node == null)
                return;

            string regexPattern = "^" + Regex.Escape(pattern).Replace("%", ".*").Replace("_", ".") + "$";
            if (!Regex.IsMatch(node.key.ToString(), regexPattern, RegexOptions.IgnoreCase))
            {
                result.Add(node.value);
            }

            SearchNotLikeRec(node.left, pattern, result);
            SearchNotLikeRec(node.right, pattern, result);
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