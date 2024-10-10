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

        private static readonly string DatabaseBasePath = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), "TinySQL");
        private static readonly string SystemCatalogPath = Path.Combine(DatabaseBasePath, "SystemCatalog");
        private static readonly string DataPath = Path.Combine(DatabaseBasePath, "Databases");
        private static readonly string SystemDatabasesFile = Path.Combine(SystemCatalogPath, "SystemDatabases.table");
        private static readonly string SystemTablesFile = Path.Combine(SystemCatalogPath, "SystemTables.table");
        private static readonly string SystemIndicesFile = Path.Combine(SystemCatalogPath, "SystemIndices.table");

        private string currentDatabase = "";
        private Dictionary<string, Dictionary<string, Index>> tableIndices = new Dictionary<string, Dictionary<string, Index>>();

        private Store()
        {
            InitializeDirectories();
            LoadIndices();
        }

        private void InitializeDirectories()
        {
            Directory.CreateDirectory(DatabaseBasePath);
            Directory.CreateDirectory(SystemCatalogPath);
            Directory.CreateDirectory(DataPath);

            if (!File.Exists(SystemDatabasesFile))
            {
                File.Create(SystemDatabasesFile).Close();
            }
            if (!File.Exists(SystemTablesFile))
            {
                File.Create(SystemTablesFile).Close();
            }
            if (!File.Exists(SystemIndicesFile))
            {
                File.Create(SystemIndicesFile).Close();
            }
        }

        public OperationStatus SetDatabase(string databaseName)
        {
            if (string.IsNullOrWhiteSpace(databaseName))
            {
                return OperationStatus.Error;
            }

            string databasePath = Path.Combine(DataPath, databaseName);
            if (!Directory.Exists(databasePath))
            {
                return OperationStatus.Error;
            }

            currentDatabase = databaseName;
            return OperationStatus.Success;
        }

        public OperationStatus CreateDatabase(string databaseName)
        {
            // Remove semicolon from the database name if present
            databaseName = databaseName.TrimEnd(';');

            if (string.IsNullOrWhiteSpace(databaseName))
            {
                Console.WriteLine("Database name cannot be empty or whitespace.");
                return OperationStatus.Error;
            }

            string databasePath = Path.Combine(DataPath, databaseName);
            if (Directory.Exists(databasePath))
            {
                Console.WriteLine($"Database '{databaseName}' already exists.");
                return OperationStatus.Error;
            }

            // Check if the database name is already in use
            if (File.ReadLines(SystemDatabasesFile).Any(line => line.Trim().Equals(databaseName, StringComparison.OrdinalIgnoreCase)))
            {
                Console.WriteLine($"Database name '{databaseName}' is already in use.");
                return OperationStatus.Error;
            }

            Directory.CreateDirectory(databasePath);
            
            // Add the new database to SystemDatabases.table
            using (StreamWriter writer = File.AppendText(SystemDatabasesFile))
            {
                writer.WriteLine(databaseName);
            }

            Console.WriteLine($"Database '{databaseName}' created successfully.");
            return OperationStatus.Success;
        }

        private string GetCurrentDatabasePath()
        {
            if (string.IsNullOrEmpty(currentDatabase))
            {
                throw new InvalidOperationException("No database selected.");
            }
            return Path.Combine(DataPath, currentDatabase);
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

                string tablePath = Path.Combine(GetCurrentDatabasePath(), $"{tableName}.table");
                if (File.Exists(tablePath))
                {
                    Console.WriteLine($"Table {tableName} already exists");
                    return OperationStatus.Error;
                }

                // Check if the table name is already in use in the current database
                if (File.ReadLines(SystemTablesFile).Any(line => 
                    line.Split(',')[0].Equals(currentDatabase, StringComparison.OrdinalIgnoreCase) &&
                    line.Split(',')[1].Equals(tableName, StringComparison.OrdinalIgnoreCase)))
                {
                    Console.WriteLine($"Table name '{tableName}' is already in use in the current database.");
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
                    writer.WriteLine($"{currentDatabase},{tableName}");
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
            string tablePath = Path.Combine(GetCurrentDatabasePath(), $"{tableName}.table");
            if (!File.Exists(tablePath))
            {
                Console.WriteLine($"Table {tableName} does not exist.");
                return OperationStatus.Error;
            }

            List<string[]> results = new List<string[]>();
            List<string> headers = new List<string>();
            List<string> types = new List<string>();
            List<int> columnIndices = new List<int>();

            // Parse WHERE clause
            string whereColumn = "";
            string whereOperation = "";
            string whereValue = "";
            if (!string.IsNullOrEmpty(whereClause))
            {
                string[] whereParts = whereClause.Split(new[] { ' ' }, 3);
                if (whereParts.Length == 3)
                {
                    whereColumn = whereParts[0];
                    whereOperation = whereParts[1].ToUpper();
                    whereValue = whereParts[2];
                }
            }

            // Check if an index exists for the WHERE column
            bool useIndex = false;
            Index? index = null;
            if (tableIndices.ContainsKey(tableName) && tableIndices[tableName].ContainsKey(whereColumn))
            {
                index = tableIndices[tableName][whereColumn];
                useIndex = true;
            }

            using (StreamReader reader = new StreamReader(tablePath))
            {
                // Read headers and types
                string? line;
                while ((line = reader.ReadLine()) != null && !line.StartsWith("("))
                {
                    var parts = line.Split(',');
                    headers.Add(parts[0]);
                    types.Add(parts[1]);
                }

                // Handle SELECT * case
                if (columns.Count == 1 && columns[0] == "*")
                {
                    columns = headers;
                }

                // Find indices of requested columns
                foreach (string column in columns)
                {
                    int columnIndex = headers.IndexOf(column);
                    if (columnIndex != -1)
                    {
                        columnIndices.Add(columnIndex);
                    }
                    else
                    {
                        Console.WriteLine($"Column {column} does not exist in table {tableName}");
                        return OperationStatus.Error;
                    }
                }

                if (useIndex)
                {
                    // Use index for faster search
                    IEnumerable<int> matchingRows;
                    switch (whereOperation)
                    {
                        case "=":
                            matchingRows = new List<int> { index.Search(whereValue) };
                            break;
                        case "<":
                            matchingRows = index.SearchLessThan(whereValue);
                            break;
                        case ">":
                            matchingRows = index.SearchGreaterThan(whereValue);
                            break;
                        case "<=":
                            matchingRows = index.SearchLessThanOrEqual(whereValue);
                            break;
                        case ">=":
                            matchingRows = index.SearchGreaterThanOrEqual(whereValue);
                            break;
                        case "LIKE":
                            matchingRows = index.SearchLike(whereValue);
                            break;
                        case "NOT LIKE":
                            matchingRows = index.SearchNotLike(whereValue);
                            break;
                        default:
                            Console.WriteLine($"Unsupported operation for indexed search: {whereOperation}");
                            return OperationStatus.Error;
                    }

                    foreach (int rowIndex in matchingRows)
                    {
                        reader.BaseStream.Seek(0, SeekOrigin.Begin);
                        for (int i = 0; i <= rowIndex; i++)
                        {
                            line = reader.ReadLine();
                        }
                        if (line != null && line.StartsWith("("))
                        {
                            ProcessDataLine(line, headers, columnIndices, "", results);
                        }
                    }
                }
                else
                {
                    // Process data rows without index
                    do
                    {
                        if (line != null && line.StartsWith("("))
                        {
                            ProcessDataLine(line, headers, columnIndices, whereClause, results);
                        }
                    } while ((line = reader.ReadLine()) != null);
                }
            }

            if (!string.IsNullOrEmpty(orderByClause))
            {
                ApplyOrderBy(results, headers.ToArray(), orderByClause, columns);
            }

            // Print results
            Console.WriteLine();
            Console.WriteLine(string.Join("\t", columns));  // Print column headers
            foreach (var row in results)
            {
                Console.WriteLine(string.Join("\t", row.Select(v => string.IsNullOrEmpty(v) ? "NULL" : v)));
            }
            Console.WriteLine();

            return OperationStatus.Success;
        }

        private void ProcessDataLine(string line, List<string> headers, List<int> columnIndices, string whereClause, List<string[]> results)
        {
            // Remove leading and trailing parentheses
            line = line.Trim('(', ')');
            
            // Split the line by ",," to handle empty values correctly
            string[] values = line.Split(new[] { ",," }, StringSplitOptions.None);
            
            if (EvaluateWhereClause(headers, values, whereClause))
            {
                string[] selectedValues = columnIndices.Select(i => i < values.Length ? values[i].Trim('\'', ' ') : "NULL").ToArray();
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
            // Escape special regex characters, but not % and _
            string escapedPattern = Regex.Escape(pattern).Replace(@"\%", "%").Replace(@"\_", "_");
            string regexPattern = "^" + escapedPattern.Replace("%", ".*").Replace("_", ".") + "$";
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
                if (int.TryParse(a.SortValue, out int intA) && int.TryParse(b.SortValue, out int intB))
                {
                    comparison = intA.CompareTo(intB);
                }
                else if (double.TryParse(a.SortValue, out double doubleA) && double.TryParse(b.SortValue, out double doubleB))
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

                return direction == "DESC" ? -comparison : comparison;
            });

            // Update the results list with the sorted data, but only include selected columns
            results.Clear();
            results.AddRange(tempResults.Select(item => item.SelectedValues));
        }

        public OperationStatus ShowDatabases()
        {
            if (!Directory.Exists(DataPath))
            {
                Console.WriteLine("No databases found.");
                return OperationStatus.Success;
            }

            string[] databases = Directory.GetDirectories(DataPath);
            foreach (var database in databases)
            {
                string databaseName = Path.GetFileName(database);
                Console.WriteLine(databaseName);
            }

            return OperationStatus.Success;
        }

        public OperationStatus ShowTables()
        {
            if (string.IsNullOrEmpty(currentDatabase))
            {
                Console.WriteLine("No database selected.");
                return OperationStatus.Error;
            }

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
                    string[] parts = line.Split(',');
                    if (parts.Length == 2 && parts[0] == currentDatabase)
                    {
                        Console.WriteLine(parts[1]);
                        tablesFound = true;
                    }
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
            if (string.IsNullOrEmpty(currentDatabase))
            {
                Console.WriteLine("No database selected.");
                return OperationStatus.Error;
            }

            string currentDatabasePath;
            try
            {
                currentDatabasePath = GetCurrentDatabasePath();
            }
            catch (InvalidOperationException)
            {
                Console.WriteLine("No database selected.");
                return OperationStatus.Error;
            }

            string tablePath = Path.Combine(currentDatabasePath, $"{tableName}.table");
            if (!File.Exists(tablePath))
            {
                Console.WriteLine($"Table '{tableName}' does not exist in the current database.");
                return OperationStatus.Error;
            }

            File.Delete(tablePath);

            // Remove the table from SystemTables.table
            string[] lines = File.ReadAllLines(SystemTablesFile);
            using (StreamWriter writer = new StreamWriter(SystemTablesFile))
            {
                foreach (var line in lines)
                {
                    if (!line.Equals($"{currentDatabase},{tableName}", StringComparison.OrdinalIgnoreCase))
                    {
                        writer.WriteLine(line);
                    }
                }
            }

            // Remove any indices associated with the dropped table
            if (tableIndices.ContainsKey(tableName))
            {
                tableIndices.Remove(tableName);
            }

            // Remove index files associated with the dropped table
            string[] indexFiles = Directory.GetFiles(currentDatabasePath, $"{tableName}_*_index.idx");
            foreach (string indexFile in indexFiles)
            {
                File.Delete(indexFile);
            }

            // Update SystemIndicesFile to remove entries for the dropped table
            lines = File.ReadAllLines(SystemIndicesFile);
            using (StreamWriter writer = new StreamWriter(SystemIndicesFile))
            {
                foreach (var line in lines)
                {
                    if (!line.StartsWith($"{currentDatabase},{tableName},", StringComparison.OrdinalIgnoreCase))
                    {
                        writer.WriteLine(line);
                    }
                }
            }

            Console.WriteLine($"Table '{tableName}' has been successfully dropped from the database '{currentDatabase}'.");
            return OperationStatus.Success;
        }

        public OperationStatus DropDatabase(string databaseName)
        {
            string databasePath = Path.Combine(DataPath, databaseName);
            if (!Directory.Exists(databasePath))
            {
                Console.WriteLine($"Database '{databaseName}' does not exist.");
                return OperationStatus.Error;
            }

            try
            {
                // Delete all files and subdirectories
                Directory.Delete(databasePath, true);

                // Remove the database entry from SystemDatabasesFile
                List<string> lines = File.ReadAllLines(SystemDatabasesFile).ToList();
                lines.RemoveAll(line => line.Trim() == databaseName);
                File.WriteAllLines(SystemDatabasesFile, lines);

                // Remove all entries for this database from SystemTablesFile
                lines = File.ReadAllLines(SystemTablesFile).ToList();
                lines.RemoveAll(line => line.StartsWith($"{databaseName},"));
                File.WriteAllLines(SystemTablesFile, lines);

                // Remove all entries for this database from SystemIndicesFile
                lines = File.ReadAllLines(SystemIndicesFile).ToList();
                lines.RemoveAll(line => line.StartsWith($"{databaseName},"));
                File.WriteAllLines(SystemIndicesFile, lines);

                Console.WriteLine($"Database '{databaseName}' has been successfully dropped.");
                return OperationStatus.Success;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"An error occurred while dropping the database: {ex.Message}");
                return OperationStatus.Error;
            }
        }

        public OperationStatus DescribeTable(string tableName)
        {
            string tablePath = Path.Combine(GetCurrentDatabasePath(), $"{tableName}.table");
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
            string tablePath = Path.Combine(GetCurrentDatabasePath(), $"{tableName}.table");
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

            // Check if an index exists for the WHERE column
            string indexPath = Path.Combine(GetCurrentDatabasePath(), $"{tableName}_{whereColumn}_index.idx");
            if (File.Exists(indexPath))
            {
                return UpdateTableUsingIndex(tableName, setValues, whereColumn, whereValue, indexPath, lines, columnNames, dataStartIndex);
            }
            else
            {
                return UpdateTableWithoutIndex(tableName, setValues, whereColumn, whereValue, lines, columnNames, dataStartIndex);
            }
        }

        private OperationStatus UpdateTableUsingIndex(string tableName, Dictionary<string, string> setValues, string whereColumn, string whereValue, string indexPath, List<string> lines, List<string> columnNames, int dataStartIndex)
        {
            Dictionary<string, List<int>> index = LoadIndex(indexPath);

            if (index.TryGetValue(whereValue, out List<int> matchingRows))
            {
                foreach (int rowIndex in matchingRows)
                {
                    if (rowIndex >= dataStartIndex && rowIndex < lines.Count)
                    {
                        string line = lines[rowIndex];
                        if (line.StartsWith("(") && line.EndsWith(")"))
                        {
                            string[] values = line.Substring(1, line.Length - 2).Split(",,");
                            foreach (var setValue in setValues)
                            {
                                int setColumnIndex = columnNames.IndexOf(setValue.Key);
                                if (setColumnIndex != -1)
                                {
                                    values[setColumnIndex] = setValue.Value;
                                }
                            }
                            lines[rowIndex] = "(" + string.Join(",,", values) + ")";
                        }
                    }
                }

                // Write updated content back to file
                File.WriteAllLines(Path.Combine(GetCurrentDatabasePath(), $"{tableName}.table"), lines);

                // Update the index if necessary
                if (setValues.ContainsKey(whereColumn))
                {
                    UpdateIndex(tableName, whereColumn, lines, dataStartIndex);
                }

                return OperationStatus.Success;
            }
            else
            {
                Console.WriteLine($"No rows found matching the condition: {whereColumn} = {whereValue}");
                return OperationStatus.Error;
            }
        }

        private OperationStatus UpdateTableWithoutIndex(string tableName, Dictionary<string, string> setValues, string whereColumn, string whereValue, List<string> lines, List<string> columnNames, int dataStartIndex)
        {
            int whereColumnIndex = columnNames.IndexOf(whereColumn);
            bool rowsUpdated = false;

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
                        rowsUpdated = true;
                    }
                }
            }

            if (rowsUpdated)
            {
                // Write updated content back to file
                File.WriteAllLines(Path.Combine(GetCurrentDatabasePath(), $"{tableName}.table"), lines);
                return OperationStatus.Success;
            }
            else
            {
                Console.WriteLine($"No rows found matching the condition: {whereColumn} = {whereValue}");
                return OperationStatus.Error;
            }
        }

        private Dictionary<string, List<int>> LoadIndex(string indexPath)
        {
            Dictionary<string, List<int>> index = new Dictionary<string, List<int>>();
            string[] indexLines = File.ReadAllLines(indexPath);
            foreach (string line in indexLines)
            {
                string[] parts = line.Split(':');
                if (parts.Length == 2)
                {
                    string key = parts[0];
                    List<int> values = parts[1].Split(',').Select(int.Parse).ToList();
                    index[key] = values;
                }
            }
            return index;
        }

        private void UpdateIndex(string tableName, string columnName, List<string> lines, int dataStartIndex)
        {
            Dictionary<string, List<int>> index = new Dictionary<string, List<int>>();

            for (int i = dataStartIndex; i < lines.Count; i++)
            {
                string line = lines[i];
                if (line.StartsWith("(") && line.EndsWith(")"))
                {
                    string[] values = line.Substring(1, line.Length - 2).Split(",,");
                    string key = values[dataStartIndex];
                    if (!index.ContainsKey(key))
                    {
                        index[key] = new List<int>();
                    }
                    index[key].Add(i);
                }
            }

            string indexPath = Path.Combine(GetCurrentDatabasePath(), $"{tableName}_{columnName}_index.idx");
            using (StreamWriter writer = new StreamWriter(indexPath))
            {
                foreach (var entry in index)
                {
                    writer.WriteLine($"{entry.Key}:{string.Join(",", entry.Value)}");
                }
            }
        }

        public OperationStatus Delete(string tableName, string whereClause)
        {
            string tablePath = Path.Combine(GetCurrentDatabasePath(), $"{tableName}.table");
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

            // Check if an index exists for the WHERE column
            string indexPath = Path.Combine(GetCurrentDatabasePath(), $"{tableName}_{whereColumn}_index.idx");
            if (File.Exists(indexPath))
            {
                // Use index for faster deletion
                Dictionary<string, List<int>> index = LoadIndex(indexPath);
                if (index.ContainsKey(whereValue))
                {
                    List<int> rowsToDelete = index[whereValue];
                    rowsToDelete.Sort((a, b) => b.CompareTo(a)); // Sort in descending order
                    foreach (int rowIndex in rowsToDelete)
                    {
                        if (rowIndex >= dataStartIndex && rowIndex < lines.Count)
                        {
                            lines.RemoveAt(rowIndex);
                        }
                    }
                }
            }
            else
            {
                // Fallback to linear search if no index is available
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
            }

            // Write updated content back to file
            File.WriteAllLines(tablePath, lines);

            // Update all indices for this table
            UpdateAllIndices(tableName, lines, dataStartIndex);

            return OperationStatus.Success;
        }

        private void UpdateAllIndices(string tableName, List<string> lines, int dataStartIndex)
        {
            string[] indexFiles = Directory.GetFiles(GetCurrentDatabasePath(), $"{tableName}_*_index.idx");
            foreach (string indexFile in indexFiles)
            {
                string columnName = Path.GetFileNameWithoutExtension(indexFile).Split('_')[1];
                UpdateIndex(tableName, columnName, lines, dataStartIndex);
            }
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

                string tablePath = Path.Combine(GetCurrentDatabasePath(), $"{tableName}.table");
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

        public OperationStatus InsertIntoTable(string tableName, List<string> values)
        {
            try
            {
                string tablePath = Path.Combine(GetCurrentDatabasePath(), $"{tableName}.table");
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