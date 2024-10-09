using Entities;
using QueryProcessor.Exceptions;
using QueryProcessor.Operations;
using StoreDataManager;

namespace QueryProcessor
{
    public class SQLQueryProcessor
    {
        public static OperationStatus Execute(string sentence)
        {
            if (sentence.StartsWith("SET DATABASE"))
            {
                string databaseName = ExtractDatabaseName(sentence);
                return new SetDatabase(databaseName).Execute();
            }
            if (sentence.StartsWith("CREATE TABLE"))
            {
                // Extract table name and column definitions from the sentence
                string tableName = ExtractTableName(sentence);
                List<(string Name, Type Type)> columns = ExtractColumnDefinitions(sentence);
                Console.WriteLine(tableName);
                return new CreateTable().Execute(tableName, columns);
            }   
            if (sentence.StartsWith("SELECT"))
            {
                string tableName = ExtractTableName(sentence);
                List<string> columns = ExtractColumns(sentence);
                string whereClause = ExtractWhereClause(sentence);
                string orderByClause = ExtractOrderByClause(sentence);
                return new Select(tableName, columns, whereClause, orderByClause).Execute();
            }
            if (sentence.StartsWith("CREATE DATABASE"))
            {
                string databaseName = ExtractDatabaseName(sentence);
                return new CreateDatabase(databaseName).Execute();
            }
            if (sentence.StartsWith("SHOW DATABASES"))
            {
                return new ShowDatabases().Execute();
            }
            if (sentence.StartsWith("SHOW TABLES"))
            {
                return new ShowTables().Execute();
            }
            if (sentence.StartsWith("DROP TABLE"))
            {
                string tableName = ExtractTableName(sentence);
                return new DropTable().Execute(tableName);
            }
            if (sentence.StartsWith("INSERT INTO"))
            {
                string tableName = ExtractTableName(sentence);
                List<string> values = ExtractValues(sentence);
                return new InsertIntoTable().Execute(tableName, values);
            }
            if (sentence.StartsWith("CREATE INDEX"))
            {
                string indexName = ExtractIndexName(sentence);
                string tableName = ExtractTableNameForIndex(sentence);
                string columnName = ExtractColumnName(sentence);
                string indexType = ExtractIndexType(sentence);
                return new CreateIndex().Execute(indexName, tableName, columnName, indexType);
            }
            if (sentence.StartsWith("DESCRIBE"))
            {
                string tableName = ExtractTableName(sentence);
                return new DescribeTable().Execute(tableName);
            }
            else
            {
                throw new UnknownSQLSentenceException();
            }
        }

        public static List<string> ExtractColumns(string sentence)
        {
            int fromIndex = sentence.IndexOf(" FROM ", StringComparison.OrdinalIgnoreCase);
            if (fromIndex == -1) return new List<string>();
            
            string columnsString = sentence.Substring(6, fromIndex - 6).Trim();
            return columnsString == "*" 
                ? new List<string> { "*" } 
                : columnsString.Split(',').Select(c => c.Trim()).ToList();
        }

        public static string ExtractWhereClause(string sentence)
        {
            int whereIndex = sentence.IndexOf(" WHERE ");
            return whereIndex != -1 ? sentence.Substring(whereIndex + 6).Trim() : string.Empty;
        }

        public static string ExtractOrderByClause(string sentence)
        {
            int orderByIndex = sentence.IndexOf(" ORDER BY ", StringComparison.OrdinalIgnoreCase);
            if (orderByIndex == -1)
                return string.Empty;

            string orderByClause = sentence.Substring(orderByIndex + 9).Trim();
            string[] parts = orderByClause.Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);

            if (parts.Length >= 2)
            {
                string direction = parts[parts.Length - 1].ToUpper();
                if (direction == "ASC" || direction == "DESC")
                {
                    return orderByClause;
                }
            }

            // If no direction is specified, append "ASC" as the default
            return orderByClause + " ASC";
        }

        public static string ExtractIndexName(string sentence)
        {
            string[] words = sentence.Split(' ');
            return words[2]; // Assumes index name is the third word
        }

        public static string ExtractTableNameForIndex(string sentence)
        {
            int onIndex = sentence.IndexOf(" ON ");
            int openParenIndex = sentence.IndexOf('(', onIndex);
            return sentence.Substring(onIndex + 4, openParenIndex - (onIndex + 4)).Trim();
        }

        public static string ExtractColumnName(string sentence)
        {
            int openParenIndex = sentence.IndexOf('(');
            int closeParenIndex = sentence.IndexOf(')', openParenIndex);
            return sentence.Substring(openParenIndex + 1, closeParenIndex - openParenIndex - 1).Trim();
        }

        public static string ExtractIndexType(string sentence)
        {
            int typeIndex = sentence.IndexOf(" OF TYPE ");
            return sentence.Substring(typeIndex + 9).Trim();
        }

        public static List<string> ExtractValues(string sentence)
        {
            string[] words = sentence.Split(' ');
            int fromIndex = Array.IndexOf(words, "VALUES");
            return words.Skip(fromIndex + 1).ToList();
        }

        public static string ExtractTableName(string sentence)
        {
            string[] words = sentence.Split(' ');
            int fromIndex = Array.IndexOf(words, "FROM");
            int intoIndex = Array.IndexOf(words, "INTO");
            int describeIndex = Array.IndexOf(words, "DESCRIBE");
            
            if (fromIndex != -1 && fromIndex + 1 < words.Length)
            {
                return words[fromIndex + 1];
            }
            else if (intoIndex != -1 && intoIndex + 1 < words.Length)
            {
                return words[intoIndex + 1];
            }
            else if (describeIndex != -1 && describeIndex + 1 < words.Length)
            {
                return words[describeIndex + 1];
            }
            
            // Fallback to the original logic if none of the keywords are found or there's no word after it
            return words.Length > 2 ? words[2] : string.Empty;
        }

        public static string ExtractDatabaseName(string sentence)
        {
            return sentence.Split(' ')[2]; // Assumes database name is the third word
        }

        public static List<(string Name, Type Type)> ExtractColumnDefinitions(string sentence)
        {
            var columns = new List<(string Name, Type Type)>();
            
            int startIndex = sentence.IndexOf('(') + 1;
            int endIndex = sentence.LastIndexOf(')');
            if (startIndex > 0 && endIndex > startIndex)
            {
                string columnDefinitions = sentence.Substring(startIndex, endIndex - startIndex);
                string[] columnDefs = columnDefinitions.Split(',', StringSplitOptions.TrimEntries);
                
                foreach (var columnDef in columnDefs)
                {
                    string[] parts = columnDef.Split(new[] { ' ' }, 2, StringSplitOptions.TrimEntries);
                    if (parts.Length >= 2)
                    {
                        string name = parts[0];
                        string typeString = parts[1].Split(' ')[0]; // Get the type, ignore constraints
                        Type type = MapSqlTypeToSystemType(typeString);
                        columns.Add((name, type));
                    }
                }
            }
            
            return columns;
        }

        private static Type MapSqlTypeToSystemType(string sqlType)
        {
            return sqlType.ToUpper() switch
            {
                "INT" => typeof(int),
                "VARCHAR" => typeof(string),
                // Add more type mappings as needed
                _ => typeof(string) // Default to string for unknown types
            };
        }
    }

}

namespace QueryProcessor.Interfaces
{
    public interface IOperation
    {
        OperationStatus Execute();
    }
}
