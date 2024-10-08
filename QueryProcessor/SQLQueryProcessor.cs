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
            /// The following is example code. Parser should be called
            /// on the sentence to understand and process what is requested
            if (sentence.StartsWith("CREATE TABLE"))
            {
                // Extract table name and column definitions from the sentence
                string tableName = ExtractTableName(sentence);
                List<(string Name, Type Type)> columns = ExtractColumnDefinitions(sentence);
                return new CreateTable().Execute(tableName, columns);
            }   
            if (sentence.StartsWith("SELECT"))
            {
                string tableName = ExtractTableName(sentence);
                return new Select(tableName).Execute();
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
            else
            {
                throw new UnknownSQLSentenceException();
            }
        }

        public static string ExtractTableName(string sentence)
        {
            return sentence.Split(' ')[2]; // Assumes table name is the third word
        }

        public static string ExtractDatabaseName(string sentence)
        {
            return sentence.Split(' ')[2]; // Assumes database name is the third word
        }

        public static List<(string Name, Type Type)> ExtractColumnDefinitions(string sentence)
        {
            return new List<(string Name, Type Type)>();
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
