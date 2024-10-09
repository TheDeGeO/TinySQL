using Entities;
using StoreDataManager;
using QueryProcessor.Interfaces;
namespace QueryProcessor.Operations
{
    internal class InsertIntoTable
    {
        internal OperationStatus Execute(string tableName, List<string> values)
        {
            return Store.GetInstance().InsertIntoTable(tableName, values);
        }
    }
}