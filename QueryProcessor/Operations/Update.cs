using Entities;
using StoreDataManager;

namespace QueryProcessor.Operations
{
    internal class Update
    {
        internal OperationStatus Execute(string tableName, Dictionary<string, string> setClause, string whereClause)
        {
            return Store.GetInstance().UpdateTable(tableName, setClause, whereClause);
        }
    }
}