using Entities;
using StoreDataManager;

namespace QueryProcessor.Operations
{
    internal class Delete
    {
        internal OperationStatus Execute(string tableName, string whereClause)
        {
            return Store.GetInstance().Delete(tableName, whereClause);
        }
    }
}