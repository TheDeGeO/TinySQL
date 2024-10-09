using Entities;
using StoreDataManager;

namespace QueryProcessor.Operations
{
    internal class DropTable
    {
        internal OperationStatus Execute(string tableName)
        {
            return Store.GetInstance().DropTable(tableName);
        }
    }
}
