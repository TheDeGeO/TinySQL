using StoreDataManager;
using Entities;

namespace QueryProcessor.Operations
{
    internal class DropDatabase
    {
        internal OperationStatus Execute(string databaseName)
        {
            return Store.GetInstance().DropDatabase(databaseName);
        }
    }
}