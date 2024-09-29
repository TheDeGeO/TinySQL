using Entities;
using StoreDataManager;

namespace QueryProcessor.Operations
{
    internal class CreateTable
    {
        internal OperationStatus Execute()
        {
            return Store.GetInstance().CreateTable();
        }
    }
}
