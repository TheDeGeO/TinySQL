using QueryProcessor.Interfaces;
using Entities;
using StoreDataManager;

namespace QueryProcessor.Operations
{
    public class ShowDatabases : IOperation
    {
        public OperationStatus Execute()
        {
            return Store.GetInstance().ShowDatabases();
        }
    }
}
