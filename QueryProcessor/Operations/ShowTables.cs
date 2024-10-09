using Entities;
using QueryProcessor.Interfaces;
using StoreDataManager;

namespace QueryProcessor.Operations
{
    public class ShowTables : IOperation
    {
        public OperationStatus Execute()
        {
            return Store.GetInstance().ShowTables();
        }
    }
}
