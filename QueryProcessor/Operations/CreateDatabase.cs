using Entities;
using QueryProcessor.Interfaces;
using StoreDataManager;

namespace QueryProcessor.Operations
{
    public class CreateDatabase : IOperation
    {
        private string databaseName;

        public CreateDatabase(string databaseName)
        {
            this.databaseName = databaseName;
        }

        public OperationStatus Execute()
        {
            return Store.GetInstance().CreateDatabase(databaseName);
        }
    }
}
