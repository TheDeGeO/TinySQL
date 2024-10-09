using Entities;
using QueryProcessor.Interfaces;
using StoreDataManager;

namespace QueryProcessor.Operations
{
    public class SetDatabase : IOperation
    {
        private string databaseName;

        public SetDatabase(string databaseName)
        {
            this.databaseName = databaseName;
        }

        public OperationStatus Execute()
        {
            return Store.GetInstance().SetDatabase(databaseName);
        }
    }
}
