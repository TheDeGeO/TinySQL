using Entities;
using StoreDataManager;
using QueryProcessor.Interfaces;

namespace QueryProcessor.Operations
{
    internal class DescribeTable
    {
        internal OperationStatus Execute(string tableName)
        {
            return Store.GetInstance().DescribeTable(tableName);
        }
    }
}