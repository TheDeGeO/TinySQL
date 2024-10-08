using Entities;
using StoreDataManager;

namespace QueryProcessor.Operations
{
    internal class CreateTable
    {
        internal OperationStatus Execute(string tableName, List<(string Name, Type Type)> columns)
        {
            return Store.GetInstance().CreateTable(tableName, columns);
        }
    }
}
