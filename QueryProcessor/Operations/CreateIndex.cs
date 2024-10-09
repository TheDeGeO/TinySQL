using Entities;
using StoreDataManager;
using QueryProcessor.Interfaces;

namespace QueryProcessor.Operations
{
    internal class CreateIndex
    {
        internal OperationStatus Execute(string indexName, string tableName, string columnName, string indexType)
        {
            return Store.GetInstance().CreateIndex(indexName, tableName, columnName, indexType);
        }
    }
}