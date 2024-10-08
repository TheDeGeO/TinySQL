using Entities;
using StoreDataManager;
using System;
using QueryProcessor.Interfaces;

namespace QueryProcessor.Operations
{
    public class Select : IOperation
    {
        private string tableName;

        // Add a constructor to set the table name
        public Select(string tableName)
        {
            this.tableName = tableName;
        }

        public OperationStatus Execute()
        {
            // This is only doing the query but not returning results.
            var result = Store.GetInstance().Select(tableName); // Pass the table name here
            return result;
        }
    }
}
