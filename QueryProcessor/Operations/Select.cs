using Entities;
using StoreDataManager;
using System;
using QueryProcessor.Interfaces;

namespace QueryProcessor.Operations
{
    public class Select : IOperation
    {
        private string tableName;
        private List<string> columns;
        private string whereClause;
        private string orderByClause;

        // Add a constructor to set the table name
        public Select(string tableName, List<string> columns, string whereClause, string orderByClause)
        {
            this.tableName = tableName;
            this.columns = columns;
            this.whereClause = whereClause;
            this.orderByClause = orderByClause;
        }

        public OperationStatus Execute()
        {
            // This is only doing the query but not returning results.
            var result = Store.GetInstance().Select(tableName, columns, whereClause, orderByClause); // Pass the table name here
            return result;
        }
    }
}
