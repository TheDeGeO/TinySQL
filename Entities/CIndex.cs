namespace Entities
{
    public class CIndex
    {
        public string Name { get; set; }
        public string TableName { get; set; }
        public string ColumnName { get; set; }
        public string IndexType { get; set; }

        public CIndex(string name, string tableName, string columnName, string indexType)
        {
            Name = name;
            TableName = tableName;
            ColumnName = columnName;
            IndexType = indexType;
        }
    }
}
