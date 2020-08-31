using System.Collections.Generic;

namespace Dotmim.Sync.Manager
{
    /// <summary>
    /// Relation definition from the datastore.
    /// This class is used only when retrieving the relation definition from the datastore
    /// </summary>
    public class DbRelationDefinition
    {
        public string ForeignKey { get; set; }
        public string TableName { get; set; }
        public string SchemaName { get; set; }

        public List<DbRelationColumnDefinition> Columns { get; set; } = new List<DbRelationColumnDefinition>();
        public string ReferenceTableName { get; set; }
        public string ReferenceSchemaName { get; set; }

    }

    /// <summary>
    /// Each column from foreign key and reference key, with the order used
    /// </summary>
    public class DbRelationColumnDefinition
    {
        public string KeyColumnName { get; set; }
        public string ReferenceColumnName { get; set; }
        public int Order { get; set; }

    }
}
