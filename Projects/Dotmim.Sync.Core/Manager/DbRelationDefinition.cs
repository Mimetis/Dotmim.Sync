using System.Collections.Generic;

namespace Dotmim.Sync.Manager
{
    /// <summary>
    /// Relation definition from the datastore.
    /// This class is used only when retrieving the relation definition from the datastore.
    /// </summary>
    public class DbRelationDefinition
    {
        /// <summary>
        /// Gets or Sets the foreign key name.
        /// </summary>
        public string ForeignKey { get; set; }

        /// <summary>
        /// Gets or Sets the table name.
        /// </summary>
        public string TableName { get; set; }

        /// <summary>
        /// Gets or Sets the schema name.
        /// </summary>
        public string SchemaName { get; set; }

        /// <summary>
        /// Gets the table columns collection.
        /// </summary>
        public IList<DbRelationColumnDefinition> Columns { get; } = [];

        /// <summary>
        /// Gets or Sets the reference table name.
        /// </summary>
        public string ReferenceTableName { get; set; }

        /// <summary>
        /// Gets or Sets the reference schema name.
        /// </summary>
        public string ReferenceSchemaName { get; set; }
    }

    /// <summary>
    /// Each column from foreign key and reference key, with the order used.
    /// </summary>
    public class DbRelationColumnDefinition
    {
        /// <summary>
        /// Gets or Sets the key column name.
        /// </summary>
        public string KeyColumnName { get; set; }

        /// <summary>
        /// Gets or Sets the reference column name.
        /// </summary>
        public string ReferenceColumnName { get; set; }

        /// <summary>
        /// Gets or Sets the order.
        /// </summary>
        public int Order { get; set; }
    }
}