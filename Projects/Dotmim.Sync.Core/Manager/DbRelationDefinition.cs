using System;
using System.Collections.Generic;
using System.Text;

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
        public string ColumnName { get; set; }
        public string ReferenceTableName { get; set; }
        public string ReferenceColumnName { get; set; }

    }
}
