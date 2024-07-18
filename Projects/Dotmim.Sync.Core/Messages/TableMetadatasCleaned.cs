using System;
using System.Runtime.Serialization;

namespace Dotmim.Sync
{

    /// <summary>
    /// TableMetadatasCleaned is a class that contains the summary of metadatas cleaned for one table.
    /// </summary>
    [DataContract(Name = "tmc"), Serializable]
    public class TableMetadatasCleaned
    {
        /// <summary>
        /// Gets or sets the name of the table that the DmTableSurrogate object represents.
        /// </summary>
        [DataMember(Name = "tn", IsRequired = true, Order = 1)]
        public string TableName { get; set; }

        /// <summary>
        /// Gets or sets get or Set the schema used for the DmTableSurrogate.
        /// </summary>
        [DataMember(Name = "sn", IsRequired = false, EmitDefaultValue = false, Order = 2)]
        public string SchemaName { get; set; }

        /// <summary>
        /// Gets or Sets the last timestamp used as the limit to clean the table metadatas. All rows below this limit have beed cleaned.
        /// </summary>
        [DataMember(Name = "ttl", IsRequired = true, Order = 3)]
        public long TimestampLimit { get; set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="TableMetadatasCleaned"/> class.
        /// Createa new instance of a summary of metadatas cleaned for one table.
        /// </summary>
        public TableMetadatasCleaned(string tableName, string schemaName)
        {
            this.TableName = tableName;
            this.SchemaName = schemaName;
        }

        /// <summary>
        /// Gets or Sets the metadatas rows count, that have been cleaned.
        /// </summary>
        [DataMember(Name = "rcc", IsRequired = true, Order = 4)]
        public int RowsCleanedCount { get; set; }
    }
}