using System;
using System.Collections.Generic;
using System.Runtime.Serialization;

namespace Dotmim.Sync
{
    /// <summary>
    /// SyncFilterJoin represents a join between two tables for a filter.
    /// </summary>
    [DataContract(Name = "sfj"), Serializable]
    public class SyncFilterJoin : SyncNamedItem<SyncFilterJoin>
    {

        /// <summary>
        /// Ensure filter parameter as the correct schema (since the property is not serialized).
        /// </summary>
        public void EnsureFilterJoin(SyncSet schema) => this.Schema = schema;

        /// <summary>
        /// Gets or sets the ShemaTable's SyncSchema.
        /// </summary>
        [IgnoreDataMember]
        public SyncSet Schema { get; set; }

        /// <summary>
        /// Gets or sets the join type.
        /// </summary>
        [DataMember(Name = "je", IsRequired = true, Order = 1)]
        public Join JoinEnum { get; set; }

        /// <summary>
        /// Gets or sets the table name.
        /// </summary>
        [DataMember(Name = "tbl", IsRequired = true, Order = 2)]
        public string TableName { get; set; }

        /// <summary>
        /// Gets or sets the left table name.
        /// </summary>
        [DataMember(Name = "ltbl", IsRequired = true, Order = 3)]
        public string LeftTableName { get; set; }

        /// <summary>
        /// Gets or sets the left column name.
        /// </summary>
        [DataMember(Name = "lcol", IsRequired = true, Order = 4)]
        public string LeftColumnName { get; set; }

        /// <summary>
        /// Gets or sets the right table name.
        /// </summary>
        [DataMember(Name = "rtbl", IsRequired = true, Order = 5)]
        public string RightTableName { get; set; }

        /// <summary>
        /// Gets or sets the right column name.
        /// </summary>
        [DataMember(Name = "rcol", IsRequired = true, Order = 6)]
        public string RightColumnName { get; set; }

        /// <summary>
        /// Gets or sets the schema name.
        /// </summary>
        [DataMember(Name = "tblsn", IsRequired = false, Order = 7)]
        public string TableSchemaName { get; set; }

        /// <summary>
        /// Gets or sets the left table schema name.
        /// </summary>
        [DataMember(Name = "ltblsn", IsRequired = false, Order = 8)]
        public string LeftTableSchemaName { get; set; }

        /// <summary>
        /// Gets or sets the right table schema name.
        /// </summary>
        [DataMember(Name = "rtblsn", IsRequired = false, Order = 9)]
        public string RightTableSchemaName { get; set; }

        /// <inheritdoc cref="SyncFilterJoin"/>
        public SyncFilterJoin()
        {
        }

        /// <inheritdoc cref="SyncNamedItem{T}.GetAllNamesProperties"/>
        public override IEnumerable<string> GetAllNamesProperties()
        {
            yield return this.JoinEnum.ToString();
            yield return this.TableName;
            yield return this.LeftColumnName;
            yield return this.LeftTableName;
            yield return this.RightColumnName;
            yield return this.TableSchemaName;
            yield return this.LeftTableSchemaName;
            yield return this.RightTableSchemaName;
        }
    }
}