using System;
using System.Collections.Generic;
using System.Runtime.Serialization;

namespace Dotmim.Sync
{
    /// <summary>
    /// Represents a column identifier.
    /// </summary>
    [DataContract(Name = "sci"), Serializable]
    public class SyncColumnIdentifier : SyncNamedItem<SyncColumnIdentifier>
    {
        /// <summary>
        /// Gets or Sets the column name.
        /// </summary>
        [DataMember(Name = "c", IsRequired = true, Order = 1)]
        public string ColumnName { get; set; }

        /// <summary>
        /// Gets or Sets the table name.
        /// </summary>
        [DataMember(Name = "t", IsRequired = true, Order = 2)]
        public string TableName { get; set; }

        /// <summary>
        /// Gets or Sets the schema name.
        /// </summary>
        [DataMember(Name = "s", IsRequired = false, EmitDefaultValue = false, Order = 3)]
        public string SchemaName { get; set; }

        /// <inheritdoc cref="SyncColumnIdentifier"/>
        public SyncColumnIdentifier()
        {
        }

        /// <inheritdoc cref="SyncColumnIdentifier"/>
        public SyncColumnIdentifier(string columnName, string tableName, string schemaName = null)
        {
            this.TableName = tableName;
            this.SchemaName = schemaName;
            this.ColumnName = columnName;
        }

        /// <summary>
        /// Clone the current SyncColumnIdentifier.
        /// </summary>
        public SyncColumnIdentifier Clone() => new()
        {
            ColumnName = this.ColumnName,
            SchemaName = this.SchemaName,
            TableName = this.TableName,
        };

        /// <summary>
        /// return the string representation of the SyncColumnIdentifier.
        /// </summary>
        public override string ToString()
        {
            if (string.IsNullOrEmpty(this.SchemaName))
                return $"{this.TableName}-{this.ColumnName}";
            else
                return $"{this.SchemaName}.{this.TableName}-{this.ColumnName}";
        }

        /// <inheritdoc cref="SyncNamedItem{T}.GetAllNamesProperties"/>
        public override IEnumerable<string> GetAllNamesProperties()
        {
            yield return this.TableName;
            yield return this.SchemaName;
            yield return this.ColumnName;
        }
    }
}