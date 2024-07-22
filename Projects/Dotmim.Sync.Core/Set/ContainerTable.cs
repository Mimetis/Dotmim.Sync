using System;
using System.Collections.Generic;
using System.Runtime.Serialization;

namespace Dotmim.Sync
{
    /// <summary>
    /// ContainerTable is a table with columns and rows to be sent over the wire.
    /// </summary>
    [DataContract(Name = "ct"), Serializable]
    public class ContainerTable : SyncNamedItem<ContainerTable>
    {
        /// <summary>
        /// Gets or sets the name of the table that the DmTableSurrogate object represents.
        /// </summary>
        [DataMember(Name = "n", IsRequired = true, Order = 1)]
        public string TableName { get; set; }

        /// <summary>
        /// Gets or sets get or Set the schema used for the DmTableSurrogate.
        /// </summary>
        [DataMember(Name = "s", IsRequired = false, EmitDefaultValue = false, Order = 2)]
        public string SchemaName { get; set; }

        /// <summary>
        /// Gets or sets get or Set the columns name used for the DmTableSurrogate.
        /// </summary>
        [DataMember(Name = "c", IsRequired = false, EmitDefaultValue = false, Order = 3)]
        public List<ContainerTableColum> Columns { get; set; }

        /// <summary>
        /// Gets or sets list of rows.
        /// </summary>
        [DataMember(Name = "r", IsRequired = false, Order = 4)]

        // [JsonConverter(typeof(ArrayJsonConverter))]
        public List<object[]> Rows { get; set; } = new List<dynamic[]>();

        /// <inheritdoc cref="ContainerTable"/>
        public ContainerTable()
        {
        }

        /// <inheritdoc cref="ContainerTable"/>
        public ContainerTable(SyncTable table)
        {
            this.TableName = table.TableName;
            this.SchemaName = table.SchemaName;
            this.Columns = [];
            foreach (var column in table.Columns)
            {
                this.Columns.Add(new ContainerTableColum { ColumnName = column.ColumnName, TypeName = column.DataType, IsPrimaryKey = table.IsPrimaryKey(column) ? 1 : null });
            }
        }

        /// <summary>
        /// Gets a value indicating whether check if we have rows in this container table.
        /// </summary>
        public bool HasRows => this.Rows.Count > 0;

        public void Clear() => this.Rows.Clear();

        /// <inheritdoc cref="SyncNamedItem{T}.GetAllNamesProperties"/>
        public override IEnumerable<string> GetAllNamesProperties()
        {
            yield return this.TableName;
            yield return this.SchemaName;
        }
    }

    /// <summary>
    /// Represents a column in a container table.
    /// </summary>
    [DataContract(Name = "c"), Serializable]
    public class ContainerTableColum
    {
        /// <summary>
        /// Gets or sets the name of the column.
        /// </summary>
        [DataMember(Name = "n", IsRequired = true, Order = 1)]
        public string ColumnName { get; set; }

        /// <summary>
        /// Gets or sets the type of the column.
        /// </summary>
        [DataMember(Name = "t", IsRequired = true, Order = 2)]
        public string TypeName { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether the column is a primary key.
        /// </summary>
        [DataMember(Name = "p", IsRequired = false, Order = 3, EmitDefaultValue = false)]
        public byte? IsPrimaryKey { get; set; }

        /// <inheritdoc cref="ContainerTableColum"/>
        public ContainerTableColum()
        {
        }
    }
}