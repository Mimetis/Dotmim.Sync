using Dotmim.Sync.Builders;

using Dotmim.Sync.Serialization;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;

namespace Dotmim.Sync
{
    [DataContract(Name = "ct"), Serializable]
    public class ContainerTable : SyncNamedItem<ContainerTable>
    {
        /// <summary>
        /// Gets or sets the name of the table that the DmTableSurrogate object represents.
        /// </summary>
        [DataMember(Name = "n", IsRequired = true, Order = 1)]
        public string TableName { get; set; }

        /// <summary>
        /// Get or Set the schema used for the DmTableSurrogate
        /// </summary>
        [DataMember(Name = "s", IsRequired = false, EmitDefaultValue = false, Order = 2)]
        public string SchemaName { get; set; }


        /// <summary>
        /// Get or Set the columns name used for the DmTableSurrogate
        /// </summary>
        [DataMember(Name = "c", IsRequired = false, EmitDefaultValue = false, Order = 3)]
        public List<ContainerTableColum> Columns { get; set; }

        /// <summary>
        /// List of rows
        /// </summary>
        [DataMember(Name = "r", IsRequired = false, Order = 4)]
        public List<object[]> Rows { get; set; } = new List<object[]>();

        public ContainerTable()
        {

        }

        public ContainerTable(SyncTable table)
        {
            this.TableName = table.TableName;
            this.SchemaName = table.SchemaName;
            this.Columns = new List<ContainerTableColum>();
            foreach (var column in table.Columns)
            {
                this.Columns.Add(new ContainerTableColum { ColumnName = column.ColumnName, TypeName = column.DataType, IsPrimaryKey = table.IsPrimaryKey(column) ? 1 : null });
            }

        }

        /// <summary>
        /// Check if we have rows in this container table
        /// </summary>
        public bool HasRows => this.Rows.Count > 0;

        public void Clear() => Rows.Clear();
        public override IEnumerable<string> GetAllNamesProperties()
        {
            yield return this.TableName;
            yield return this.SchemaName;

        }

    }


    [DataContract(Name = "c"), Serializable]
    public class ContainerTableColum
    {
        [DataMember(Name = "n", IsRequired = true, Order = 1)]
        public string ColumnName { get; set; }

        [DataMember(Name = "t", IsRequired = true, Order = 2)]
        public string TypeName { get; set; }

        [DataMember(Name = "p", IsRequired = false, Order = 3, EmitDefaultValue = false)]
        public byte? IsPrimaryKey { get; set; }

        public ContainerTableColum()
        {

        }
    }

}
