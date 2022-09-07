using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;

namespace Dotmim.Sync.Batch
{
    /// <summary>
    /// Represents a serialized table within in a batch file. with the table rows's count contained in the file
    /// </summary>
    [DataContract(Name = "t"), Serializable]
    [Obsolete]
    public class BatchPartTableInfo : SyncNamedItem<BatchPartTableInfo>
    {

        /// <summary>
        /// Represents a serialized table within in a batch file. with the table rows's count contained in the file
        /// </summary>
        public BatchPartTableInfo()
        {

        }

        /// <inheritdoc />
        public BatchPartTableInfo(string tableName, string schemaName = null, int rowsCount = 0)
        {
            this.TableName = tableName;
            this.SchemaName = schemaName;
            this.RowsCount = rowsCount;
        }

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
        /// Tables contained rows count
        /// </summary>
        [DataMember(Name = "rc", IsRequired = false, Order = 3)]
        public int RowsCount { get; set; }


        /// <summary>
        /// Get the name properties
        /// </summary>
        public override IEnumerable<string> GetAllNamesProperties()
        {
            yield return this.TableName;
            yield return this.SchemaName;

        }
    }
}
