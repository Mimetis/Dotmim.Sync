

using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Serialization;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.Batch
{
    /// <summary>
    /// Info about a BatchPart
    /// Will be serialized in the BatchInfo file
    /// </summary>
    [DataContract(Name = "batchpartinfo"), Serializable]
    public class BatchPartInfo : SyncNamedItem<BatchPartInfo>
    {
        /// <summary>
        /// Batch part file name
        /// </summary>
        [DataMember(Name = "file", IsRequired = true, Order = 1)]
        public string FileName { get; set; }

        /// <summary>
        /// Ordered batch part file index
        /// </summary>
        [DataMember(Name = "index", IsRequired = true, Order = 2)]
        public int Index { get; set; }

        /// <summary>
        /// Gets if the batch part file is the last one
        /// </summary>
        [DataMember(Name = "last", IsRequired = true, Order = 3)]
        public bool IsLastBatch { get; set; }

        /// <summary>
        /// Tables contained in the batchpart (NEW v0.9.3 : Only One table per file)
        /// </summary>
        [Obsolete]
        [DataMember(Name = "tables", IsRequired = true, Order = 4)]
        public BatchPartTableInfo[] Tables { get; set; }

        /// <summary>
        /// Tables contained rows count
        /// </summary>
        [DataMember(Name = "rc", IsRequired = false, Order = 5)]
        public int RowsCount { get; set; }


        /// <summary>
        /// Gets or sets the name of the table that the DmTableSurrogate object represents.
        /// </summary>
        [DataMember(Name = "tn", IsRequired = true, Order = 6)]
        public string TableName { get; set; }

        /// <summary>
        /// Get or Set the schema used for the DmTableSurrogate
        /// </summary>
        [DataMember(Name = "ts", IsRequired = false, EmitDefaultValue = false, Order = 7)]
        public string SchemaName { get; set; }

        /// <summary>
        /// ctor for serialization purpose
        /// </summary>
        public BatchPartInfo()
        {
        }

        /// <summary>
        /// ctor for serialization purpose
        /// </summary>
        public BatchPartInfo(string fileName, string tableName, string schemaName, int rowsCount = 0, int index = 0)
        {
            this.FileName = fileName;
            this.TableName = tableName;
            this.SchemaName = schemaName;
            this.RowsCount = rowsCount;
            this.Index = index;
        }

        /// <summary>
        /// Return batch part table name
        /// </summary>
        public override string ToString()
        {
            if (!string.IsNullOrEmpty(this.TableName))
            {
                if (!string.IsNullOrEmpty(this.SchemaName))
                    return $"{this.SchemaName}.{this.TableName} [{this.RowsCount}]";
                else
                    return $"{this.TableName} [{this.RowsCount}]";

            }
            else
            {

                if (this.Tables == null || this.Tables.Length <= 0)
                    return base.ToString();

                var table = this.Tables[0];

                if (!string.IsNullOrEmpty(table.SchemaName))
                    return $"{table.SchemaName}.{table.TableName}";
                else
                    return table.TableName;
            }
        }

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
