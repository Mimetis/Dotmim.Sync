using Dotmim.Sync.Enumerations;
using System;
using System.Collections.Generic;
using System.Runtime.Serialization;

namespace Dotmim.Sync.Batch
{
    /// <summary>
    /// Info about a BatchPart
    /// Will be serialized in the BatchInfo file.
    /// </summary>
    [DataContract(Name = "batchpartinfo"), Serializable]
    public class BatchPartInfo : SyncNamedItem<BatchPartInfo>
    {
        /// <summary>
        /// Gets or sets batch part file name.
        /// </summary>
        [DataMember(Name = "file", IsRequired = true, Order = 1)]
        public string FileName { get; set; }

        /// <summary>
        /// Gets or sets ordered batch part file index.
        /// </summary>
        [DataMember(Name = "index", IsRequired = true, Order = 2)]
        public int Index { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether gets if the batch part file is the last one.
        /// </summary>
        [DataMember(Name = "last", IsRequired = true, Order = 3)]
        public bool IsLastBatch { get; set; }

        /// <summary>
        /// Gets or sets tables contained rows count.
        /// </summary>
        [DataMember(Name = "rc", IsRequired = false, Order = 5)]
        public int RowsCount { get; set; }

        /// <summary>
        /// Gets or sets the name of the table that the DmTableSurrogate object represents.
        /// </summary>
        [DataMember(Name = "tn", IsRequired = true, Order = 6)]
        public string TableName { get; set; }

        /// <summary>
        /// Gets or sets get or Set the schema used for the DmTableSurrogate.
        /// </summary>
        [DataMember(Name = "ts", IsRequired = false, EmitDefaultValue = false, Order = 7)]
        public string SchemaName { get; set; }

        /// <summary>
        /// Gets or sets get or Set the schema used for the DmTableSurrogate.
        /// </summary>
        [DataMember(Name = "state", IsRequired = false, EmitDefaultValue = false, Order = 8)]
        public SyncRowState State { get; set; } = SyncRowState.None;

        /// <summary>
        /// Initializes a new instance of the <see cref="BatchPartInfo"/> class.
        /// ctor for serialization purpose.
        /// </summary>
        public BatchPartInfo()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BatchPartInfo"/> class.
        /// ctor for serialization purpose.
        /// </summary>
        public BatchPartInfo(string fileName, string tableName, string schemaName, SyncRowState state, int rowsCount = 0, int index = 0)
        {
            this.FileName = fileName;
            this.TableName = tableName;
            this.SchemaName = schemaName;
            this.RowsCount = rowsCount;
            this.Index = index;
            this.State = state;
        }

        /// <summary>
        /// Return batch part table name.
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

            return base.ToString();
        }

        /// <inheritdoc cref="SyncNamedItem{T}.GetAllNamesProperties"/>
        public override IEnumerable<string> GetAllNamesProperties()
        {
            yield return this.TableName;
            yield return this.SchemaName;
        }
    }
}