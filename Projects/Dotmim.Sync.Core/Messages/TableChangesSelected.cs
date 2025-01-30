﻿using System;
using System.Runtime.Serialization;

namespace Dotmim.Sync
{
    /// <summary>
    /// Get changes to be applied (contains Deletes AND Inserts AND Updates).
    /// </summary>
    [DataContract(Name = "tcs"), Serializable]
    public class TableChangesSelected
    {

        /// <summary>
        /// Initializes a new instance of the <see cref="TableChangesSelected"/> class.
        /// Ctor for serialization purpose.
        /// </summary>
        public TableChangesSelected()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TableChangesSelected"/> class.
        /// </summary>
        public TableChangesSelected(string tableName, string schemaName)
        {
            this.TableName = tableName;
            this.SchemaName = schemaName;
        }

        /// <summary>
        /// Gets or sets the table name.
        /// </summary>
        [DataMember(Name = "n", IsRequired = false, EmitDefaultValue = false, Order = 1)]
        public string TableName { get; set; }

        /// <summary>
        /// Gets or sets get or Set the schema used for the DmTableSurrogate.
        /// </summary>
        [DataMember(Name = "s", IsRequired = false, EmitDefaultValue = false, Order = 2)]
        public string SchemaName { get; set; }

        /// <summary>
        /// Gets or sets the number of deletes that should be applied to a table during the synchronization session.
        /// </summary>
        [DataMember(Name = "d", IsRequired = false, Order = 3)]
        public int Deletes { get; set; }

        /// <summary>
        /// Gets or sets the number of updates OR inserts that should be applied to a table during the synchronization session.
        /// </summary>
        [DataMember(Name = "u", IsRequired = false, Order = 4)]
        public int Upserts { get; set; }

        /// <summary>
        /// Gets the total number of changes that are applied to a table during the synchronization session.
        /// </summary>
        [IgnoreDataMember]
        public int TotalChanges => this.Upserts + this.Deletes;

        /// <summary>
        /// Returns a string that represents the current object.
        /// </summary>
        public override string ToString()
        {
            string tn = string.IsNullOrEmpty(this.SchemaName) ? this.TableName : $"{this.SchemaName}.{this.TableName}";
            return $"{tn}: [{this.Upserts} inserts /{this.Upserts} deletes]";
        }
    }
}