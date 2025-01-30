using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;

namespace Dotmim.Sync
{

    /// <summary>
    /// All tables changes selected.
    /// </summary>
    [DataContract(Name = "dcs"), Serializable]
    public class DatabaseChangesSelected
    {
        /// <summary>
        /// Gets or sets get the changes selected to be applied for a current table.
        /// </summary>
        [DataMember(Name = "tcs", IsRequired = false, EmitDefaultValue = false, Order = 1)]
        public List<TableChangesSelected> TableChangesSelected { get; set; } = [];

        /// <summary>
        /// Gets the total number of changes that are to be applied during the synchronization session.
        /// </summary>
        [IgnoreDataMember]
        public int TotalChangesSelected => this.TableChangesSelected.Sum(t => t.TotalChanges);

        /// <summary>
        /// Gets the total number of deletes that are to be applied during the synchronization session.
        /// </summary>
        [IgnoreDataMember]
        public int TotalChangesSelectedDeletes => this.TableChangesSelected.Sum(t => t.Deletes);

        /// <summary>
        /// Gets the total number of updates OR inserts that are to be applied during the synchronization session.
        /// </summary>
        [IgnoreDataMember]
        public int TotalChangesSelectedUpdates => this.TableChangesSelected.Sum(t => t.Upserts);

        /// <summary>
        /// Gets the total number of inserts that are to be applied during the synchronization session.
        /// </summary>
        public override string ToString() => $"{this.TotalChangesSelected} changes selected for {this.TableChangesSelected.Count} tables";
    }
}