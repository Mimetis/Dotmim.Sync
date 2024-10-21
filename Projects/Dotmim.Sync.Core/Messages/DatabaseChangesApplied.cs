using System;
using System.Collections.Generic;
using System.Runtime.Serialization;

namespace Dotmim.Sync
{
    /// <summary>
    /// All table changes applied on a provider.
    /// </summary>
    [DataContract(Name = "dca"), Serializable]
    public class DatabaseChangesApplied
    {

        /// <summary>
        /// Initializes a new instance of the <see cref="DatabaseChangesApplied"/> class.
        /// ctor for serialization purpose.
        /// </summary>
        public DatabaseChangesApplied()
        {
        }

        /// <summary>
        /// Gets or sets get the list of tables where we have changes applied.
        /// </summary>
        [DataMember(Name = "tca", IsRequired = false, EmitDefaultValue = false, Order = 1)]
        public List<TableChangesApplied> TableChangesApplied { get; set; } = [];

        /// <summary>
        /// Gets the total number of conflicts that have been applied resolved during the synchronization session.
        /// </summary>
        [IgnoreDataMember]
        public int TotalResolvedConflicts
        {
            get
            {
                int conflicts = 0;
                foreach (var tableProgress in this.TableChangesApplied)
                {
                    conflicts += tableProgress.ResolvedConflicts;
                }

                return conflicts;
            }
        }

        /// <summary>
        /// Gets the total number of changes that have been applied during the synchronization session.
        /// </summary>
        [IgnoreDataMember]
        public int TotalAppliedChanges
        {
            get
            {
                int changesApplied = 0;
                foreach (var tableProgress in this.TableChangesApplied)
                {
                    changesApplied += tableProgress.Applied;
                }

                return changesApplied;
            }
        }

        /// <summary>
        /// Gets the total number of changes that have failed to be applied during the synchronization session.
        /// </summary>
        [IgnoreDataMember]
        public int TotalAppliedChangesFailed
        {
            get
            {
                int changesFailed = 0;
                foreach (var tableProgress in this.TableChangesApplied)
                    changesFailed += tableProgress.Failed;

                return changesFailed;
            }
        }

        /// <summary>
        /// Gets the total number of changes that have been applied during the synchronization session.
        /// </summary>
        public override string ToString() => $"{this.TotalAppliedChanges} changes applied for {this.TableChangesApplied.Count} tables";
    }
}