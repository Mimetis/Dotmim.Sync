using Dotmim.Sync.Enumerations;
using System;
using Dotmim.Sync.Filter;
using System.Runtime.Serialization;

namespace Dotmim.Sync
{
    /// <summary>
    /// Context of the current Sync session
    /// Encapsulates data changes and metadata for a synchronization session.
    /// </summary>
    [DataContract(Name = "ctx"), Serializable]
    public class SyncContext
    {
        /// <summary>
        /// Current Session, in progress
        /// </summary>
        [DataMember(Name = "id", IsRequired = true, Order = 1)]
        public Guid SessionId { get; set; }

        /// <summary>Gets or sets the time when a sync sessionn started.
        /// </summary>
        [DataMember(Name = "st", IsRequired = false, EmitDefaultValue = false, Order = 2)]
        public DateTime StartTime { get; set; }

        /// <summary>
        /// Gets or Sets the ScopeName for this sync session
        /// </summary>
        [DataMember(Name = "sn", IsRequired = false, EmitDefaultValue = false, Order = 3)]
        public string ScopeName { get; set; }

        /// <summary>
        /// <summary>Gets or sets the time when a sync session ended.
        /// </summary>
        [DataMember(Name = "ct", IsRequired = false, EmitDefaultValue = false, Order = 4)]
        public DateTime CompleteTime { get; set; }

        /// <summary>
        /// Gets or sets the sync type used during this session. Can be : Normal, Reinitialize, ReinitializeWithUpload
        /// </summary>
        [DataMember(Name = "typ", IsRequired = false, EmitDefaultValue = false, Order = 5)]
        public SyncType SyncType { get; set; }

        /// <summary>
        /// Gets or Sets the current Sync direction. 
        /// When locally GetChanges and remote ApplyChanges, we are in Upload direction
        /// When remote GetChanges and locally ApplyChanges, we are in Download direction
        /// this Property is used to check SyncDirection on each table.
        /// </summary>
        [DataMember(Name = "way", IsRequired = false, EmitDefaultValue = false, Order = 6)]
        public SyncWay SyncWay { get; set; }

        /// <summary>
        /// Total number of change sets downloaded
        /// </summary>
        [DataMember(Name = "tcd", IsRequired = false, EmitDefaultValue = false, Order = 7)]
        public int TotalChangesDownloaded { get; set; }

        /// <summary>
        /// Total number of change sets uploaded
        /// </summary>
        [DataMember(Name = "tcu", IsRequired = false, EmitDefaultValue = false, Order = 8)]
        public int TotalChangesUploaded { get; set; }

        /// <summary>
        /// Total number of Sync Conflicts
        /// </summary>
        [DataMember(Name = "tsc", IsRequired = false, EmitDefaultValue = false, Order = 9)]
        public int TotalSyncConflicts { get; set; }

        /// <summary>
        /// Total number of Sync Conflicts
        /// </summary>
        [DataMember(Name = "tse", IsRequired = false, EmitDefaultValue = false, Order = 10)]
        public int TotalSyncErrors { get; set; }

        /// <summary>
        /// Actual sync stage
        /// </summary>
        [DataMember(Name = "stage", IsRequired = false, EmitDefaultValue = false, Order = 11)]
        public SyncStage SyncStage { get; set; }

        /// <summary>
        /// Get or Sets the Sync parameter to pass to Remote provider for filtering rows
        /// </summary>
        [DataMember(Name = "ps", IsRequired = false, EmitDefaultValue = false, Order = 12)]
        public SyncParameterCollection Parameters { get; set; }

        public SyncContext()
        {

        }
        /// <summary>
        /// Ctor. New sync context with a new Guid
        /// </summary>
        public SyncContext(Guid sessionId)
        {
            this.SessionId = sessionId;
        }

        /// <summary>
        /// Get the result if sync session is ended
        /// </summary>
        public override string ToString()
        {
            if (this.CompleteTime != this.StartTime && this.CompleteTime > this.StartTime)
            {
                var tsEnded = TimeSpan.FromTicks(CompleteTime.Ticks);
                var tsStarted = TimeSpan.FromTicks(StartTime.Ticks);

                var durationTs = tsEnded.Subtract(tsStarted);
                var durationstr = $"{durationTs.Hours}:{durationTs.Minutes}:{durationTs.Seconds}.{durationTs.Milliseconds}";

                return ($"Synchronization done. " + Environment.NewLine +
                        $"\tTotal changes downloaded: {TotalChangesDownloaded} " + Environment.NewLine +
                        $"\tTotal changes uploaded: {TotalChangesUploaded}" + Environment.NewLine +
                        $"\tTotal conflicts: {TotalSyncConflicts}" + Environment.NewLine +
                        $"\tTotal duration :{durationstr} ");

            }
            return base.ToString();
        }
    }
}
