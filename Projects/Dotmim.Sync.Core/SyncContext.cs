using Dotmim.Sync.Enumerations;
using System;
using Dotmim.Sync.Filter;

namespace Dotmim.Sync
{
    /// <summary>
    /// Context of the current Sync session
    /// Encapsulates data changes and metadata for a synchronization session.
    /// </summary>
    [Serializable]
    public class SyncContext
    {
        /// <summary>
        /// Current Session, in progress
        /// </summary>
        public Guid SessionId { get; set; }

        /// <summary>Gets or sets the time when a sync sessionn started.
        /// </summary>
        public DateTime StartTime { get; set; }


        /// <summary>
        /// Gets or Sets the ScopeName for this sync session
        /// </summary>
        public String ScopeName { get; set; }

        /// <summary>
        /// <summary>Gets or sets the time when a sync session ended.
        /// </summary>
        public DateTime CompleteTime { get; set; }

        /// <summary>
        /// Gets or sets the sync type used during this session. Can be : Normal, Reinitialize, ReinitializeWithUpload
        /// </summary>
        public SyncType SyncType { get; set; }

        /// <summary>
        /// Gets or Sets the current Sync direction. 
        /// When locally GetChanges and remote ApplyChanges, we are in Upload direction
        /// When remote GetChanges and locally ApplyChanges, we are in Download direction
        /// this Property is used to check SyncDirection on each table.
        /// </summary>
        public SyncWay SyncWay { get; set; }

        /// <summary>
        /// Total number of change sets downloaded
        /// </summary>
        public int TotalChangesDownloaded { get; set; }

        /// <summary>
        /// Total number of change sets uploaded
        /// </summary>
        public int TotalChangesUploaded { get; set; }

        /// <summary>
        /// Total number of Sync Conflicts
        /// </summary>
        public int TotalSyncConflicts { get; set; }

        /// <summary>
        /// Total number of Sync Conflicts
        /// </summary>
        public int TotalSyncErrors { get; set; }

        /// <summary>
        /// Actual sync stage
        /// </summary>
        public SyncStage SyncStage { get; set; }

        /// <summary>
        /// Get or Sets the Sync parameter to pass to Remote provider for filtering rows
        /// </summary>
        public SyncParameterCollection Parameters { get; set; }

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
