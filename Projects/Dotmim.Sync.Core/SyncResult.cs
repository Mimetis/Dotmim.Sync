using Dotmim.Sync.Enumerations;
using System;

using System.Runtime.Serialization;

namespace Dotmim.Sync
{
    /// <summary>
    /// Context of the current Sync session
    /// Encapsulates data changes and metadata for a synchronization session.
    /// </summary>
    public class SyncResult
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
        public string ScopeName { get; set; }

        /// <summary>
        /// <summary>Gets or sets the time when a sync session ended.
        /// </summary>
        public DateTime CompleteTime { get; set; }


        /// <summary>
        /// Gets the number of changes applied on the client
        /// </summary>
        public int TotalChangesApplied => this.ChangesAppliedOnClient.TotalAppliedChanges;

        /// <summary>
        /// Gets total number of changes downloaded from server. 
        /// </summary>
        public int TotalChangesDownloaded => this.ServerChangesSelected.TotalChangesSelected;

        /// <summary>
        /// Gets the number of change uploaded to the server
        /// </summary>
        public int TotalChangesUploaded => this.ClientChangesSelected.TotalChangesSelected;

        /// <summary>
        /// Gets the number of conflicts resolved
        /// </summary>
        public int TotalResolvedConflicts =>
            Math.Max(this.ChangesAppliedOnClient.TotalResolvedConflicts, this.ChangesAppliedOnServer.TotalResolvedConflicts);

        /// <summary>
        /// Gets the number of sync errors
        /// </summary>
        public int TotalSyncErrors { get; set; }


        /// <summary>
        /// Gets or Sets the summary of client changes that where applied on the server
        /// </summary>
        public DatabaseChangesApplied ChangesAppliedOnServer { get; set; }


        /// <summary>
        /// Gets or Sets the summary of server changes that where applied on the client
        /// </summary>
        public DatabaseChangesApplied ChangesAppliedOnClient { get; set; }


        /// <summary>
        /// Gets or Sets the summary of client changes to be applied on the server
        /// </summary>
        public DatabaseChangesSelected ClientChangesSelected { get; set; }


        /// <summary>
        /// Gets or Sets the summary of server changes selected to be applied on the client
        /// </summary>
        public DatabaseChangesSelected ServerChangesSelected { get; set; }

        public SyncResult()
        {

        }
        /// <summary>
        /// Ctor. New sync context with a new Guid
        /// </summary>
        public SyncResult(Guid sessionId)
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
                        $"\tTotal conflicts: {TotalResolvedConflicts}" + Environment.NewLine +
                        $"\tTotal duration :{durationstr} ");

            }
            return base.ToString();
        }
    }
}
