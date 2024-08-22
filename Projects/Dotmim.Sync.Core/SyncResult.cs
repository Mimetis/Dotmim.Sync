using System;

namespace Dotmim.Sync
{
    /// <summary>
    /// Compute all the results after a successfull sync.
    /// </summary>
    public class SyncResult
    {
        /// <inheritdoc cref="SyncResult" />
        public SyncResult() { }

        /// <inheritdoc cref="SyncResult" />
        public SyncResult(Guid sessionId) => this.SessionId = sessionId;

        /// <summary>
        /// Gets or Sets the current Session id, for an in progress sync.
        /// </summary>
        public Guid SessionId { get; set; }

        /// <summary>Gets or sets the time when a sync sessionn started.
        /// </summary>
        public DateTime StartTime { get; set; }

        /// <summary>
        /// Gets or Sets the ScopeName for this sync session.
        /// </summary>
        public string ScopeName { get; set; }

        /// <summary>
        /// Gets or sets the time when a sync session ended.
        /// </summary>
        public DateTime CompleteTime { get; set; }

        /// <summary>
        /// Gets the number of changes applied on the client.
        /// </summary>
        public int TotalChangesAppliedOnClient => (this.ChangesAppliedOnClient?.TotalAppliedChanges ?? 0) + (this.SnapshotChangesAppliedOnClient?.TotalAppliedChanges ?? 0);

        /// <summary>
        /// Gets the number of changes applied on the server.
        /// </summary>
        public int TotalChangesAppliedOnServer => this.ChangesAppliedOnServer?.TotalAppliedChanges ?? 0;

        /// <summary>
        /// Gets total number of changes downloaded from server.
        /// </summary>
        public int TotalChangesDownloadedFromServer => (this.ServerChangesSelected?.TotalChangesSelected ?? 0) + (this.SnapshotChangesAppliedOnClient?.TotalAppliedChanges ?? 0);

        /// <summary>
        /// Gets the number of change uploaded to the server.
        /// </summary>
        public int TotalChangesUploadedToServer => this.ClientChangesSelected?.TotalChangesSelected ?? 0;

        /// <summary>
        /// Gets the number of conflicts resolved.
        /// </summary>
        public int TotalResolvedConflicts =>
            Math.Max(this.ChangesAppliedOnClient?.TotalResolvedConflicts ?? 0, this.ChangesAppliedOnServer?.TotalResolvedConflicts ?? 0);

        /// <summary>
        /// Gets the number of row failed to apply on client.
        /// </summary>
        public int TotalChangesFailedToApplyOnClient => this.ChangesAppliedOnClient?.TotalAppliedChangesFailed ?? 0;

        /// <summary>
        /// Gets the number of sync errors.
        /// </summary>
        public int TotalChangesFailedToApplyOnServer => this.ChangesAppliedOnServer?.TotalAppliedChangesFailed ?? 0;

        /// <summary>
        /// Gets or Sets the summary of client changes that where applied on the server.
        /// </summary>
        public DatabaseChangesApplied ChangesAppliedOnServer { get; set; }

        /// <summary>
        /// Gets or Sets the summary of server changes that where applied on the client.
        /// </summary>
        public DatabaseChangesApplied ChangesAppliedOnClient { get; set; }

        /// <summary>
        /// Gets or Sets the summary of snapshot changes that where applied on the client.
        /// </summary>
        public DatabaseChangesApplied SnapshotChangesAppliedOnClient { get; set; }

        /// <summary>
        /// Gets or Sets the summary of client changes to be applied on the server.
        /// </summary>
        public DatabaseChangesSelected ClientChangesSelected { get; set; }

        /// <summary>
        /// Gets or Sets the summary of server changes selected to be applied on the client.
        /// </summary>
        public DatabaseChangesSelected ServerChangesSelected { get; set; }

        /// <summary>
        /// Get the result if sync session is ended.
        /// </summary>
        public override string ToString()
        {
            if (this.CompleteTime != this.StartTime && this.CompleteTime > this.StartTime)
            {
                var tsEnded = TimeSpan.FromTicks(this.CompleteTime.Ticks);
                var tsStarted = TimeSpan.FromTicks(this.StartTime.Ticks);
                var durationTs = tsEnded.Subtract(tsStarted);

                return $"Synchronization done. " + Environment.NewLine +
                        $"\tTotal changes  uploaded: {this.TotalChangesUploadedToServer}" + Environment.NewLine +
                        $"\tTotal changes  downloaded: {this.TotalChangesDownloadedFromServer} " + Environment.NewLine +
                        $"\tTotal changes  applied on client: {this.TotalChangesAppliedOnClient} " + Environment.NewLine +
                        $"\tTotal changes  applied on server: {this.TotalChangesAppliedOnServer} " + Environment.NewLine +
                        $"\tTotal changes  failed to apply on client: {this.TotalChangesFailedToApplyOnClient}" + Environment.NewLine +
                        $"\tTotal changes  failed to apply on server: {this.TotalChangesFailedToApplyOnServer}" + Environment.NewLine +
                        $"\tTotal resolved conflicts: {this.TotalResolvedConflicts}" + Environment.NewLine +
                        $"\tTotal duration :{durationTs:hh\\.mm\\:ss\\.fff} ";
            }

            return base.ToString();
        }
    }
}