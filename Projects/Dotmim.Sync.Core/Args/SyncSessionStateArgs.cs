using Dotmim.Sync.Enumerations;
using System;

namespace Dotmim.Sync
{
    /// <summary>
    /// Event args generated before to raise the SyncSessionStateChanged event.
    /// </summary>
    public class SyncSessionStateEventArgs : EventArgs
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="SyncSessionStateEventArgs"/> class.
        /// </summary>
        public SyncSessionStateEventArgs(SyncSessionState state)
        {
            this.State = state;
        }

        /// <summary>
        /// Gets the current state of the session.
        /// </summary>
        public SyncSessionState State { get; }
    }
}