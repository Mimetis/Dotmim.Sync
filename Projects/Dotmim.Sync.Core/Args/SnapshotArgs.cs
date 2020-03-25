using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync
{
    /// <summary>
    /// Event args generated before applying a snapshot on the target database
    /// </summary>
    public class SnapshotApplyingArgs : ProgressArgs
    {
        public SnapshotApplyingArgs(SyncContext context) : base(context, null, null)
        {
        }

        public override string Message => $"Applying snapshot.";
    }


    /// <summary>
    /// Event args generated before applying a snapshot on the target database
    /// </summary>
    public class SnapshotAppliedArgs : ProgressArgs
    {
        public SnapshotAppliedArgs(SyncContext context) : base(context, null, null)
        {
        }

        public override string Message => $"Snapshot applied.";
    }
}
