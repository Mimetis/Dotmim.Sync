using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;

namespace Dotmim.Sync
{
    public class MetadataCleaningArgs : ProgressArgs
    {
        public SyncSetup Setup { get; }
        public long TimeStampStart { get; }

        public MetadataCleaningArgs(SyncContext context, SyncSetup setup, long timeStampStart, DbConnection connection, DbTransaction transaction)
        : base(context, connection, transaction)

        {
            this.Setup = setup;
            this.TimeStampStart = timeStampStart;
        }

        public override string Message => $"tables cleaning count:{Setup.Tables.Count}";

    }

    public class MetadataCleanedArgs : MetadataCleaningArgs
    {
        public MetadataCleanedArgs(SyncContext context, SyncSetup setup, long timeStampStart, DbConnection connection, DbTransaction transaction) : base(context, setup, timeStampStart, connection, transaction)
        {
        }

        public override string Message => $"tables cleaned count:{Setup.Tables.Count}";

    }
}
