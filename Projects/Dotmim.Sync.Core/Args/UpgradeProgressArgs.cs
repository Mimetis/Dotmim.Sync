using Dotmim.Sync.Enumerations;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;

namespace Dotmim.Sync
{
    public class UpgradeProgressArgs : ProgressArgs
    {
        private string message;

        public SyncTable Table { get; }

        public Version Version { get; }

        public UpgradeProgressArgs(SyncContext context, string message, Version version, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.message= message;
            this.Version = version;
        }
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Information;

        public override string Message => this.message;

        public override int EventId => 999999;
    }
}
