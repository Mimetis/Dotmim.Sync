using Dotmim.Sync.Enumerations;
using System;
using System.Data.Common;

namespace Dotmim.Sync
{
    /// <summary>
    /// Event args raised during the upgrade process.
    /// </summary>
    public class UpgradeProgressArgs : ProgressArgs
    {

        private string message;

        /// <inheritdoc cref="UpgradeProgressArgs" />
        public UpgradeProgressArgs(SyncContext context, string message, Version version, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.message = message;
            this.Version = version;
        }

        /// <summary>
        /// Gets the table that has been upgraded.
        /// </summary>
        public SyncTable Table { get; }

        /// <summary>
        /// Gets the version of the database.
        /// </summary>
        public Version Version { get; }

        /// <inheritdoc cref="ProgressArgs.ProgressLevel" />
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Information;

        /// <inheritdoc cref="ProgressArgs.Message" />
        public override string Message => this.message;

        /// <inheritdoc cref="ProgressArgs.EventId" />
        public override int EventId => 999999;
    }
}