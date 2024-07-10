using Dotmim.Sync.Enumerations;
using System.Data.Common;

namespace Dotmim.Sync
{

    /// <summary>
    /// Progress args raised during the sync process.
    /// </summary>
    public class ProgressArgs
    {
        /// <summary>
        /// Gets current connection used.
        /// </summary>
        public DbConnection Connection { get; internal set; }

        /// <summary>
        /// Gets current transaction used for the sync.
        /// </summary>
        public DbTransaction Transaction { get; internal set; }

        /// <summary>
        /// Gets the current context.
        /// </summary>
        public SyncContext Context { get; }

        /// <summary>
        /// Gets the Progress Level.
        /// </summary>
        public virtual SyncProgressLevel ProgressLevel { get; }

        /// <summary>
        /// Gets or Sets an arbitrary args you can use for you own purpose.
        /// </summary>
        public virtual string Hint { get; set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="ProgressArgs"/> class.
        /// Constructor.
        /// </summary>
        public ProgressArgs(SyncContext context, DbConnection connection, DbTransaction transaction)
        {
            this.Context = context;
            this.Connection = connection;
            this.Transaction = transaction;
            this.Message = this.GetType().Name;
            this.ProgressLevel = SyncProgressLevel.Information;
            this.Source = connection?.Database;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ProgressArgs"/> class.
        /// Constructor.
        /// </summary>
        public ProgressArgs(SyncContext context, DbConnection connection)
        {
            this.Context = context;
            this.Connection = connection;
            this.Message = this.GetType().Name;
            this.ProgressLevel = SyncProgressLevel.Information;
            this.Source = connection?.Database;
        }

        /// <summary>
        /// Gets the args type.
        /// </summary>
        public string TypeName => this.GetType().Name;

        /// <summary>
        /// Gets or sets return a global message about current progress.
        /// </summary>
        public virtual string Message { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets return the progress initiator source.
        /// </summary>
        public virtual string Source { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the event id, used for logging purpose.
        /// </summary>
        public virtual int EventId { get; set; } = 1;

        /// <summary>
        /// Gets the overall percentage progress.
        /// </summary>
        public double ProgressPercentage => this.Context.ProgressPercentage;

        /// <summary>
        /// Gets the Message property if any.
        /// </summary>
        public override string ToString()
        {
            if (!string.IsNullOrEmpty(this.Message))
                return this.Message;

            return base.ToString();
        }
    }
}