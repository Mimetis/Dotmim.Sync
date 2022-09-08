using Dotmim.Sync.Enumerations;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;

namespace Dotmim.Sync
{
    public class ProgressArgs
    {
        /// <summary>
        /// Current connection used 
        /// </summary>
        public DbConnection Connection { get; internal set; }

        /// <summary>
        /// Current transaction used for the sync
        /// </summary>
        public DbTransaction Transaction { get; internal set; }

        /// <summary>
        /// Gets the current context
        /// </summary>
        public SyncContext Context { get; }

        /// <summary>
        /// Gets the Progress Level
        /// </summary>
        public virtual SyncProgressLevel ProgressLevel { get; }

        /// <summary>
        /// Gets or Sets an arbitrary args you can use for you own purpose
        /// </summary>
        public virtual string Hint { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        public ProgressArgs(SyncContext context, DbConnection connection, DbTransaction transaction)
        {
            this.Context = context;
            this.Connection = connection;
            this.Transaction = transaction;
            this.Message = this.GetType().Name;
            this.ProgressLevel = SyncProgressLevel.Information;
        }

        /// <summary>
        /// Constructor
        /// </summary>
        public ProgressArgs(SyncContext context, DbConnection connection)
        {
            this.Context = context;
            this.Connection = connection;
            this.Message = this.GetType().Name;
            this.ProgressLevel = SyncProgressLevel.Information;
        }


        /// <summary>
        /// Gets the args type
        /// </summary>
        public string TypeName => this.GetType().Name;

        /// <summary>
        /// return a global message about current progress
        /// </summary>
        public virtual string Message { get; } = string.Empty;

        /// <summary>
        /// return the progress initiator source
        /// </summary>
        public virtual string Source { get; } = string.Empty;

        /// <summary>
        /// Gets the event id, used for logging purpose
        /// </summary>
        public virtual int EventId { get; } = 1;

        /// <summary>
        /// Gets the overall percentage progress
        /// </summary>
        public double ProgressPercentage => this.Context.ProgressPercentage;

        /// <summary>
        /// Gets the Message property if any
        /// </summary>
        public override string ToString()
        {
            if (!string.IsNullOrEmpty(this.Message))
                return this.Message;

            return base.ToString();
        }



    }
}
