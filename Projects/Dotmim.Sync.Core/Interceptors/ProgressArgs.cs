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
        public DbConnection Connection { get; }

        /// <summary>
        /// Current transaction used for the sync
        /// </summary>
        public DbTransaction Transaction { get; }

        /// <summary>
        /// Gets the current context
        /// </summary>
        public SyncContext Context { get; }

        /// <summary>
        /// Gets or Sets the action to be taken : Could eventually Rollback the current sync
        /// </summary>
        public ChangeApplicationAction Action { get; set; }

        /// <summary>
        /// Gets or Sets an arbitrary args you can use for you own purpose
        /// </summary>
        public string Hint { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        public ProgressArgs(SyncContext context, DbConnection connection, DbTransaction transaction)
        {
            this.Context = context;
            this.Connection = connection;
            this.Transaction = transaction;
        }

        public ProgressArgs(SyncContext context, DbConnection connection)
        {
            this.Context = context;
            this.Connection = connection;
        }

        public ProgressArgs(SyncContext context, string message, DbConnection connection, DbTransaction transaction)
            : this(context, connection, transaction) => this.Message = message;



        /// <summary>
        /// return a global message about current progress
        /// </summary>
        public virtual string Message { get; } = string.Empty;

    }
}
