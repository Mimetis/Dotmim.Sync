using Dotmim.Sync.Enumerations;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;

namespace Dotmim.Sync
{
    public abstract class BaseArgs
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
        public BaseArgs(SyncContext context, DbConnection connection, DbTransaction transaction)
        {
            this.Context = context;
            this.Connection = connection;
            this.Transaction = transaction;
        }

    }
}
