using Dotmim.Sync.Core.Adapter;
using Dotmim.Sync.Data;
using Dotmim.Sync.Enumerations;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.Core
{
    /// <summary>
    /// Provide datas for the ApplyingChanges event
    /// </summary>
    public class ApplyingChangesEventArgs
    {
        public DmRowState State { get;}
        public DbTransaction Transaction { get; }
        public DbConnection Connection { get; }
        public DmView Changes { get;  }
        public ChangeApplicationAction Action { get; set; }

        public ApplyingChangesEventArgs(DmView changes, DmRowState state, DbConnection connection, DbTransaction transaction)
        {
            this.Changes = changes;
            this.State = state;
            this.Connection = connection;
            this.Transaction = transaction;
            this.Action = ChangeApplicationAction.Continue;
        }

    }
}
