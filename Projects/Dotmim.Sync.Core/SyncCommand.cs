using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;

namespace Dotmim.Sync
{
    public class SyncPreparedCommand 
    {
        public bool IsPrepared { get; set; }
        public string CommandCodeName { get; }

        public SyncPreparedCommand(string commandCodeName)
        {
            this.CommandCodeName = commandCodeName;
            this.IsPrepared = false;

        }
    }
}
