using Dotmim.Sync.Enumerations;
using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Tools
{
   public class Table
    {
        public String Name { get; set; }
        public String Schema { get; set; }
        public SyncDirection Direction { get; set; }
    }
}
