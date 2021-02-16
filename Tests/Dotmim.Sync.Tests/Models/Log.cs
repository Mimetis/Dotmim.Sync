using System;
using System.Collections.Generic;

namespace Dotmim.Sync.Tests.Models
{
    public partial class Log
    {
        public Guid Oid { get; set; }
        public DateTime? TimeStampDate { get; set; }
        public string Operation { get; set; }
        public string ErrorDescription { get; set; }
        public int? OptimisticLockField { get; set; }
        public int? Gcrecord { get; set; }
    }
}
