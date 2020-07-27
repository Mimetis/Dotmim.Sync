using System;
using System.Collections.Generic;

namespace Dotmim.Sync.Tests.Models
{
    public partial class Sql
    {
        public Guid SqlId { get; set; }
        public string File { get; set; }
        public string Read { get; set; }
        public string From { get; set; }
        public string To { get; set; }
        public string Select { get; set; }
        public string Array { get; set; }
        public string String { get; set; }
        public object Value { get; set; }
    }
}
