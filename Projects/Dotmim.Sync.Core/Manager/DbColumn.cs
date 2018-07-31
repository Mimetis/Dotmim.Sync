using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Manager
{

    /// <summary>
    /// Represents the db type for the current provider
    /// </summary>
    public class DbColumn
    {
        public string Provider { get; set; }
        public String TypeName { get; set; }
        public Int32 Precision { get; set; }
        public Int32 Scale { get; set; }
        public Int32 MaxLength { get; set; }
        public Boolean IsUnsigned { get; set; }
        public Boolean IsUnicode { get; set; }
        public Boolean AllowNull { get; set; }
        public Boolean IsText { get; set; }
        public Boolean IsNumeric { get; set; }
        public Boolean SupportScale { get; set; }

    }
}
