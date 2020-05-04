using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Tests.Models
{
    public partial class PostTag
    {
        public int PostId { get; set; }
        public int TagId { get; set; }

        public virtual Posts Post { get; set; }
        public virtual Tags Tag { get; set; }
    }
}
