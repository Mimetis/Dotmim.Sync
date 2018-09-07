using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Tests.Models
{
    public partial class Tags
    {
        public Tags()
        {
            PostTag = new HashSet<PostTag>();
        }

        public int TagId { get; set; }
        public string Text { get; set; }

        public virtual ICollection<PostTag> PostTag { get; set; }
    }
}
