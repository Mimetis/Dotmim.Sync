using System.Collections.Generic;

namespace Dotmim.Sync.Tests.Models
{
    public partial class Region
    {
        public Region()
        {
            Employee = new HashSet<Employee>();
        }

        public string RegionId { get; set; }
        public string Name { get; set; }

        public ICollection<Employee> Employee { get; set; }

    }
}
