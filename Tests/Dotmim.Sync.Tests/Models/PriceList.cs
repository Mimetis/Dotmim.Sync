using System;
using System.Collections.Generic;

namespace Dotmim.Sync.Tests.Models
{
    public class PriceList
    {
        public PriceList()
        {
            Categories = new List<PriceListCategory>();
        }

        public Guid PriceListId { get; set; }
        public string Description { get; set; }
        public DateTime? From { get; set; }
        public DateTime? To { get; set; }
        public IList<PriceListCategory> Categories { get; set; }
    }
}
