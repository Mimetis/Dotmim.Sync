using System;
using System.Collections.Generic;

namespace Dotmim.Sync.Tests.Models
{
    public class PriceListCategory
    {
        public PriceListCategory()
        {
            Details = new List<PriceListDetail>();
        }

        public PriceList PriceList { get; set; }

        public Guid PriceListId { get; set; }
        public string PriceCategoryId { get; set; }

        public IList<PriceListDetail> Details { get; set; }


    }
}
