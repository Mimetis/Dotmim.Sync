using System;

namespace Dotmim.Sync.Tests.Models
{
    public class PriceListDetail
    {
        public PriceListCategory Category { get; set; }

        public Guid PriceListId { get; set; }
        public string PriceCategoryId { get; set; }
        public Guid PriceListDettailId { get; set; }

        public Guid ProductId { get; set; }
        public string ProductDescription { get; set; }
        public decimal Amount { get; set; }
        public decimal Discount { get; set; }

        public decimal? Total { get; set; }

        public int? MinQuantity { get; set; }
    }
}
