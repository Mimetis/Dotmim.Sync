using System;
using System.Collections.Generic;

namespace Dotmim.Sync.Tests.Models
{
    public partial class ProductCategory
    {
        public ProductCategory()
        {
            Product = new HashSet<Product>();
        }

        public string ProductCategoryId { get; set; }

        public string ParentProductCategoryId { get; set; }

        public ProductCategory ParentProductCategory { get; set; }

        public string Name { get; set; }
        public Guid? Rowguid { get; set; }
        public DateTime? ModifiedDate { get; set; }
        public string AttributeWithSpace { get; set; }

        public ICollection<Product> Product { get; set; }
        public ICollection<ProductCategory> ProductCategories { get; set; }
    }
}
