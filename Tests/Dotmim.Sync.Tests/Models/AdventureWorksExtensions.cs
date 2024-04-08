using Dotmim.Sync.Tests.Core;
using Dotmim.Sync.Tests.Misc;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static Microsoft.EntityFrameworkCore.DbLoggerCategory.Database;

namespace Dotmim.Sync.Tests.Models
{
    public static class AdventureWorksExtensions
    {

        public static bool UseFallbackSchema(this CoreProvider coreProvider, bool? defaultValue = null)
        {
            if (defaultValue.HasValue)
            {
                coreProvider.AdditionalProperties["UseFallbackSchema"] = defaultValue.Value.ToString();
                return defaultValue.Value;
            }

            if (coreProvider.AdditionalProperties.TryGetValue("UseFallbackSchema", out var useFallbackSchema))
            {
                if (bool.TryParse(useFallbackSchema, out var isUsingFallbackSchema))
                    return isUsingFallbackSchema;
            }

            return false;
        }

        public static bool UseShouldDropDatabase(this CoreProvider coreProvider, bool? defaultValue = null)
        {
            if (defaultValue.HasValue)
            {
                coreProvider.AdditionalProperties["UseShouldDropDatabase"] = defaultValue.Value.ToString();
                return defaultValue.Value;
            }

            if (coreProvider.AdditionalProperties.TryGetValue("UseShouldDropDatabase", out var useShouldDropDatabase))
            {
                if (bool.TryParse(useShouldDropDatabase, out var isUseShouldDropDatabase))
                    return isUseShouldDropDatabase;
            }

            return false;
        }



        public static async Task<ProductCategory> GetProductCategoryAsync(this CoreProvider provider, string productCategoryId,  DbTransaction transaction = null)
        {
            using var ctx = new AdventureWorksContext(provider, provider.UseFallbackSchema());

            if (transaction != null)
                ctx.Database.UseTransaction(transaction);

            return await ctx.ProductCategory.FindAsync(productCategoryId);
        }

        public static async Task<ProductCategory> AddProductCategoryAsync(this CoreProvider provider,
                                string productCategoryId = default, string parentProductCategoryId = default, string name = default, Guid? rowguid = default,
                                DateTime? modifiedDate = default, string attributeWithSpace = default,  DbTransaction transaction = null)
        {
            using var ctx = new AdventureWorksContext(provider, provider.UseFallbackSchema());

            if (transaction != null)
                ctx.Database.UseTransaction(transaction);

            name = string.IsNullOrEmpty(name) ? HelperDatabase.GetRandomName() : name;
            productCategoryId = string.IsNullOrEmpty(productCategoryId) ? name.ToUpperInvariant()[..11] : productCategoryId;

            var pc = new ProductCategory
            {
                ProductCategoryId = productCategoryId,
                ParentProductCategoryId = parentProductCategoryId,
                Name = name,
                Rowguid = rowguid,
                ModifiedDate = modifiedDate != null ? modifiedDate.Value : (DateTime?)null,
                AttributeWithSpace = attributeWithSpace

            };
            ctx.Add(pc);

            await ctx.SaveChangesAsync();

            return pc;
        }

        public async static Task<ProductCategory> UpdateProductCategoryAsync(this CoreProvider provider, ProductCategory productCategory, DbTransaction transaction = null)
        {
            using var ctx = new AdventureWorksContext(provider, provider.UseFallbackSchema());

            if (transaction != null)
                ctx.Database.UseTransaction(transaction);

            ctx.ProductCategory.Add(productCategory);
            ctx.Entry(productCategory).State = EntityState.Modified;

            await ctx.SaveChangesAsync();

            return productCategory;
        }

        public async static Task DeleteProductCategoryAsync(this CoreProvider provider, string productCategoryId,  DbTransaction transaction = null)
        {
            using var ctx = new AdventureWorksContext(provider, provider.UseFallbackSchema());

            if (transaction != null)
                ctx.Database.UseTransaction(transaction);

            var pc = await ctx.ProductCategory.FirstOrDefaultAsync(pc => pc.ProductCategoryId == productCategoryId);
            ctx.ProductCategory.Remove(pc);

            await ctx.SaveChangesAsync();
        }

        public async static Task<Product> AddProductAsync(this CoreProvider provider,
            Guid? productId = default,
            string productNumber = default, string name = default, string productCategoryId = default,
            string color = default, decimal? standardCost = default, decimal? listPrice = default, string size = default, decimal? weight = default, Guid? rowguid = default,
            DateTime? modifiedDate = default, byte[] thumbNailPhoto = default, string thumbnailPhotoFileName = default, DbTransaction transaction = null)
        {
            using var ctx = new AdventureWorksContext(provider, provider.UseFallbackSchema());

            if (transaction != null)
                ctx.Database.UseTransaction(transaction);

            productId ??= Guid.NewGuid();
            name = string.IsNullOrEmpty(name) ? HelperDatabase.GetRandomName() : name;
            productNumber = string.IsNullOrEmpty(productNumber) ? name.ToUpperInvariant()[..10] : productNumber;

            var p = new Product
            {
                ProductId = productId.Value,
                ProductNumber = productNumber,
                ProductCategoryId = productCategoryId,
                Name = name,
                Color = color,
                StandardCost = standardCost,
                ListPrice = listPrice,
                Size = size,
                Weight = weight,
                Rowguid = rowguid,
                ModifiedDate = modifiedDate != null ? modifiedDate.Value : (DateTime?)null,
                ThumbNailPhoto = thumbNailPhoto,
                ThumbnailPhotoFileName = thumbnailPhotoFileName
            };
            ctx.Add(p);

            await ctx.SaveChangesAsync();

            return p;
        }

        public async static Task<List<Product>> GetProductsAsync(this CoreProvider provider, DbTransaction transaction = null)
        {
            using var ctx = new AdventureWorksContext(provider, provider.UseFallbackSchema());

            if (transaction != null)
                ctx.Database.UseTransaction(transaction);

            return await ctx.Product.ToListAsync();
        }
      
        public async static Task<Product> GetProductAsync(this CoreProvider provider, Guid productId, DbTransaction transaction = null)
        {
            using var ctx = new AdventureWorksContext(provider, provider.UseFallbackSchema());

            if (transaction != null)
                ctx.Database.UseTransaction(transaction);

            return await ctx.Product.FindAsync(productId);
        }

        public async static Task<Product> UpdateProductAsync(this CoreProvider provider, Product product,  DbTransaction transaction = null)
        {
            using var ctx = new AdventureWorksContext(provider, provider.UseFallbackSchema());

            if (transaction != null)
                ctx.Database.UseTransaction(transaction);

            ctx.Product.Add(product);
            ctx.Entry(product).State = EntityState.Modified;

            await ctx.SaveChangesAsync();

            return product;
        }

        public async static Task DeleteProductAsync(this CoreProvider provider, Guid productId, DbTransaction transaction = null)
        {
            using var ctx = new AdventureWorksContext(provider, provider.UseFallbackSchema());

            if (transaction != null)
                ctx.Database.UseTransaction(transaction);

            var pc = await ctx.Product.FirstOrDefaultAsync(pc => pc.ProductId == productId);
            ctx.Product.Remove(pc);

            await ctx.SaveChangesAsync();
        }

        public async static Task<PriceList> AddPriceListAsync(this CoreProvider provider, Guid? priceListId = default, string description = default, DateTime? from = default,
           DateTime? to = default, DbTransaction transaction = null)
        {
            using var ctx = new AdventureWorksContext(provider, provider.UseFallbackSchema());

            if (transaction != null)
                ctx.Database.UseTransaction(transaction);

            priceListId = priceListId.HasValue ? priceListId : Guid.NewGuid();
            description = string.IsNullOrEmpty(description) ? HelperDatabase.GetRandomName() : description;

            var pl = new PriceList
            {
                PriceListId = priceListId.Value,
                Description = description,
                From = from != null ? from.Value : (DateTime?)null,
                To = to != null ? to.Value : (DateTime?)null
            };
            ctx.Add(pl);

            await ctx.SaveChangesAsync();

            return pl;
        }

        public async static Task<PriceList> GetPriceListAsync(this CoreProvider provider, Guid priceListId, DbTransaction transaction = null)
        {
            using var ctx = new AdventureWorksContext(provider, provider.UseFallbackSchema());

            if (transaction != null)
                ctx.Database.UseTransaction(transaction);

            return await ctx.PricesList.FindAsync(priceListId);
        }

        public async static Task<Customer> AddCustomerAsync(this CoreProvider provider, Guid? customerId = default, string firstName = default,
            string lastName = default, string companyName = default, DbTransaction transaction = null)
        {
            using var ctx = new AdventureWorksContext(provider, provider.UseFallbackSchema());

            if (transaction != null)
                ctx.Database.UseTransaction(transaction);

            var customer = new Customer
            {
                CustomerId = customerId == default ? Guid.NewGuid() : customerId.Value,
                FirstName = firstName == default ? HelperDatabase.GetRandomName() : firstName,
                LastName = lastName == default ? HelperDatabase.GetRandomName() : lastName,
                CompanyName = companyName,
            };

            ctx.Add(customer);

            await ctx.SaveChangesAsync();

            return customer;

        }

        public async static Task<Address> AddAddressAsync(this CoreProvider provider, int? addressId = default, string addressLine1 = default, string addressLine2 = default,
          string city = default, string postalCode = default, DbTransaction transaction = null)
        {
            using var ctx = new AdventureWorksContext(provider, provider.UseFallbackSchema());

            var (providerType, _) = HelperDatabase.GetDatabaseType(provider);

            if (transaction != null)
                ctx.Database.UseTransaction(transaction);

            ctx.Database.OpenConnection();

            var address = new Address
            {
                AddressId = addressId == null ? default : addressId.Value,
                AddressLine1 = addressLine1 == default ? HelperDatabase.GetRandomName() : addressLine1,
                AddressLine2 = addressLine2 == default ? HelperDatabase.GetRandomName() : addressLine2,
                City = city == default ? HelperDatabase.GetRandomName() : city,
                PostalCode = postalCode == default ? HelperDatabase.GetRandomName() : postalCode,
            };

            ctx.Add(address);

            if (providerType == ProviderType.Sql && addressId.HasValue)
                ctx.Database.ExecuteSqlRaw($"SET IDENTITY_INSERT Address ON;");

            await ctx.SaveChangesAsync();

            if (providerType == ProviderType.Sql && addressId.HasValue)
                ctx.Database.ExecuteSqlRaw($"SET IDENTITY_INSERT Address OFF;");

            ctx.Database.CloseConnection();

            return address;

        }

        public async static Task<Address> UpdateAddressAsync(this CoreProvider provider, Address address, DbTransaction transaction = null)
        {
            using var ctx = new AdventureWorksContext(provider, provider.UseFallbackSchema());

            if (transaction != null)
                ctx.Database.UseTransaction(transaction);

            ctx.Address.Add(address);
            ctx.Entry(address).State = EntityState.Modified;

            await ctx.SaveChangesAsync();

            return address;
        }

        public async static Task DeleteAddressAsync(this CoreProvider provider, int addressId,  DbTransaction transaction = null)
        {
            using var ctx = new AdventureWorksContext(provider, provider.UseFallbackSchema());

            if (transaction != null)
                ctx.Database.UseTransaction(transaction);

            var pc = await ctx.Address.FirstOrDefaultAsync(pc => pc.AddressId == addressId);
            ctx.Address.Remove(pc);

            await ctx.SaveChangesAsync();
        }

        public async static Task<Address> GetAddressAsync(this CoreProvider provider, int addressId, DbTransaction transaction = null)
        {
            using var ctx = new AdventureWorksContext(provider, provider.UseFallbackSchema());

            if (transaction != null)
                ctx.Database.UseTransaction(transaction);

            return await ctx.Address.FindAsync(addressId);
        }

        public async static Task<CustomerAddress> AddCustomerAddressAsync(this CoreProvider provider, int addressId, Guid customerId, string addressType = default,
            DbTransaction transaction = null)
        {
            using var ctx = new AdventureWorksContext(provider, provider.UseFallbackSchema());

            if (transaction != null)
                ctx.Database.UseTransaction(transaction);

            var customerAddress = new CustomerAddress
            {
                AddressId = addressId,
                CustomerId = customerId,
                AddressType = addressType == default ? "Home" : addressType
            };

            ctx.Add(customerAddress);

            await ctx.SaveChangesAsync();

            return customerAddress;

        }

        public async static Task<CustomerAddress> UpdateCustomerAddressAsync(this CoreProvider provider, CustomerAddress customerAddress, DbTransaction transaction = null)
        {
            using var ctx = new AdventureWorksContext(provider, provider.UseFallbackSchema());

            if (transaction != null)
                ctx.Database.UseTransaction(transaction);

            ctx.CustomerAddress.Add(customerAddress);
            ctx.Entry(customerAddress).State = EntityState.Modified;

            await ctx.SaveChangesAsync();

            return customerAddress;
        }

        public async static Task DeleteCustomerAddressAsync(this CoreProvider provider, int addressId, Guid customerId, DbTransaction transaction = null)
        {
            using var ctx = new AdventureWorksContext(provider, provider.UseFallbackSchema());

            if (transaction != null)
                ctx.Database.UseTransaction(transaction);

            var pc = await ctx.CustomerAddress.FirstOrDefaultAsync(pc => pc.AddressId == addressId && pc.CustomerId == customerId);
            ctx.CustomerAddress.Remove(pc);

            await ctx.SaveChangesAsync();
        }

        public async static Task<CustomerAddress> GetCustomerAddressAsync(this CoreProvider provider, int addressId, Guid customerId, DbTransaction transaction = null)
        {
            using var ctx = new AdventureWorksContext(provider, provider.UseFallbackSchema());

            if (transaction != null)
                ctx.Database.UseTransaction(transaction);

            return await ctx.CustomerAddress.FirstOrDefaultAsync(pc => pc.AddressId == addressId && pc.CustomerId == customerId);
        }

        public async static Task<SalesOrderHeader> AddSalesOrderHeaderAsync(this CoreProvider provider, Guid? customerId, int? salesOrderId = default,
          DbTransaction transaction = null)
        {
            using var ctx = new AdventureWorksContext(provider, provider.UseFallbackSchema());

            var (providerType, _) = HelperDatabase.GetDatabaseType(provider);

            if (transaction != null)
                ctx.Database.UseTransaction(transaction);

            ctx.Database.OpenConnection();

            var soh = new SalesOrderHeader
            {
                SalesOrderId = salesOrderId.HasValue ? salesOrderId.Value : default,
                CustomerId = customerId.HasValue ? customerId.Value : AdventureWorksContext.CustomerId1ForFilter,
                SalesOrderNumber = $"SO-99099",
                RevisionNumber = 1,
                Status = 5,
                OnlineOrderFlag = true,
                PurchaseOrderNumber = "PO348186287",
                ShipMethod = "CAR TRANSPORTATION",
                SubTotal = 6530.35M,
                TaxAmt = 70.4279M,
                Freight = 22.0087M,
                TotalDue = 6530.35M + 70.4279M + 22.0087M
            };


            ctx.Add(soh);

            var sohTableName = provider.UseFallbackSchema() ? "SalesLT.SalesOrderHeader" : "SalesOrderHeader";

            if (providerType == ProviderType.Sql && salesOrderId.HasValue)
                ctx.Database.ExecuteSqlRaw($"SET IDENTITY_INSERT {sohTableName} ON;");

            await ctx.SaveChangesAsync();

            if (providerType == ProviderType.Sql && salesOrderId.HasValue)
                ctx.Database.ExecuteSqlRaw($"SET IDENTITY_INSERT {sohTableName} OFF;");

            ctx.Database.CloseConnection();

            return soh;

        }

        public async static Task<SalesOrderHeader> UpdateSalesOrderHeaderAsync(this CoreProvider provider, SalesOrderHeader salesOrderHeader, DbTransaction transaction = null)
        {
            using var ctx = new AdventureWorksContext(provider, provider.UseFallbackSchema());

            if (transaction != null)
                ctx.Database.UseTransaction(transaction);

            ctx.SalesOrderHeader.Add(salesOrderHeader);
            ctx.Entry(salesOrderHeader).State = EntityState.Modified;

            await ctx.SaveChangesAsync();

            return salesOrderHeader;
        }

        public async static Task DeleteSalesOrderHeaderAsync(this CoreProvider provider, int salesOrderId, DbTransaction transaction = null)
        {
            using var ctx = new AdventureWorksContext(provider, provider.UseFallbackSchema());

            if (transaction != null)
                ctx.Database.UseTransaction(transaction);

            var pc = await ctx.SalesOrderHeader.FirstOrDefaultAsync(pc => pc.SalesOrderId == salesOrderId);
            ctx.SalesOrderHeader.Remove(pc);

            await ctx.SaveChangesAsync();
        }

        public async static Task<SalesOrderHeader> GetSalesOrderHeaderAsync(this CoreProvider provider, int salesOrderId,  DbTransaction transaction = null)
        {
            using var ctx = new AdventureWorksContext(provider, provider.UseFallbackSchema());

            if (transaction != null)
                ctx.Database.UseTransaction(transaction);

            return await ctx.SalesOrderHeader.FindAsync(salesOrderId);
        }

        public async static Task<SalesOrderDetail> AddSalesOrderDetailAsync(this CoreProvider provider, int salesOrderId, Guid productId,
           int? salesOrderDetailId = default, short? orderQty = default, decimal? unitPrice = default,
           DbTransaction transaction = null)
        {
            using var ctx = new AdventureWorksContext(provider, provider.UseFallbackSchema());

            var (providerType, _) = HelperDatabase.GetDatabaseType(provider);

            if (transaction != null)
                ctx.Database.UseTransaction(transaction);

            ctx.Database.OpenConnection();

            var sod = new SalesOrderDetail
            {
                SalesOrderId = salesOrderId,
                SalesOrderDetailId = salesOrderDetailId.HasValue ? salesOrderDetailId.Value : default,
                ProductId = productId,
                OrderQty = orderQty.HasValue ? orderQty.Value : (short)1,
                UnitPrice = unitPrice.HasValue ? unitPrice.Value : 10M,
                UnitPriceDiscount = 0M,
            };


            ctx.Add(sod);

            var sodTableName = provider.UseFallbackSchema() ? "SalesLT.SalesOrderDetail" : "SalesOrderDetail";

            if (providerType == ProviderType.Sql && salesOrderDetailId.HasValue)
                ctx.Database.ExecuteSqlRaw($"SET IDENTITY_INSERT {sodTableName} ON;");

            await ctx.SaveChangesAsync();

            if (providerType == ProviderType.Sql && salesOrderDetailId.HasValue)
                ctx.Database.ExecuteSqlRaw($"SET IDENTITY_INSERT {sodTableName} OFF;");

            ctx.Database.CloseConnection();

            return sod;

        }

        public async static Task<SalesOrderDetail> UpdateSalesOrderDetailAsync(this CoreProvider provider, SalesOrderDetail salesOrderDetail, DbTransaction transaction = null)
        {
            using var ctx = new AdventureWorksContext(provider, provider.UseFallbackSchema());

            if (transaction != null)
                ctx.Database.UseTransaction(transaction);

            ctx.SalesOrderDetail.Add(salesOrderDetail);
            ctx.Entry(salesOrderDetail).State = EntityState.Modified;

            await ctx.SaveChangesAsync();

            return salesOrderDetail;
        }

        public async static Task DeleteSalesOrderDetailAsync(this CoreProvider provider, int salesOrderDetailId, DbTransaction transaction = null)
        {
            using var ctx = new AdventureWorksContext(provider, provider.UseFallbackSchema());

            if (transaction != null)
                ctx.Database.UseTransaction(transaction);

            var pc = await ctx.SalesOrderDetail.FirstOrDefaultAsync(pc => pc.SalesOrderDetailId == salesOrderDetailId);
            ctx.SalesOrderDetail.Remove(pc);

            await ctx.SaveChangesAsync();
        }

        public async static Task<SalesOrderDetail> GetSalesOrderDetailAsync(this CoreProvider provider, int salesOrderDetailId, DbTransaction transaction = null)
        {
            using var ctx = new AdventureWorksContext(provider, provider.UseFallbackSchema());

            if (transaction != null)
                ctx.Database.UseTransaction(transaction);

            return await ctx.SalesOrderDetail.FindAsync(salesOrderDetailId);
        }

        public async static Task ExecuteSqlRawAsync(this CoreProvider provider, string sql, DbTransaction transaction = null)
        {
            using var ctx = new AdventureWorksContext(provider, provider.UseFallbackSchema());

            var (providerType, _) = HelperDatabase.GetDatabaseType(provider);

 
            if (transaction != null)
                ctx.Database.UseTransaction(transaction);

            await ctx.Database.ExecuteSqlRawAsync(sql);

        }

        public static async Task EnsureTablesAreCreatedAsync(this CoreProvider coreProvider, bool seeding)
        {
            var (t, d) = HelperDatabase.GetDatabaseType(coreProvider);

            if (t == ProviderType.Sqlite)
                HelperDatabase.DropDatabase(t, d);

            new AdventureWorksContext(coreProvider, seeding).Database.EnsureCreated();

            var localOrchestrator = new LocalOrchestrator(coreProvider);
            using var c = coreProvider.CreateConnection();
            c.Open();

            var setup = await localOrchestrator.GetAllTablesAsync(c);

            if (!setup.HasTables)
            {
                Console.WriteLine($"Tables not created for provider {t} in database {d}");
                Debug.WriteLine($"Tables not created for provider {t} in database {d}");

            }
            c.Close();
        }

        public static async Task DropAllTablesAsync(this CoreProvider provider, bool tablesIncluded = false)
        {
            var localOrchestrator = new LocalOrchestrator(provider, new SyncOptions { DisableConstraintsOnApplyChanges = true });
            localOrchestrator.OnDropAll(args => args.ConfirmYouWantToDeleteTables = () => true);
            await localOrchestrator.DropAllAsync(tablesIncluded);
        }

        public static async Task EmptyAllTablesAsync(this CoreProvider provider)
        {
            var localOrchestrator = new LocalOrchestrator(provider, new SyncOptions { DisableConstraintsOnApplyChanges = true });
            var setup = await localOrchestrator.GetAllTablesAsync();
            var schema = await localOrchestrator.GetSchemaAsync(setup);
            var schemaTables = schema.Tables.SortByDependencies(tab => tab.GetRelations().Select(r => r.GetParentTable()));

            await localOrchestrator.DropAllAsync(false);

            var (providerType, dbName) = HelperDatabase.GetDatabaseType(provider);
            foreach (var schemaTable in schemaTables.Reverse())
                HelperDatabase.TruncateTable(providerType, dbName, schemaTable.TableName, schemaTable.SchemaName);
        }


        public static int GetDatabaseRowsCount(this CoreProvider provider)
        {
            int totalCountRows = 0;

            using var ctx = new AdventureWorksContext(provider, provider.UseFallbackSchema());

            totalCountRows += ctx.Address.Count();
            totalCountRows += ctx.Customer.Count();
            totalCountRows += ctx.CustomerAddress.Count();
            totalCountRows += ctx.Employee.Count();
            totalCountRows += ctx.EmployeeAddress.Count();
            totalCountRows += ctx.Log.Count();
            totalCountRows += ctx.Posts.Count();
            totalCountRows += ctx.PostTag.Count();
            totalCountRows += ctx.PricesList.Count();
            totalCountRows += ctx.PricesListCategory.Count();
            totalCountRows += ctx.PricesListDetail.Count();
            totalCountRows += ctx.Product.Count();
            totalCountRows += ctx.ProductCategory.Count();
            totalCountRows += ctx.ProductModel.Count();
            totalCountRows += ctx.SalesOrderDetail.Count();
            totalCountRows += ctx.SalesOrderHeader.Count();
            totalCountRows += ctx.Tags.Count();

            return totalCountRows;
        }


        public static int GetDatabaseFilteredRowsCount(this CoreProvider coreProvider, Guid? customerId = default)
        {
            int totalCountRows = 0;

            if (!customerId.HasValue)
                customerId = AdventureWorksContext.CustomerId1ForFilter;

            using var ctx = new AdventureWorksContext(coreProvider);

            totalCountRows += ctx.Address.Where(a => a.CustomerAddress.Any(ca => ca.CustomerId == customerId)).Count();
            totalCountRows += ctx.Customer.Where(c => c.CustomerId == customerId).Count();
            totalCountRows += ctx.CustomerAddress.Where(c => c.CustomerId == customerId).Count();
            totalCountRows += ctx.SalesOrderDetail.Where(sod => sod.SalesOrder.CustomerId == customerId).Count();
            totalCountRows += ctx.SalesOrderHeader.Where(c => c.CustomerId == customerId).Count();
            totalCountRows += ctx.Product.Where(p => !String.IsNullOrEmpty(p.ProductCategoryId)).Count();

            totalCountRows += ctx.Employee.Count();
            totalCountRows += ctx.EmployeeAddress.Count();
            totalCountRows += ctx.Log.Count();
            totalCountRows += ctx.Posts.Count();
            totalCountRows += ctx.PostTag.Count();
            totalCountRows += ctx.PricesList.Count();
            totalCountRows += ctx.PricesListCategory.Count();
            totalCountRows += ctx.PricesListDetail.Count();
            totalCountRows += ctx.ProductCategory.Count();
            totalCountRows += ctx.ProductModel.Count();
            totalCountRows += ctx.Tags.Count();

            return totalCountRows;
        }


    }
}
