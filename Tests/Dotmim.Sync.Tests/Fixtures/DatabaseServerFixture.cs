using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Tests.Core;
using Dotmim.Sync.Tests.Models;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.Tests.Fixtures
{
    public class DatabaseServerFixture<T> : IDisposable where T : RelationalFixture
    {

        // Two clients : First one is always same type as server. Second one is Sqlite
        //public virtual List<ProviderType> ClientsType => new List<ProviderType> { HelperDatabase.GetProviderType<T>(), ProviderType.Postgres};
        public virtual List<ProviderType> ClientsType => new List<ProviderType> { ProviderType.Postgres};

        // One Server type of T
        public virtual ProviderType ServerProviderType => HelperDatabase.GetProviderType<T>();

        // SQL Server has schema on server database
        private string salesSchema = typeof(T) == typeof(SqlServerFixtureType) ? "SalesLT." : "";

        public virtual string[] Tables => new string[]
        {
            $"{salesSchema}ProductCategory", $"{salesSchema}ProductModel", $"{salesSchema}Product",
            "Employee", "Customer", "Address", "CustomerAddress", "EmployeeAddress",
            $"{salesSchema}SalesOrderHeader", $"{salesSchema}SalesOrderDetail",
            "Posts", "Tags", "PostTag",
            "PricesList", "PricesListCategory", "PricesListDetail", "Log"
        };

        public virtual SyncSetup GetSyncSetup() => new SyncSetup(Tables);

        public virtual bool UseFallbackSchema => typeof(T) == typeof(SqlServerFixtureType);

        public string ServerDatabaseName { get; set; }

        public Dictionary<ProviderType, string> ClientDatabaseNames { get; set; } = new Dictionary<ProviderType, string>();


        public DatabaseServerFixture()
        {
            this.ServerDatabaseName = HelperDatabase.GetRandomName("tcp_srv");
            new AdventureWorksContext(ServerDatabaseName, ServerProviderType, UseFallbackSchema, true).Database.EnsureCreated();

            foreach (var type in this.ClientsType)
            {
                var dbName = HelperDatabase.GetRandomName("tcp_cli");
                new AdventureWorksContext(dbName, type, UseFallbackSchema, false).Database.EnsureCreated();
                ClientDatabaseNames.Add(type, dbName);
            }
        }

        /// <summary>
        /// Get the server provider. Creates database if not exists
        /// </summary>
        public CoreProvider GetServerProvider() => HelperDatabase.GetSyncProvider(ServerProviderType, ServerDatabaseName);

        /// <summary>
        /// Returns all clients providers. Create database if not exists
        /// </summary>
        public IEnumerable<CoreProvider> GetClientProviders()
        {
            foreach (var type in this.ClientsType)
                yield return HelperDatabase.GetSyncProvider(type, ClientDatabaseNames[type]);
        }


        /// <summary>
        /// Drop all tables from client database to have an empty database
        /// </summary>
        public async Task DropAllTablesAsync(CoreProvider provider, bool tablesIncluded = false)
        {
            var localOrchestrator = new LocalOrchestrator(provider, new SyncOptions { DisableConstraintsOnApplyChanges = true });
            localOrchestrator.OnDropAll(args => args.ConfirmYouWantToDeleteTables = () => true);
            await localOrchestrator.DropAllAsync(tablesIncluded);
        }

        /// <summary>
        /// Drop all tables from client database to have an empty database
        /// </summary>
        public async Task EmptyAllTablesAsync(CoreProvider provider)
        {
            var localOrchestrator = new LocalOrchestrator(provider, new SyncOptions { DisableConstraintsOnApplyChanges = true });
            var schema = await localOrchestrator.GetSchemaAsync(this.GetSyncSetup());
            var schemaTables = schema.Tables.SortByDependencies(tab => tab.GetRelations().Select(r => r.GetParentTable()));

            await localOrchestrator.DropAllAsync(false);

            var (providerType, dbName) = HelperDatabase.GetDatabaseType(provider);
            foreach (var schemaTable in schemaTables.Reverse())
                HelperDatabase.TruncateTable(providerType, dbName, schemaTable.TableName, schemaTable.SchemaName);
        }

        /// <summary>
        /// Add a ProductCategory row to the database
        /// </summary>
        public async Task<ProductCategory> AddProductCategoryAsync(CoreProvider provider,
            string productCategoryId = default, string parentProductCategoryId = default, string name = default, Guid? rowguid = default, DateTime? modifiedDate = default, string attributeWithSpace = default)
        {
            using var ctx = new AdventureWorksContext(provider);

            name = string.IsNullOrEmpty(name) ? HelperDatabase.GetRandomName() : name;
            productCategoryId = string.IsNullOrEmpty(productCategoryId) ? name.ToUpperInvariant()[..11] : productCategoryId;

            var pc = new ProductCategory
            {
                ProductCategoryId = productCategoryId,
                ParentProductCategoryId = parentProductCategoryId,
                Name = name,
                Rowguid = rowguid,
                ModifiedDate = modifiedDate != null ? modifiedDate.Value.ToUniversalTime() : (DateTime?)null,
                AttributeWithSpace = attributeWithSpace
                
            };
            ctx.Add(pc);

            await ctx.SaveChangesAsync();

            return pc;
        }


        /// <summary>
        /// Add a ProductCategory row to the database
        /// </summary>
        public async Task<ProductCategory> UpdateProductCategoryAsync(CoreProvider provider, ProductCategory productCategory)
        {
            using var ctx = new AdventureWorksContext(provider);

            ctx.ProductCategory.Add(productCategory);
            ctx.Entry(productCategory).State = EntityState.Modified;
            
            await ctx.SaveChangesAsync();

            return productCategory;
        }

        /// <summary>
        /// Delete a ProductCategory row to the database
        /// </summary>
        public async Task DeleteProductCategoryAsync(CoreProvider provider, string productCategoryId)
        {
            using var ctx = new AdventureWorksContext(provider);

            var pc = ctx.ProductCategory.FirstOrDefault(pc => pc.ProductCategoryId == productCategoryId);
            ctx.ProductCategory.Remove(pc);

            await ctx.SaveChangesAsync();
        }

        /// <summary>
        /// Get a ProductCategory row from the database
        /// </summary>
        public async Task<ProductCategory> GetProductCategoryAsync(CoreProvider provider, string productCategoryId)
        {
            using var ctx = new AdventureWorksContext(provider);
            return await ctx.ProductCategory.FindAsync(productCategoryId);
        }



        /// Add a Product item to the database identified by its name and its provider type
        public async Task<Product> AddProductAsync(CoreProvider provider,
            Guid? productId = default,
            string productNumber = default, string name = default, string productCategoryId = default,
            string color = default, decimal? standardCost = default, decimal? listPrice = default, string size = default, decimal? weight = default, Guid? rowguid = default,
            DateTime? modifiedDate = default, byte[] thumbNailPhoto = default, string thumbnailPhotoFileName = default)
        {
            using var ctx = new AdventureWorksContext(provider);

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
                ModifiedDate = modifiedDate != null ? modifiedDate.Value.ToUniversalTime() : (DateTime?)null,
                ThumbNailPhoto = thumbNailPhoto,
                ThumbnailPhotoFileName = thumbnailPhotoFileName
            };
            ctx.Add(p);

            await ctx.SaveChangesAsync();

            return p;
        }



        /// <summary>
        /// Get a Product row from the database
        /// </summary>
        public async Task<Product> GetProductAsync(CoreProvider provider, Guid productId)
        {
            using var ctx = new AdventureWorksContext(provider);
            return await ctx.Product.FindAsync(productId);
        }


        /// <summary>
        /// Add a Customer item to the database identified by its name and its provider type.
        /// </summary>
        public async Task<Customer> AddCustomerAsync(string dbName, ProviderType providerType, Guid? customerId = default, string firstName = default, string lastName = default, string companyName = default)
        {
            using var ctx = new AdventureWorksContext(dbName, providerType);

            var customer = new Customer
            {
                CustomerId = customerId == default ? Guid.NewGuid() : customerId.Value,
                FirstName = firstName,
                LastName = lastName,
                CompanyName = companyName,
            };

            ctx.Add(customer);

            await ctx.SaveChangesAsync();

            return customer;

        }

        /// <summary>
        /// Get the server database rows count
        /// </summary>
        /// <returns></returns>
        public int GetDatabaseRowsCount(CoreProvider coreProvider)
        {
            int totalCountRows = 0;

            using var ctx = new AdventureWorksContext(coreProvider, UseFallbackSchema, false);

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


        public void Dispose()
        {

            HelperDatabase.ClearPool(ServerProviderType);

            foreach (var tvp in ClientDatabaseNames)
                HelperDatabase.ClearPool(tvp.Key);

            foreach (var tvp in ClientDatabaseNames)
                HelperDatabase.DropDatabase(tvp.Key, tvp.Value);

            HelperDatabase.DropDatabase(ServerProviderType, ServerDatabaseName);
        }



    }
}
