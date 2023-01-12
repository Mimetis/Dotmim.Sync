using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Tests.Core;
using Dotmim.Sync.Tests.Models;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;
using System.Transactions;
using static Microsoft.EntityFrameworkCore.DbLoggerCategory.Database;

namespace Dotmim.Sync.Tests.Fixtures
{

    public class DatabaseServerFixture<T> : IDisposable where T : RelationalFixture
    {
        public virtual List<ProviderType> ClientsType => new List<ProviderType> {
            HelperDatabase.GetProviderType<T>(),
            ProviderType.Sqlite,
            typeof(T) == typeof(SqlServerFixtureType) ? ProviderType.Postgres : ProviderType.Sql };

        public virtual ProviderType ServerProviderType => HelperDatabase.GetProviderType<T>();

        // SQL Server has schema on server database
        protected string salesSchema = typeof(T) == typeof(SqlServerFixtureType) || typeof(T) == typeof(PostgresFixtureType) ? "SalesLT." : "";

        public Stopwatch OverallStopwatch { get; }

        public virtual string[] Tables => new string[]
        {
            $"{salesSchema}ProductCategory", $"{salesSchema}ProductModel", $"{salesSchema}Product",
            "Employee", "Customer", "Address", "CustomerAddress", "EmployeeAddress",
            $"{salesSchema}SalesOrderHeader", $"{salesSchema}SalesOrderDetail",
            "Posts", "Tags", "PostTag",
            "PricesList", "PricesListCategory", "PricesListDetail", "Log"
        };

        public virtual SyncSetup GetSyncSetup() => new SyncSetup(Tables);

        public virtual bool UseFallbackSchema => typeof(T) == typeof(SqlServerFixtureType) || typeof(T) == typeof(PostgresFixtureType);

        public string ServerDatabaseName { get; set; }

        public Dictionary<ProviderType, string> ClientDatabaseNames { get; set; } = new Dictionary<ProviderType, string>();


        public DatabaseServerFixture()
        {
            this.OverallStopwatch = Stopwatch.StartNew();

            this.ServerDatabaseName = HelperDatabase.GetRandomName("tcp_srv");
            //new AdventureWorksContext(ServerDatabaseName, ServerProviderType, UseFallbackSchema, true).Database.EnsureCreated();

            foreach (var type in this.ClientsType)
            {
                var dbName = HelperDatabase.GetRandomName("tcp_cli");
                //new AdventureWorksContext(dbName, type, UseFallbackSchema, false).Database.EnsureCreated();
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


        public void EnsureTablesAreCreated(CoreProvider coreProvider, bool seeding)
        {
            var (t, d) = HelperDatabase.GetDatabaseType(coreProvider);

            if (t == ProviderType.Sqlite)
                HelperDatabase.DropDatabase(t, d);

            new AdventureWorksContext(coreProvider, UseFallbackSchema, seeding).Database.EnsureCreated();

            var localOrchestrator = new LocalOrchestrator(coreProvider);
            using var c = coreProvider.CreateConnection();
            c.Open();

            var setup = localOrchestrator.GetAllTablesAsync(c).GetAwaiter().GetResult();

            if (!setup.HasTables)
            {
                Console.WriteLine($"Tables not created for provider {t} in database {d}");
                Debug.WriteLine($"Tables not created for provider {t} in database {d}");

            }
            c.Close();

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
            string productCategoryId = default, string parentProductCategoryId = default, string name = default, Guid? rowguid = default,
            DateTime? modifiedDate = default, string attributeWithSpace = default, DbConnection connection = null, DbTransaction transaction = null)
        {
            using var ctx = new AdventureWorksContext(provider, UseFallbackSchema);

            if (connection != null)
                ctx.Database.SetDbConnection(connection);

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
        public async Task<ProductCategory> UpdateProductCategoryAsync(CoreProvider provider, ProductCategory productCategory, DbConnection connection = null, DbTransaction transaction = null)
        {
            using var ctx = new AdventureWorksContext(provider, UseFallbackSchema);

            if (connection != null)
                ctx.Database.SetDbConnection(connection);

            if (transaction != null)
                ctx.Database.UseTransaction(transaction);

            ctx.ProductCategory.Add(productCategory);
            ctx.Entry(productCategory).State = EntityState.Modified;

            await ctx.SaveChangesAsync();

            return productCategory;
        }

        /// <summary>
        /// Delete a ProductCategory row to the database
        /// </summary>
        public async Task DeleteProductCategoryAsync(CoreProvider provider, string productCategoryId, DbConnection connection = null, DbTransaction transaction = null)
        {
            using var ctx = new AdventureWorksContext(provider, UseFallbackSchema);

            if (connection != null)
                ctx.Database.SetDbConnection(connection);

            if (transaction != null)
                ctx.Database.UseTransaction(transaction);

            var pc = await ctx.ProductCategory.FirstOrDefaultAsync(pc => pc.ProductCategoryId == productCategoryId);
            ctx.ProductCategory.Remove(pc);

            await ctx.SaveChangesAsync();
        }

        /// <summary>
        /// Get a ProductCategory row from the database
        /// </summary>
        public async Task<ProductCategory> GetProductCategoryAsync(CoreProvider provider, string productCategoryId, DbConnection connection = null, DbTransaction transaction = null)
        {
            using var ctx = new AdventureWorksContext(provider, UseFallbackSchema);

            if (connection != null)
                ctx.Database.SetDbConnection(connection);

            if (transaction != null)
                ctx.Database.UseTransaction(transaction);

            return await ctx.ProductCategory.FindAsync(productCategoryId);
        }

        /// Add a Product item to the database identified by its name and its provider type
        public async Task<Product> AddProductAsync(CoreProvider provider,
            Guid? productId = default,
            string productNumber = default, string name = default, string productCategoryId = default,
            string color = default, decimal? standardCost = default, decimal? listPrice = default, string size = default, decimal? weight = default, Guid? rowguid = default,
            DateTime? modifiedDate = default, byte[] thumbNailPhoto = default, string thumbnailPhotoFileName = default, DbConnection connection = null, DbTransaction transaction = null)
        {
            using var ctx = new AdventureWorksContext(provider, UseFallbackSchema);

            if (connection != null)
                ctx.Database.SetDbConnection(connection);

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
        public async Task<Product> GetProductAsync(CoreProvider provider, Guid productId, DbConnection connection = null, DbTransaction transaction = null)
        {
            using var ctx = new AdventureWorksContext(provider, UseFallbackSchema);

            if (connection != null)
                ctx.Database.SetDbConnection(connection);

            if (transaction != null)
                ctx.Database.UseTransaction(transaction);

            return await ctx.Product.FindAsync(productId);
        }

        /// <summary>
        /// Update a Product row to the database
        /// </summary>
        public async Task<Product> UpdateProductAsync(CoreProvider provider, Product product, DbConnection connection = null, DbTransaction transaction = null)
        {
            using var ctx = new AdventureWorksContext(provider, UseFallbackSchema);

            if (connection != null)
                ctx.Database.SetDbConnection(connection);

            if (transaction != null)
                ctx.Database.UseTransaction(transaction);

            ctx.Product.Add(product);
            ctx.Entry(product).State = EntityState.Modified;

            await ctx.SaveChangesAsync();

            return product;
        }

        /// <summary>
        /// Delete a Product row to the database
        /// </summary>
        public async Task DeleteProductAsync(CoreProvider provider, Guid productId, DbConnection connection = null, DbTransaction transaction = null)
        {
            using var ctx = new AdventureWorksContext(provider, UseFallbackSchema);

            if (connection != null)
                ctx.Database.SetDbConnection(connection);

            if (transaction != null)
                ctx.Database.UseTransaction(transaction);

            var pc = await ctx.Product.FirstOrDefaultAsync(pc => pc.ProductId == productId);
            ctx.Product.Remove(pc);

            await ctx.SaveChangesAsync();
        }

        /// <summary>
        /// Add a price list
        /// </summary>
        public async Task<PriceList> AddPriceListAsync(CoreProvider provider, Guid? priceListId = default, string description = default, DateTime? from = default,
            DateTime? to = default, DbConnection connection = null, DbTransaction transaction = null)
        {
            using var ctx = new AdventureWorksContext(provider, UseFallbackSchema);

            if (connection != null)
                ctx.Database.SetDbConnection(connection);

            if (transaction != null)
                ctx.Database.UseTransaction(transaction);

            priceListId = priceListId.HasValue ? priceListId : Guid.NewGuid();
            description = string.IsNullOrEmpty(description) ? HelperDatabase.GetRandomName() : description;

            var pl = new PriceList
            {
                PriceListId = priceListId.Value,
                Description = description,
                From = from != null ? from.Value.ToUniversalTime() : (DateTime?)null,
                To = to != null ? to.Value.ToUniversalTime() : (DateTime?)null
            };
            ctx.Add(pl);

            await ctx.SaveChangesAsync();

            return pl;
        }


        /// <summary>
        /// Get a PriceList row from the database
        /// </summary>
        public async Task<PriceList> GetPriceListAsync(CoreProvider provider, Guid priceListId, DbConnection connection = null, DbTransaction transaction = null)
        {
            using var ctx = new AdventureWorksContext(provider, UseFallbackSchema);

            if (connection != null)
                ctx.Database.SetDbConnection(connection);

            if (transaction != null)
                ctx.Database.UseTransaction(transaction);

            return await ctx.PricesList.FindAsync(priceListId);
        }

        /// <summary>
        /// Add a Customer item to the database identified by its name and its provider type.
        /// </summary>
        public async Task<Customer> AddCustomerAsync(CoreProvider provider, Guid? customerId = default, string firstName = default,
            string lastName = default, string companyName = default, DbConnection connection = null, DbTransaction transaction = null)
        {
            using var ctx = new AdventureWorksContext(provider, UseFallbackSchema);

            if (connection != null)
                ctx.Database.SetDbConnection(connection);

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

        /// <summary>
        /// Add an Address item to the database identified by its name and its provider type.
        /// </summary>
        public async Task<Address> AddAddressAsync(CoreProvider provider, int? addressId = default, string addressLine1 = default, string addressLine2 = default,
            string city = default, string postalCode = default, DbConnection connection = null, DbTransaction transaction = null)
        {
            using var ctx = new AdventureWorksContext(provider, UseFallbackSchema);

            var (providerType, _) = HelperDatabase.GetDatabaseType(provider);

            if (connection != null)
                ctx.Database.SetDbConnection(connection);

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

        /// <summary>
        /// Update an Address row to the database
        /// </summary>
        public async Task<Address> UpdateAddressAsync(CoreProvider provider, Address address, DbConnection connection = null, DbTransaction transaction = null)
        {
            using var ctx = new AdventureWorksContext(provider, UseFallbackSchema);

            if (connection != null)
                ctx.Database.SetDbConnection(connection);

            if (transaction != null)
                ctx.Database.UseTransaction(transaction);

            ctx.Address.Add(address);
            ctx.Entry(address).State = EntityState.Modified;

            await ctx.SaveChangesAsync();

            return address;
        }

        /// <summary>
        /// Delete an Address row to the database
        /// </summary>
        public async Task DeleteAddressAsync(CoreProvider provider, int addressId, DbConnection connection = null, DbTransaction transaction = null)
        {
            using var ctx = new AdventureWorksContext(provider, UseFallbackSchema);

            if (connection != null)
                ctx.Database.SetDbConnection(connection);

            if (transaction != null)
                ctx.Database.UseTransaction(transaction);

            var pc = await ctx.Address.FirstOrDefaultAsync(pc => pc.AddressId == addressId);
            ctx.Address.Remove(pc);

            await ctx.SaveChangesAsync();
        }

        /// <summary>
        /// Get an Address row from the database
        /// </summary>
        public async Task<Address> GetAddressAsync(CoreProvider provider, int addressId, DbConnection connection = null, DbTransaction transaction = null)
        {
            using var ctx = new AdventureWorksContext(provider, UseFallbackSchema);

            if (connection != null)
                ctx.Database.SetDbConnection(connection);

            if (transaction != null)
                ctx.Database.UseTransaction(transaction);

            return await ctx.Address.FindAsync(addressId);
        }


        /// <summary>
        /// Add a CustomerAddress item to the database identified by its addressId and customerId and its provider type.
        /// </summary>
        public async Task<CustomerAddress> AddCustomerAddressAsync(CoreProvider provider, int addressId, Guid customerId, string addressType = default,
            DbConnection connection = null, DbTransaction transaction = null)
        {
            using var ctx = new AdventureWorksContext(provider, UseFallbackSchema);

            if (connection != null)
                ctx.Database.SetDbConnection(connection);

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

        /// <summary>
        /// Update a CustomerAddress row to the database
        /// </summary>
        public async Task<CustomerAddress> UpdateCustomerAddressAsync(CoreProvider provider, CustomerAddress customerAddress, DbConnection connection = null, DbTransaction transaction = null)
        {
            using var ctx = new AdventureWorksContext(provider, UseFallbackSchema);

            if (connection != null)
                ctx.Database.SetDbConnection(connection);

            if (transaction != null)
                ctx.Database.UseTransaction(transaction);

            ctx.CustomerAddress.Add(customerAddress);
            ctx.Entry(customerAddress).State = EntityState.Modified;

            await ctx.SaveChangesAsync();

            return customerAddress;
        }

        /// <summary>
        /// Delete a CustomerAddress row to the database
        /// </summary>
        public async Task DeleteCustomerAddressAsync(CoreProvider provider, int addressId, Guid customerId, DbConnection connection = null, DbTransaction transaction = null)
        {
            using var ctx = new AdventureWorksContext(provider, UseFallbackSchema);

            if (connection != null)
                ctx.Database.SetDbConnection(connection);

            if (transaction != null)
                ctx.Database.UseTransaction(transaction);

            var pc = await ctx.CustomerAddress.FirstOrDefaultAsync(pc => pc.AddressId == addressId && pc.CustomerId == customerId);
            ctx.CustomerAddress.Remove(pc);

            await ctx.SaveChangesAsync();
        }

        /// <summary>
        /// Get a CustomerAddress row from the database
        /// </summary>
        public async Task<CustomerAddress> GetCustomerAddressAsync(CoreProvider provider, int addressId, Guid customerId, DbConnection connection = null, DbTransaction transaction = null)
        {
            using var ctx = new AdventureWorksContext(provider, UseFallbackSchema);

            if (connection != null)
                ctx.Database.SetDbConnection(connection);

            if (transaction != null)
                ctx.Database.UseTransaction(transaction);

            return await ctx.CustomerAddress.FirstOrDefaultAsync(pc => pc.AddressId == addressId && pc.CustomerId == customerId);
        }

        /// <summary>
        /// Add a SalesOrderHeader item to the database identified by its provider type.
        /// </summary>
        public async Task<SalesOrderHeader> AddSalesOrderHeaderAsync(CoreProvider provider, Guid? customerId, int? salesOrderId = default,
            DbConnection connection = null, DbTransaction transaction = null)
        {
            using var ctx = new AdventureWorksContext(provider, UseFallbackSchema);

            var (providerType, _) = HelperDatabase.GetDatabaseType(provider);

            if (connection != null)
                ctx.Database.SetDbConnection(connection);

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

            var sohTableName = UseFallbackSchema ? "SalesLT.SalesOrderHeader" : "SalesOrderHeader";

            if (providerType == ProviderType.Sql && salesOrderId.HasValue)
                ctx.Database.ExecuteSqlRaw($"SET IDENTITY_INSERT {sohTableName} ON;");

            await ctx.SaveChangesAsync();

            if (providerType == ProviderType.Sql && salesOrderId.HasValue)
                ctx.Database.ExecuteSqlRaw($"SET IDENTITY_INSERT {sohTableName} OFF;");

            ctx.Database.CloseConnection();

            return soh;

        }

        /// <summary>
        /// Update a SalesOrderHeader row to the database
        /// </summary>
        public async Task<SalesOrderHeader> UpdateSalesOrderHeaderAsync(CoreProvider provider, SalesOrderHeader salesOrderHeader, DbConnection connection = null, DbTransaction transaction = null)
        {
            using var ctx = new AdventureWorksContext(provider, UseFallbackSchema);

            if (connection != null)
                ctx.Database.SetDbConnection(connection);

            if (transaction != null)
                ctx.Database.UseTransaction(transaction);

            ctx.SalesOrderHeader.Add(salesOrderHeader);
            ctx.Entry(salesOrderHeader).State = EntityState.Modified;

            await ctx.SaveChangesAsync();

            return salesOrderHeader;
        }

        /// <summary>
        /// Delete a SalesOrderHeader row to the database
        /// </summary>
        public async Task DeleteSalesOrderHeaderAsync(CoreProvider provider, int salesOrderId, DbConnection connection = null, DbTransaction transaction = null)
        {
            using var ctx = new AdventureWorksContext(provider, UseFallbackSchema);

            if (connection != null)
                ctx.Database.SetDbConnection(connection);

            if (transaction != null)
                ctx.Database.UseTransaction(transaction);

            var pc = await ctx.SalesOrderHeader.FirstOrDefaultAsync(pc => pc.SalesOrderId == salesOrderId);
            ctx.SalesOrderHeader.Remove(pc);

            await ctx.SaveChangesAsync();
        }

        /// <summary>
        /// Get a SalesOrderHeader row from the database
        /// </summary>
        public async Task<SalesOrderHeader> GetSalesOrderHeaderAsync(CoreProvider provider, int salesOrderId, DbConnection connection = null, DbTransaction transaction = null)
        {
            using var ctx = new AdventureWorksContext(provider, UseFallbackSchema);

            if (connection != null)
                ctx.Database.SetDbConnection(connection);

            if (transaction != null)
                ctx.Database.UseTransaction(transaction);

            return await ctx.SalesOrderHeader.FindAsync(salesOrderId);
        }

        /// <summary>
        /// Add a SalesOrderDetail item to the database identified by its provider type.
        /// </summary>
        public async Task<SalesOrderDetail> AddSalesOrderDetailAsync(CoreProvider provider, int salesOrderId, Guid productId,
            int? salesOrderDetailId = default, short? orderQty = default, decimal? unitPrice = default,
            DbConnection connection = null, DbTransaction transaction = null)
        {
            using var ctx = new AdventureWorksContext(provider, UseFallbackSchema);

            var (providerType, _) = HelperDatabase.GetDatabaseType(provider);

            if (connection != null)
                ctx.Database.SetDbConnection(connection);

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

            var sodTableName = UseFallbackSchema ? "SalesLT.SalesOrderDetail" : "SalesOrderDetail";

            if (providerType == ProviderType.Sql && salesOrderDetailId.HasValue)
                ctx.Database.ExecuteSqlRaw($"SET IDENTITY_INSERT {sodTableName} ON;");

            await ctx.SaveChangesAsync();

            if (providerType == ProviderType.Sql && salesOrderDetailId.HasValue)
                ctx.Database.ExecuteSqlRaw($"SET IDENTITY_INSERT {sodTableName} OFF;");

            ctx.Database.CloseConnection();

            return sod;

        }

        /// <summary>
        /// Update a SalesOrderDetail row to the database
        /// </summary>
        public async Task<SalesOrderDetail> UpdateSalesOrderDetailAsync(CoreProvider provider, SalesOrderDetail salesOrderDetail, DbConnection connection = null, DbTransaction transaction = null)
        {
            using var ctx = new AdventureWorksContext(provider, UseFallbackSchema);

            if (connection != null)
                ctx.Database.SetDbConnection(connection);

            if (transaction != null)
                ctx.Database.UseTransaction(transaction);

            ctx.SalesOrderDetail.Add(salesOrderDetail);
            ctx.Entry(salesOrderDetail).State = EntityState.Modified;

            await ctx.SaveChangesAsync();

            return salesOrderDetail;
        }

        /// <summary>
        /// Delete a SalesOrderDetail row to the database
        /// </summary>
        public async Task DeleteSalesOrderDetailAsync(CoreProvider provider, int salesOrderDetailId, DbConnection connection = null, DbTransaction transaction = null)
        {
            using var ctx = new AdventureWorksContext(provider, UseFallbackSchema);

            if (connection != null)
                ctx.Database.SetDbConnection(connection);

            if (transaction != null)
                ctx.Database.UseTransaction(transaction);

            var pc = await ctx.SalesOrderDetail.FirstOrDefaultAsync(pc => pc.SalesOrderDetailId == salesOrderDetailId);
            ctx.SalesOrderDetail.Remove(pc);

            await ctx.SaveChangesAsync();
        }

        /// <summary>
        /// Get a SalesOrderDetail row from the database
        /// </summary>
        public async Task<SalesOrderDetail> GetSalesOrderDetailAsync(CoreProvider provider, int salesOrderDetailId, DbConnection connection = null, DbTransaction transaction = null)
        {
            using var ctx = new AdventureWorksContext(provider, UseFallbackSchema);

            if (connection != null)
                ctx.Database.SetDbConnection(connection);

            if (transaction != null)
                ctx.Database.UseTransaction(transaction);

            return await ctx.SalesOrderDetail.FindAsync(salesOrderDetailId);
        }


        /// <summary>
        /// Get the server database rows count
        /// </summary>
        /// <returns></returns>
        public virtual int GetDatabaseRowsCount(CoreProvider coreProvider)
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

            this.OverallStopwatch.Stop();

        }



    }
}
