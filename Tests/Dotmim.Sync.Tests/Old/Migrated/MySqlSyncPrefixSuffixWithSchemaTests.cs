//using Dotmim.Sync.MySql;
//using Dotmim.Sync.SqlServer;
//using Dotmim.Sync.Tests.Misc;
//using System;
//using System.Threading.Tasks;
//using Xunit;

//namespace Dotmim.Sync.Tests.Old.MySql
//{

//    public class MySqlSyncPreffixAndSuffixWithSchemaFixture : IDisposable
//    {
//        private readonly string createSchemaScript = $@"CREATE SCHEMA [SalesLT]";

//        private readonly string createTableScript =
//        $@" CREATE TABLE [SalesLT].[Product](
//	            [ProductID] [int] IDENTITY(1,1) NOT NULL,
//	            [ProductCategoryID] [int] NULL,
//	            [ProductModelID] [int] NULL,
//	            [Name] [nvarchar](50) NOT NULL,
//	            [ProductNumber] [nvarchar](25) NOT NULL,
//	            [Color] [nvarchar](15) NULL,
//	            CONSTRAINT [PK_Product_ProductID] PRIMARY KEY CLUSTERED ([ProductID] ASC));

//            CREATE TABLE [SalesLT].[ProductCategory](
//	            [ProductCategoryID] [int] IDENTITY(1,1) NOT NULL,
//	            [ParentProductCategoryID] [int] NULL,
//	            [Name] [nvarchar](50) NOT NULL,
//	            CONSTRAINT [PK_ProductCategory_ProductCategoryID] PRIMARY KEY CLUSTERED ([ProductCategoryID] ASC));

//            CREATE TABLE [SalesLT].[ProductDescription](
//	            [ProductDescriptionID] [int] IDENTITY(1,1) NOT NULL,
//	            [Description] [nvarchar](400) NOT NULL,
//	            CONSTRAINT [PK_ProductDescription_ProductDescriptionID] PRIMARY KEY CLUSTERED ([ProductDescriptionID] ASC));

//            CREATE TABLE [SalesLT].[ProductModel](
//	            [ProductModelID] [int] IDENTITY(1,1) NOT NULL,
//	            [Name] [nvarchar](50) NOT NULL,
//	            [CatalogDescription] [nvarchar](MAX) NULL,	
//	            CONSTRAINT [PK_ProductModel_ProductModelID] PRIMARY KEY CLUSTERED ([ProductModelID] ASC));
            
//            CREATE TABLE [SalesLT].[ProductModelProductDescription](
//	            [ProductModelID] [int] NOT NULL,
//	            [ProductDescriptionID] [int] NOT NULL,
//	            [Culture] [nchar](6) NOT NULL,
//                [CreatedDate] [DateTime] NULL default(getdate()),
//	            CONSTRAINT [PK_ProductModelProductDescription_ProductModelID_ProductDescriptionID_Culture] PRIMARY KEY CLUSTERED 
//            (
//	            [ProductModelID] ASC,
//	            [ProductDescriptionID] ASC,
//	            [Culture] ASC
//            ));
//            ALTER TABLE [SalesLT].[Product] ADD  CONSTRAINT [FK_Product_ProductCategory_ProductCategoryID] FOREIGN KEY([ProductCategoryID])
//            REFERENCES [SalesLT].[ProductCategory] ([ProductCategoryID]);
//            ALTER TABLE [SalesLT].[Product] ADD  CONSTRAINT [FK_Product_ProductModel_ProductModelID] FOREIGN KEY([ProductModelID])
//            REFERENCES [SalesLT].[ProductModel] ([ProductModelID]);
//            ALTER TABLE [SalesLT].[ProductCategory] ADD  CONSTRAINT [FK_ProductCategory_ProductCategory_ParentProductCategoryID_ProductCategoryID] FOREIGN KEY([ParentProductCategoryID])
//            REFERENCES [SalesLT].[ProductCategory] ([ProductCategoryID]);
//            ALTER TABLE [SalesLT].[ProductModelProductDescription] ADD  CONSTRAINT [FK_ProductModelProductDescription_ProductDescription_ProductDescriptionID] FOREIGN KEY([ProductDescriptionID])
//            REFERENCES [SalesLT].[ProductDescription] ([ProductDescriptionID]);
//            ALTER TABLE [SalesLT].[ProductModelProductDescription] ADD  CONSTRAINT [FK_ProductModelProductDescription_ProductModel_ProductModelID] FOREIGN KEY([ProductModelID])
//            REFERENCES [SalesLT].[ProductModel] ([ProductModelID]);";

//        private string datas =
//        $@"
//            SET IDENTITY_INSERT [SalesLT].[ProductCategory] ON 
//            INSERT [SalesLT].[ProductCategory] ([ProductCategoryID], [Name]) VALUES (1, 'Bikes');
//            INSERT [SalesLT].[ProductCategory] ([ProductCategoryID], [ParentProductCategoryID], [Name]) VALUES (2, 1, 'Mountain Bikes');
//            SET IDENTITY_INSERT [SalesLT].[ProductCategory] OFF 

//            SET IDENTITY_INSERT [SalesLT].[ProductDescription] ON 
//            INSERT [SalesLT].[ProductDescription] ([ProductDescriptionID], [Description]) VALUES (1000, 'For true trail addicts.  An extremely durable bike that will go anywhere and keep you in control on challenging terrain - without breaking your budget.');
//            SET IDENTITY_INSERT [SalesLT].[ProductDescription] OFF 

//            SET IDENTITY_INSERT [SalesLT].[ProductModel] ON 
//            INSERT [SalesLT].[ProductModel] ([ProductModelID], [Name]) VALUES (100, 'Mountain-100');
//            INSERT [SalesLT].[ProductModel] ([ProductModelID], [Name]) VALUES (101, 'Mountain-200');
//            SET IDENTITY_INSERT [SalesLT].[ProductModel] OFF 

//            INSERT [SalesLT].[ProductModelProductDescription] ([ProductModelID], [ProductDescriptionID], [Culture]) 
//            VALUES (100, 1000, 'en-us');          
            
//            SET IDENTITY_INSERT [SalesLT].[Product] ON 
//            INSERT INTO [SalesLT].[Product] ([ProductID], [ProductCategoryID], [ProductModelID], [Name], [ProductNumber], [Color])
//            VALUES(1, 2, 100, 'HL Mountain Frame - Black, 44', 'FR-M94B-44', 'Black') 
//            SET IDENTITY_INSERT [SalesLT].[Product] OFF 

//         ";

        
//        private string serverDbName = "TestServerForMySqlPrefixWithSchema";
//        private string client1DbName = "testmysqlshemasuffixpreffix";

//        public string[] Tables => new string[] {
//            "SalesLT.ProductDescription",
//            "SalesLT.ProductModel",
//            "SalesLT.ProductCategory",
//            "SalesLT.ProductModelProductDescription",
//            "SalesLT.Product",

//        };


//        public String ServerConnectionString => HelperDB.GetSqlDatabaseConnectionString(serverDbName);
//        public String Client1ConnectionString => HelperDB.GetMySqlDatabaseConnectionString(client1DbName);

//        public MySqlSyncPreffixAndSuffixWithSchemaFixture()
//        {
//            // create databases
//            HelperDB.CreateDatabase(serverDbName);
//            HelperDB.CreateMySqlDatabase(client1DbName);

//            // create table
//            HelperDB.ExecuteSqlScript(serverDbName, createSchemaScript);
//            HelperDB.ExecuteSqlScript(serverDbName, createTableScript);

//            // insert table
//            HelperDB.ExecuteSqlScript(serverDbName, datas);
//        }
//        public void Dispose()
//        {
//            HelperDB.DropSqlDatabase(serverDbName);
//            HelperDB.DropMySqlDatabase(client1DbName);
//        }

//    }


//    [TestCaseOrderer("Dotmim.Sync.Tests.Misc.PriorityOrderer", "Dotmim.Sync.Tests")]
//    public class MySqlSyncPrefixSuffixWithSchemaTests : IClassFixture<MySqlSyncPreffixAndSuffixWithSchemaFixture>
//    {
//        SqlSyncProvider serverProvider;
//        MySqlSyncProvider clientProvider;
//        MySqlSyncPreffixAndSuffixWithSchemaFixture fixture;
//        SyncAgent agent;

//        public MySqlSyncPrefixSuffixWithSchemaTests(MySqlSyncPreffixAndSuffixWithSchemaFixture fixture)
//        {
//            this.fixture = fixture;

//            serverProvider = new SqlSyncProvider(fixture.ServerConnectionString);
//            clientProvider = new MySqlSyncProvider(fixture.Client1ConnectionString);
//            var simpleConfiguration = new SyncConfiguration();

//            agent = new SyncAgent(clientProvider, serverProvider, fixture.Tables);
//            agent.Configuration.StoredProceduresPrefix = "ddl_";
//            agent.Configuration.StoredProceduresSuffix = "_sync";
//            agent.Configuration.TrackingTablesPrefix = "sync_";
//            agent.Configuration.TrackingTablesSuffix = "_tr";

//        }

//        [Fact, TestPriority(0)]
//        public async Task Initialize()
//        {
//            var session = await agent.SynchronizeAsync();

//            Assert.Equal(7, session.TotalChangesDownloaded);
//            Assert.Equal(0, session.TotalChangesUploaded);
//        }


//        [Fact, TestPriority(1)]
//        public async Task SyncNoRows()
//        {
//            var session = await agent.SynchronizeAsync();

//            Assert.Equal(0, session.TotalChangesDownloaded);
//            Assert.Equal(0, session.TotalChangesUploaded);
//        }

//    }

//}
