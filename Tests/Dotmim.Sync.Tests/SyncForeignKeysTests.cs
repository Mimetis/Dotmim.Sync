using Dotmim.Sync.Enumerations;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Tests.Misc;
using Dotmim.Sync.Test.SqlUtils;
using System;
using System.Data.SqlClient;
using System.Threading.Tasks;
using Xunit;

namespace Dotmim.Sync.Test
{
    public class SyncForeignKeysTestsFixture : IDisposable
    {
        private string createTableScript =
        $@"
            CREATE TABLE [dbo].[Product](
	            [ProductID] [int] IDENTITY(1,1) NOT NULL,
	            [ProductCategoryID] [int] NULL,
	            [ProductModelID] [int] NULL,
	            [Name] [nvarchar](50) NOT NULL,
	            [ProductNumber] [nvarchar](25) NOT NULL,
	            [Color] [nvarchar](15) NULL,
	            CONSTRAINT [PK_Product_ProductID] PRIMARY KEY CLUSTERED ([ProductID] ASC));
            CREATE TABLE [dbo].[ProductCategory](
	            [ProductCategoryID] [int] IDENTITY(1,1) NOT NULL,
	            [ParentProductCategoryID] [int] NULL,
	            [Name] [nvarchar](50) NOT NULL,
	            CONSTRAINT [PK_ProductCategory_ProductCategoryID] PRIMARY KEY CLUSTERED ([ProductCategoryID] ASC));
            CREATE TABLE [dbo].[ProductDescription](
	            [ProductDescriptionID] [int] IDENTITY(1,1) NOT NULL,
	            [Description] [nvarchar](400) NOT NULL,
	            CONSTRAINT [PK_ProductDescription_ProductDescriptionID] PRIMARY KEY CLUSTERED ([ProductDescriptionID] ASC));
            CREATE TABLE [dbo].[ProductModel](
	            [ProductModelID] [int] IDENTITY(1,1) NOT NULL,
	            [Name] [nvarchar](50) NOT NULL,
	            [CatalogDescription] [nvarchar](MAX) NULL,	
	            CONSTRAINT [PK_ProductModel_ProductModelID] PRIMARY KEY CLUSTERED ([ProductModelID] ASC));
            CREATE TABLE [dbo].[ProductModelProductDescription](
	            [ProductModelID] [int] NOT NULL,
	            [ProductDescriptionID] [int] NOT NULL,
	            [Culture] [nchar](6) NOT NULL,
                [CreatedDate] [DateTime] NULL default(getdate()),
	            CONSTRAINT [PK_ProductModelProductDescription_ProductModelID_ProductDescriptionID_Culture] PRIMARY KEY CLUSTERED 
            (
	            [ProductModelID] ASC,
	            [ProductDescriptionID] ASC,
	            [Culture] ASC
            ));
            ALTER TABLE [dbo].[Product] ADD  CONSTRAINT [FK_Product_ProductCategory_ProductCategoryID] FOREIGN KEY([ProductCategoryID])
            REFERENCES [dbo].[ProductCategory] ([ProductCategoryID]);
            ALTER TABLE [dbo].[Product] ADD  CONSTRAINT [FK_Product_ProductModel_ProductModelID] FOREIGN KEY([ProductModelID])
            REFERENCES [dbo].[ProductModel] ([ProductModelID]);
            ALTER TABLE [dbo].[ProductCategory] ADD  CONSTRAINT [FK_ProductCategory_ProductCategory_ParentProductCategoryID_ProductCategoryID] FOREIGN KEY([ParentProductCategoryID])
            REFERENCES [dbo].[ProductCategory] ([ProductCategoryID]);
            ALTER TABLE [dbo].[ProductModelProductDescription] ADD  CONSTRAINT [FK_ProductModelProductDescription_ProductDescription_ProductDescriptionID] FOREIGN KEY([ProductDescriptionID])
            REFERENCES [dbo].[ProductDescription] ([ProductDescriptionID]);
            ALTER TABLE [dbo].[ProductModelProductDescription] ADD  CONSTRAINT [FK_ProductModelProductDescription_ProductModel_ProductModelID] FOREIGN KEY([ProductModelID])
            REFERENCES [dbo].[ProductModel] ([ProductModelID]);";

        private string datas =
        $@"
            SET IDENTITY_INSERT [dbo].[ProductCategory] ON 
            INSERT [ProductCategory] ([ProductCategoryID], [Name]) VALUES (1, 'Bikes');
            INSERT [ProductCategory] ([ProductCategoryID], [ParentProductCategoryID], [Name]) VALUES (2, 1, 'Mountain Bikes');
            SET IDENTITY_INSERT [dbo].[ProductCategory] OFF 

            SET IDENTITY_INSERT [dbo].[ProductDescription] ON 
            INSERT [ProductDescription] ([ProductDescriptionID], [Description]) VALUES (1000, 'For true trail addicts.  An extremely durable bike that will go anywhere and keep you in control on challenging terrain - without breaking your budget.');
            SET IDENTITY_INSERT [dbo].[ProductDescription] OFF 

            SET IDENTITY_INSERT [dbo].[ProductModel] ON 
            INSERT [ProductModel] ([ProductModelID], [Name]) VALUES (100, 'Mountain-100');
            INSERT [ProductModel] ([ProductModelID], [Name]) VALUES (101, 'Mountain-200');
            SET IDENTITY_INSERT [dbo].[ProductModel] OFF 

            INSERT [ProductModelProductDescription] ([ProductModelID], [ProductDescriptionID], [Culture]) 
            VALUES (100, 1000, 'en-us');          
            
            SET IDENTITY_INSERT [dbo].[Product] ON 
            INSERT INTO [Product] ([ProductID], [ProductCategoryID], [ProductModelID], [Name], [ProductNumber], [Color])
            VALUES(1, 2, 100, 'HL Mountain Frame - Black, 44', 'FR-M94B-44', 'Black') 
            SET IDENTITY_INSERT [dbo].[Product] OFF 

         ";

        private HelperDB helperDb = new HelperDB();
        private string serverDbName = "Test_FK_Server";
        private string client1DbName = "Test_FK_Client1";
        private string client2DbName = "Test_FK_Client2";

        public String ServerConnectionString => HelperDB.GetDatabaseConnectionString(serverDbName);
        public String Client1ConnectionString => HelperDB.GetDatabaseConnectionString(client1DbName);
        public String Client2ConnectionString => HelperDB.GetDatabaseConnectionString(client2DbName);

        public SyncForeignKeysTestsFixture()
        {
            // create databases
            helperDb.CreateDatabase(serverDbName);
            helperDb.CreateDatabase(client1DbName);
            helperDb.CreateDatabase(client2DbName);

            // create table
            helperDb.ExecuteScript(serverDbName, createTableScript);

            // insert table
            helperDb.ExecuteScript(serverDbName, datas);
        }
        public void Dispose()
        {
            helperDb.DeleteDatabase(serverDbName);
            helperDb.DeleteDatabase(client1DbName);
            helperDb.DeleteDatabase(client2DbName);
        }

    }


    [TestCaseOrderer("Dotmim.Sync.Tests.Misc.PriorityOrderer", "Dotmim.Sync.Tests")]
    public class SyncForeignKeysTests : IClassFixture<SyncForeignKeysTestsFixture>
    {
        SqlSyncProvider serverProvider;
        SqlSyncProvider clientProvider;
        SyncForeignKeysTestsFixture fixture;
        SyncAgent agent;

        public SyncForeignKeysTests(SyncForeignKeysTestsFixture fixture)
        {
            this.fixture = fixture;

        }

        [Fact, TestPriority(0)]
        public async Task Initialize()
        {
            serverProvider = new SqlSyncProvider(fixture.ServerConnectionString);
            clientProvider = new SqlSyncProvider(fixture.Client1ConnectionString);

            agent = new SyncAgent(clientProvider, serverProvider, new[] {
                "ProductCategory",
                "ProductDescription",
                "ProductModel",
                "ProductModelProductDescription",
                "Product" });

            var session = await agent.SynchronizeAsync();

            Assert.Equal(7, session.TotalChangesDownloaded);
            Assert.Equal(0, session.TotalChangesUploaded);

            int fkeysCount = 0;
            using (var sqlConnection = new SqlConnection(fixture.Client1ConnectionString))
            {
                using (var sqlCmd = new SqlCommand("select count(*) from sys.foreign_keys", sqlConnection))
                {
                    sqlConnection.Open();
                    fkeysCount = (int)sqlCmd.ExecuteScalar();
                    sqlConnection.Close();
                }
            }
            Assert.Equal(5, fkeysCount);

        }


        [Fact, TestPriority(1)]
        public async Task TableWithForeignKeyButNotParentTable()
        {
            serverProvider = new SqlSyncProvider(fixture.ServerConnectionString);
            clientProvider = new SqlSyncProvider(fixture.Client2ConnectionString);

            agent = new SyncAgent(clientProvider, serverProvider, new[] {
                "ProductCategory",
                "Product" });

            var session = await agent.SynchronizeAsync();

            Assert.Equal(3, session.TotalChangesDownloaded);
            Assert.Equal(0, session.TotalChangesUploaded);

            int fkeysCount = 0;
            using (var sqlConnection = new SqlConnection(fixture.Client2ConnectionString))
            {
                using (var sqlCmd = new SqlCommand("select count(*) from sys.foreign_keys", sqlConnection))
                {
                    sqlConnection.Open();
                    fkeysCount = (int)sqlCmd.ExecuteScalar();
                    sqlConnection.Close();
                }
            }
            Assert.Equal(2, fkeysCount);

        }


    }
}
