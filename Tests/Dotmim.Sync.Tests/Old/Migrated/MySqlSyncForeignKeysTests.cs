//using Dotmim.Sync.MySql;
//using Dotmim.Sync.SqlServer;
//using Dotmim.Sync.Tests.Misc;
//using MySql.Data.MySqlClient;
//using System;
//using System.Collections.Generic;
//using System.Text;
//using System.Threading.Tasks;
//using Dotmim.Sync.Enumerations;
//using Xunit;

//namespace Dotmim.Sync.Tests.Old.MySql
//{

//    public class MySqlSyncForeignKeysTestsFixture : IDisposable
//    {
//        private string createTableScript =
//        $@"
//            CREATE TABLE `Product`(
//	            `ProductID` int AUTO_INCREMENT NOT NULL,
//	            `ProductCategoryID` int NULL,
//	            `ProductModelID` int NULL,
//	            `Name` nvarchar(50) NOT NULL,
//	            `ProductNumber` nvarchar(25) NOT NULL,
//	            `Color` nvarchar(15) NULL,
//	            CONSTRAINT `PK_Product_ProductID` PRIMARY KEY CLUSTERED (`ProductID` ASC));
//            CREATE TABLE `ProductCategory`(
//	            `ProductCategoryID` int AUTO_INCREMENT NOT NULL,
//	            `ParentProductCategoryID` int NULL,
//	            `Name` nvarchar(50) NOT NULL,
//	            CONSTRAINT `PK_ProductCategory_ProductCategoryID` PRIMARY KEY CLUSTERED (`ProductCategoryID` ASC));
//            CREATE TABLE `ProductDescription`(
//	            `ProductDescriptionID` int AUTO_INCREMENT NOT NULL,
//	            `Description` nvarchar(400) NOT NULL,
//	            CONSTRAINT `PK_ProductDescription_ProductDescriptionID` PRIMARY KEY CLUSTERED (`ProductDescriptionID` ASC));
//            CREATE TABLE `ProductModel`(
//	            `ProductModelID` int AUTO_INCREMENT NOT NULL,
//	            `Name` nvarchar(50) NOT NULL,
//	            `CatalogDescription` LONGTEXT NULL,	
//	            CONSTRAINT `PK_ProductModel_ProductModelID` PRIMARY KEY CLUSTERED (`ProductModelID` ASC));
//            CREATE TABLE `ProductModelProductDescription`(
//	            `ProductModelID` int NOT NULL,
//	            `ProductDescriptionID` int NOT NULL,
//	            `Culture` nchar(6) NOT NULL,
//                `CreatedDate` DateTime NULL ,
//	            CONSTRAINT `PK_ProductModelProductDescription_Culture` PRIMARY KEY CLUSTERED 
//            (
//	            `ProductModelID` ASC,
//	            `ProductDescriptionID` ASC,
//	            `Culture` ASC
//            ));
//            ALTER TABLE `Product` ADD  CONSTRAINT `FK_Product_ProductCategory_ProductCategoryID` FOREIGN KEY(`ProductCategoryID`)
//            REFERENCES `ProductCategory` (`ProductCategoryID`);
//            ALTER TABLE `Product` ADD  CONSTRAINT `FK_Product_ProductModel_ProductModelID` FOREIGN KEY(`ProductModelID`)
//            REFERENCES `ProductModel` (`ProductModelID`);
//            ALTER TABLE `ProductCategory` ADD  CONSTRAINT `FK_ProductCategory_ProductCategoryID` FOREIGN KEY(`ParentProductCategoryID`)
//            REFERENCES `ProductCategory` (`ProductCategoryID`);
//            ALTER TABLE `ProductModelProductDescription` ADD  CONSTRAINT `FK_ProductModelProductDescription_ProductDescriptionID` FOREIGN KEY(`ProductDescriptionID`)
//            REFERENCES `ProductDescription` (`ProductDescriptionID`);
//            ALTER TABLE `ProductModelProductDescription` ADD  CONSTRAINT `FK_ProductModelProductDescription_ProductModelID` FOREIGN KEY(`ProductModelID`)
//            REFERENCES `ProductModel` (`ProductModelID`);";

//        private string datas =
//        $@"
//            INSERT `ProductCategory` (`ProductCategoryID`, `Name`) VALUES (1, 'Bikes');
//            INSERT `ProductCategory` (`ProductCategoryID`, `ParentProductCategoryID`, `Name`) VALUES (2, 1, 'Mountain Bikes');

//            INSERT `ProductDescription` (`ProductDescriptionID`, `Description`) VALUES (1000, 'For true trail addicts.  An extremely durable bike that will go anywhere and keep you in control on challenging terrain - without breaking your budget.');

//            INSERT `ProductModel` (`ProductModelID`, `Name`) VALUES (100, 'Mountain-100');
//            INSERT `ProductModel` (`ProductModelID`, `Name`) VALUES (101, 'Mountain-200');

//            INSERT `ProductModelProductDescription` (`ProductModelID`, `ProductDescriptionID`, `Culture`) 
//            VALUES (100, 1000, 'en-us');          
            
//            INSERT INTO `Product` (`ProductID`, `ProductCategoryID`, `ProductModelID`, `Name`, `ProductNumber`, `Color`)
//            VALUES(1, 2, 100, 'HL Mountain Frame - Black, 44', 'FR-M94B-44', 'Black');

//         ";

        
//        internal string serverDbName = "Test_FK_Server";
//        internal string client1DbName = "Test_FK_Client1";
//        internal string client2DbName = "Test_FK_Client2";

//        public String ServerConnectionString => HelperDB.GetMySqlDatabaseConnectionString(serverDbName);
//        public String Client1ConnectionString => HelperDB.GetMySqlDatabaseConnectionString(client1DbName);
//        public String Client2ConnectionString => HelperDB.GetMySqlDatabaseConnectionString(client2DbName);

//        public MySqlSyncForeignKeysTestsFixture()
//        {
//            // create databases
//            HelperDB.CreateMySqlDatabase(serverDbName);
//            HelperDB.CreateMySqlDatabase(client1DbName);
//            HelperDB.CreateMySqlDatabase(client2DbName);

//            // create table
//            HelperDB.ExecuteMySqlScript(serverDbName, createTableScript);

//            // insert table
//            HelperDB.ExecuteMySqlScript(serverDbName, datas);
//        }
//        public void Dispose()
//        {
//            HelperDB.DropMySqlDatabase(serverDbName);
//            HelperDB.DropMySqlDatabase(client1DbName);
//            HelperDB.DropMySqlDatabase(client2DbName);
//        }

//    }


//    [TestCaseOrderer("Dotmim.Sync.Tests.Misc.PriorityOrderer", "Dotmim.Sync.Tests")]
//    public class MySqlSyncForeignKeysTests : IClassFixture<MySqlSyncForeignKeysTestsFixture>
//    {
//        MySqlSyncProvider serverProvider;
//        MySqlSyncProvider clientProvider;
//        MySqlSyncForeignKeysTestsFixture fixture;
//        SyncAgent agent;

//        public MySqlSyncForeignKeysTests(MySqlSyncForeignKeysTestsFixture fixture)
//        {
//            this.fixture = fixture;

//        }

//        [Fact, TestPriority(0)]
//        public async Task Initialize()
//        {
//            serverProvider = new MySqlSyncProvider(fixture.ServerConnectionString);
//            clientProvider = new MySqlSyncProvider(fixture.Client1ConnectionString);
           
//            agent = new SyncAgent(clientProvider, serverProvider, new[] {
//                "ProductCategory",
//                "ProductDescription",
//                "ProductModel",
//                "ProductModelProductDescription",
//                "Product" });

//            var session = await agent.SynchronizeAsync();

//            Assert.Equal(7, session.TotalChangesDownloaded);
//            Assert.Equal(0, session.TotalChangesUploaded);

//            int fkeysCount = 0;
//            using (var sqlConnection = new MySqlConnection(fixture.Client1ConnectionString))
//            {
//                using (var sqlCmd = new MySqlCommand("SELECT count(*) FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE " +
//                        "WHERE TABLE_SCHEMA = @schema AND REFERENCED_TABLE_NAME is not null", sqlConnection))
//                {
//                    sqlCmd.Parameters.AddWithValue("@schema", fixture.client1DbName);
//                    sqlConnection.Open();
//                    fkeysCount = Convert.ToInt32(sqlCmd.ExecuteScalar());
//                    sqlConnection.Close();
//                }
//            }
//            Assert.Equal(5, fkeysCount);

//        }


//        [Fact, TestPriority(1)]
//        public async Task TableWithForeignKeyButNotParentTable()
//        {
//            serverProvider = new MySqlSyncProvider(fixture.ServerConnectionString);
//            clientProvider = new MySqlSyncProvider(fixture.Client2ConnectionString);
            
//            agent = new SyncAgent(clientProvider, serverProvider, new[] {
//                "ProductCategory",
//                "Product" });

//            var session = await agent.SynchronizeAsync();

//            Assert.Equal(3, session.TotalChangesDownloaded);
//            Assert.Equal(0, session.TotalChangesUploaded);

//            int fkeysCount = 0;
//            using (var sqlConnection = new MySqlConnection(fixture.Client2ConnectionString))
//            {
//                using (var sqlCmd = new MySqlCommand("SELECT count(*) FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE " +
//                      "WHERE TABLE_SCHEMA = @schema AND REFERENCED_TABLE_NAME is not null", sqlConnection))
//                {
//                    sqlCmd.Parameters.AddWithValue("@schema", fixture.client2DbName);

//                    sqlConnection.Open();
//                    fkeysCount = Convert.ToInt32(sqlCmd.ExecuteScalar());
//                    sqlConnection.Close();
//                }
//            }
//            Assert.Equal(2, fkeysCount);

//        }


//    }
//}
