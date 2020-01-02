using Dotmim.Sync.Tests.Core;
using Dotmim.Sync.Tests.Misc;
using System;
using System.Threading.Tasks;
using Xunit;

namespace Dotmim.Sync.Tests.SqlServer
{
    /// <summary>
    /// this is the class which implements concret fixture with SqlSyncProviderFixture 
    /// and will call all the base tests
    /// </summary>
    [TestCaseOrderer("Dotmim.Sync.Tests.Misc.PriorityOrderer", "Dotmim.Sync.Tests")]
    [Collection("SqlServer")]
    public class SqlServerBasicTests : BasicTestsBase, IClassFixture<SqlServerFixture>
    {
        static SqlServerBasicTests()
        {
            Configure = providerFixture =>
            {
               // Set tables to be used for your provider
               var sqlTables = new string[]
               {
                    "SalesLT.ProductCategory", "SalesLT.ProductModel", "SalesLT.Product", "Employee", "Customer", "Address", "CustomerAddress", "EmployeeAddress",
                    "SalesLT.SalesOrderHeader", "SalesLT.SalesOrderDetail", "dbo.Sql", "Posts", "Tags", "PostTag",
                    "PricesList", "PricesListCategory", "PricesListDetail"
               };

                //// Set tables to be used for your provider
                //var sqlTables = new string[]
                //{
                //   "SalesLT.Product"
                //};

                // 1) Add database name
                providerFixture.AddDatabaseName("SqlAdventureWorks");
                // 2) Add tables
                providerFixture.AddTables(sqlTables, 2109);

                if (!Setup.IsOnAzureDev)
                {
                    providerFixture.AddRun(NetworkType.Http,  ProviderType.MySql);
                    providerFixture.DeleteAllDatabasesOnDispose = false;
                }
                else
                {
                    providerFixture.AddRun(NetworkType.Tcp, ProviderType.Sql | ProviderType.MySql | ProviderType.Sqlite);
                    providerFixture.AddRun(NetworkType.Http, ProviderType.MySql | ProviderType.Sqlite | ProviderType.Sql);
                    providerFixture.DeleteAllDatabasesOnDispose = true;
                }
            };

        }

        public SqlServerBasicTests(SqlServerFixture fixture) : base(fixture)
        {

        }

        [Fact, TestPriority(1)]
        public override Task CheckHealthDatabase()
        {
            return base.CheckHealthDatabase();
        }

        [Fact, TestPriority(2)]
        public override Task Initialize()
        {
            return base.Initialize();
        }

        //[Fact, TestPriority(1)]
        //public override Task Bad_Server_Connection_Should_Raise_Error()
        //{
        //    return base.Bad_Server_Connection_Should_Raise_Error();
        //}

        //[Fact, TestPriority(2)]
        //public override Task Bad_Client_Connection_Should_Raise_Error()
        //{
        //    return base.Bad_Client_Connection_Should_Raise_Error();
        //}

        [Fact, TestPriority(3)]
        public override Task Insert_One_Table_From_Server()
        {
            return base.Insert_One_Table_From_Server();
        }

        [Fact, TestPriority(4)]
        public override Task Insert_One_Table_From_Client()
        {
            return base.Insert_One_Table_From_Client();
        }

        [Fact, TestPriority(5)]
        public override Task Insert_Multiple_Tables_From_Server()
        {
            return base.Insert_Multiple_Tables_From_Server();
        }

        [Fact, TestPriority(6)]
        public override Task Insert_Multiple_Tables_From_Client()
        {
            return base.Insert_Multiple_Tables_From_Client();
        }

        [Fact, TestPriority(7)]
        public override Task Update_One_Table_From_Server()
        {
            return base.Update_One_Table_From_Server();
        }

        [Fact, TestPriority(8)]
        public override Task Update_One_Table_From_Client()
        {
            return base.Update_One_Table_From_Client();
        }

        [Fact, TestPriority(9)]
        public override Task Conflict_Insert_Insert_Server_Should_Wins()
        {
            return base.Conflict_Insert_Insert_Server_Should_Wins();
        }

        [Fact, TestPriority(10)]
        public override Task Delete_From_Server()
        {
            return base.Delete_From_Server();
        }

        [Fact, TestPriority(12)]
        public override Task Conflict_Insert_Insert_Client_Should_Wins_Coz_Configuration()
        {
            return base.Conflict_Insert_Insert_Client_Should_Wins_Coz_Configuration();
        }

        [Fact, TestPriority(13)]
        public override Task Conflict_Insert_Insert_Client_Should_Wins_Coz_Handler_Raised()
        {
            return base.Conflict_Insert_Insert_Client_Should_Wins_Coz_Handler_Raised();
        }

        [Fact, TestPriority(14)]
        public override Task Conflict_Update_Update_Client_Should_Wins_Coz_Configuration()
        {
            return base.Conflict_Update_Update_Client_Should_Wins_Coz_Configuration();
        }

        [Fact, TestPriority(15)]
        public override Task Conflict_Update_Update_Client_Should_Wins_Coz_Handler_Raised()
        {
            return base.Conflict_Update_Update_Client_Should_Wins_Coz_Handler_Raised();
        }

        [Fact, TestPriority(16)]
        public override Task Conflict_Update_Update_Resolve_By_Merge()
        {
            return base.Conflict_Update_Update_Resolve_By_Merge();
        }

        [Fact, TestPriority(17)]
        public override Task Conflict_Update_Update_Server_Should_Wins()
        {
            return base.Conflict_Update_Update_Server_Should_Wins();
        }

        [Fact, TestPriority(18)]
        public override Task Insert_Then_Delete_From_Server_Then_Sync()
        {
            return base.Insert_Then_Delete_From_Server_Then_Sync();
        }

        [Fact, TestPriority(19)]
        public override Task Insert_Then_Update_From_Server_Then_Sync()
        {
            return base.Insert_Then_Update_From_Server_Then_Sync();
        }

        [Fact, TestPriority(20)]
        public override Task Use_Existing_Client_Database_Provision_Deprosivion()
        {
            return base.Use_Existing_Client_Database_Provision_Deprosivion();
        }

        [Fact, TestPriority(21)]
        public override Task Check_Composite_ForeignKey_Existence()
        {
            return base.Check_Composite_ForeignKey_Existence();
        }

        [Fact, TestPriority(22)]
        public override Task Insert_New_Table_Then_Update_Existing_Table_From_Client()
        {
            return base.Insert_New_Table_Then_Update_Existing_Table_From_Client();
        }

        [Fact, TestPriority(23)]
        public override Task Insert_Record_Then_Insert_During_GetChanges()
        {
            return base.Insert_Record_Then_Insert_During_GetChanges();
        }
        [Fact, TestPriority(24)]
        public override Task Check_Interceptors()
        {
            return base.Check_Interceptors();
        }

        [Fact, TestPriority(25)]
        public override Task Force_Failing_Constraints()
        {
            return base.Force_Failing_Constraints();
        }
        [Fact, TestPriority(26)]
        public override Task Insert_Then_Update_Server_Then_Sync()
        {
            return base.Insert_Then_Update_Server_Then_Sync();
        }
        [Fact, TestPriority(27)]
        public override Task Insert_Thousand_Client()
        {
            return base.Insert_Thousand_Client();
        }

        [Fact, TestPriority(28)]
        public override Task Conflict_Insert_Delete_Insert_On_Server_Should_Wins()
        {
            return base.Conflict_Insert_Delete_Insert_On_Server_Should_Wins();
        }

        [Fact, TestPriority(29)]
        public override Task Delete_Client_Sync_Insert_Server_Sync_Client_Should_Have_Insert()
        {
            return base.Delete_Client_Sync_Insert_Server_Sync_Client_Should_Have_Insert();
        }

        [Fact, TestPriority(30)]
        public override Task Reinitialize_Client_Database()
        {
            return base.Reinitialize_Client_Database();
        }

        [Fact, TestPriority(31)]
        public override Task ReinitializeWithUpload_Client_Database()
        {
            return base.ReinitializeWithUpload_Client_Database();
        }
    }
}
