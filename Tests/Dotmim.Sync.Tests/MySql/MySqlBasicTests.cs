
using Dotmim.Sync.Test.Misc;
using Dotmim.Sync.Tests.Core;
using Dotmim.Sync.Tests.Misc;
using Dotmim.Sync.Tests.Models;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Dotmim.Sync.Tests.MySql
{
    /// <summary>
    /// this is the class which implements concret fixture with SqlSyncProviderFixture 
    /// and will call all the base tests
    /// </summary>
    [Collection("MySql")]
    [TestCaseOrderer("Dotmim.Sync.Tests.Misc.PriorityOrderer", "Dotmim.Sync.Tests")]
    public class MySqlBasicTests : BasicTestsBase, IClassFixture<MySqlFixture>
    {

        /// <summary>
        /// because schemas are not replicate on SQL Server, from MySQL
        /// Override get context to set "don't use schema on Sql childs "
        /// </summary>
        protected override AdventureWorksContext GetClientDbContext(ProviderRun providerRun)
        {
            return new AdventureWorksContext(providerRun, false);
        }


        public MySqlBasicTests(MySqlFixture fixture) : base(fixture)
        {
            // Mix of dbo, SalesLT and no schema provided in string array
            this.fixture.Tables = new string[]
            {
                "productcategory",
                "productmodel",
                "product",
                "customer",
                "address",
                "customeraddress",
                "salesorderheader",
                "salesorderdetail",
                "sql",
                "posts",
                "tags",
                "posttag"
            };
            //this.fixture.Tables = new string[]
            //{
            //    "productmodel",
            //};
        }

        [Fact, TestPriority(0)]
        public override async Task Initialize()
        {
            await base.Initialize();
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
        public override async Task Insert_One_Table_From_Server()
        {
            await base.Insert_One_Table_From_Server();
        }

        [Fact, TestPriority(4)]
        public override async Task Insert_One_Table_From_Client()
        {
            await base.Insert_One_Table_From_Client();
        }

        [Fact, TestPriority(5)]
        public override async Task Insert_Multiple_Tables_From_Server()
        {
            await base.Insert_Multiple_Tables_From_Server();
        }

        [Fact, TestPriority(6)]
        public override async Task Insert_Multiple_Tables_From_Client()
        {
            await base.Insert_Multiple_Tables_From_Client();
        }

        [Fact, TestPriority(7)]
        public override async Task Update_One_Table_From_Server()
        {
            await base.Update_One_Table_From_Server();
        }

        [Fact, TestPriority(8)]
        public override async Task Update_One_Table_From_Client()
        {
            await base.Update_One_Table_From_Client();
        }

        [Fact, TestPriority(9)]
        public override async Task Conflict_Insert_Insert_Server_Should_Wins()
        {
            await base.Conflict_Insert_Insert_Server_Should_Wins();
        }

        [Fact, TestPriority(10)]
        public override async Task Conflict_Insert_Insert_Client_Should_Wins_Coz_Configuration()
        {
            await base.Conflict_Insert_Insert_Client_Should_Wins_Coz_Configuration();
        }

        [Fact, TestPriority(11)]
        public override async Task Conflict_Insert_Insert_Client_Should_Wins_Coz_Handler_Raised()
        {
            await base.Conflict_Insert_Insert_Client_Should_Wins_Coz_Handler_Raised();
        }

        [Fact, TestPriority(12)]
        public override Task Conflict_Update_Update_Client_Should_Wins_Coz_Configuration()
        {
            return base.Conflict_Update_Update_Client_Should_Wins_Coz_Configuration();
        }

        [Fact, TestPriority(13)]
        public override Task Conflict_Update_Update_Client_Should_Wins_Coz_Handler_Raised()
        {
            return base.Conflict_Update_Update_Client_Should_Wins_Coz_Handler_Raised();
        }

        [Fact, TestPriority(14)]
        public override Task Conflict_Update_Update_Resolve_By_Merge()
        {
            return base.Conflict_Update_Update_Resolve_By_Merge();
        }

        [Fact, TestPriority(15)]
        public override Task Conflict_Update_Update_Server_Should_Wins()
        {
            return base.Conflict_Update_Update_Server_Should_Wins();
        }

        [Fact, TestPriority(16)]
        public override Task Delete_One_Table_From_Server()
        {
            return base.Delete_One_Table_From_Server();
        }

        [Fact, TestPriority(17)]
        public override Task Delete_Multiple_Tables_From_Server()
        {
            return base.Delete_Multiple_Tables_From_Server();
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
        public override Task Provision()
        {
            return base.Provision();
        }
    }
}
