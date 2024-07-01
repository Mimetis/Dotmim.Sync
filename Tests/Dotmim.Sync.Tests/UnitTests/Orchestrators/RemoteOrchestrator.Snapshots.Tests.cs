using Dotmim.Sync.Enumerations;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Tests.Core;
using Dotmim.Sync.Tests.Misc;
using Dotmim.Sync.Tests.Models;
using System;
using System.Data;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Xunit;

namespace Dotmim.Sync.Tests.UnitTests
{
    public partial class RemoteOrchestratorTests : IDisposable
    {
 

        [Fact]
        public async Task RemoteOrchestrator_CreateSnapshot_CheckInterceptors()
        {
            var scopeName = "scopesnap1";
            var onSnapshotCreating = false;
            var onSnapshotCreated = false;

            // snapshot directory
            var snapshotDirctoryName = HelperDatabase.GetRandomName();
            var snapshotDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDirectory(), snapshotDirctoryName);

            var options = new SyncOptions
            {
                SnapshotsDirectory = snapshotDirectory,
                BatchSize = 200
            };

            var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options);

            remoteOrchestrator.OnSnapshotCreating(args =>
            {
                Assert.IsType<SnapshotCreatingArgs>(args);
                Assert.Equal(SyncStage.SnapshotCreating, args.Context.SyncStage);
                Assert.Equal(scopeName, args.Context.ScopeName);
                Assert.NotNull(args.Connection);
                Assert.Null(args.Transaction);
                Assert.Equal(ConnectionState.Closed, args.Connection.State);
                Assert.NotNull(args.Schema);
                Assert.Equal(snapshotDirectory, args.SnapshotDirectory);
                Assert.NotEqual(0, args.Timestamp);

                onSnapshotCreating = true;
            });
            remoteOrchestrator.OnSnapshotCreated(args =>
            {
                Assert.IsType<SnapshotCreatedArgs>(args);
                Assert.Equal(SyncStage.ChangesSelecting, args.Context.SyncStage);
                Assert.Equal(scopeName, args.Context.ScopeName);
                Assert.NotNull(args.Connection);
                Assert.Null(args.Transaction);
                Assert.NotNull(args.BatchInfo);

                var finalDirectoryFullName = Path.Combine(snapshotDirectory, scopeName);

                Assert.Equal(finalDirectoryFullName, args.BatchInfo.DirectoryRoot);
                Assert.Equal("ALL", args.BatchInfo.DirectoryName);
                Assert.NotEmpty(args.BatchInfo.BatchPartsInfo);
                Assert.Equal(16, args.BatchInfo.BatchPartsInfo.Count);
                // Get last batch 
                var lastBatch = args.BatchInfo.BatchPartsInfo.First(bi => bi.Index == 15);

                Assert.True(lastBatch.IsLastBatch);

                onSnapshotCreated = true;
            });

            var bi = await remoteOrchestrator.CreateSnapshotAsync(scopeName, setup);

            Assert.True(onSnapshotCreating);
            Assert.True(onSnapshotCreated);


            var dbNameCli = HelperDatabase.GetRandomName("tcp_lo_cli");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbNameCli, true);

            var csClient = HelperDatabase.GetConnectionString(ProviderType.Sql, dbNameCli);
            var clientProvider = new SqlSyncProvider(csClient);

            // Make a first sync to be sure everything is in place
            var agent = new SyncAgent(clientProvider, serverProvider, options);

            var onSnapshotApplying = false;
            var onSnapshotApplied = false;

            agent.LocalOrchestrator.OnSnapshotApplying(saa =>
            {
                onSnapshotApplying = true;
            });

            agent.LocalOrchestrator.OnSnapshotApplied(saa =>
            {
                onSnapshotApplied = true;
            });

            // Making a first sync, will initialize everything we need
            await agent.SynchronizeAsync(scopeName, setup);

            Assert.True(onSnapshotApplying);
            Assert.True(onSnapshotApplied);
        }


        [Fact]
        public async Task RemoteOrchestrator_CreateSnapshot_CheckBatchInfo()
        {
            var rowsCount = serverProvider.GetDatabaseRowsCount();
            var scopeName = "scopesnap2";

            // snapshot directory
            var snapshotDirctoryName = HelperDatabase.GetRandomName();
            var snapshotDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDirectory(), snapshotDirctoryName);

            var options = new SyncOptions
            {
                SnapshotsDirectory = snapshotDirectory,
                BatchSize = 200
            };

            var orchestrator = new RemoteOrchestrator(serverProvider, options);

            var bi = await orchestrator.CreateSnapshotAsync(scopeName, setup);

            var finalDirectoryFullName = Path.Combine(snapshotDirectory, scopeName);

            Assert.NotNull(bi);
            Assert.Equal(finalDirectoryFullName, bi.DirectoryRoot);
            Assert.Equal("ALL", bi.DirectoryName);
            Assert.NotEmpty(bi.BatchPartsInfo);
            Assert.Equal(16, bi.BatchPartsInfo.Count);

            // Get last batch 
            var lastBatch = bi.BatchPartsInfo.First(bi => bi.Index == 15);
                
            Assert.True(lastBatch.IsLastBatch);
            Assert.Equal(rowsCount, bi.RowsCount);

            // Check summary.json exists.
            var summaryFile = Path.Combine(bi.GetDirectoryFullPath(), "summary.json");
            await using var summaryStream = File.OpenRead(summaryFile);
            var summaryObject = JsonDocument.Parse(summaryStream).RootElement;

            Assert.NotEqual(summaryObject.ValueKind, JsonValueKind.Null);
            string summaryDirname = summaryObject.GetProperty("dirname").GetString();
            Assert.NotNull(summaryDirname);
            Assert.Equal("ALL", summaryDirname);

            string summaryDir = summaryObject.GetProperty("dir").GetString();
            Assert.NotNull(summaryDir);
            Assert.Equal(finalDirectoryFullName, summaryDir);

            var parts = summaryObject.GetProperty("parts");
            var partsEnumerator = parts.EnumerateArray();
            Assert.NotEmpty(partsEnumerator);
            Assert.Equal(16, partsEnumerator.Count());

            var firstPart = parts[0];
            Assert.NotNull(firstPart.GetProperty("file").GetString());
            Assert.Equal(0, firstPart.GetProperty("index").GetInt32());
            Assert.False(firstPart.GetProperty("last").GetBoolean());
        }

        [Fact]
        public async Task RemoteOrchestrator_CreateSnapshot_WithParameters_CheckBatchInfo()
        {
            // snapshot directory
            var snapshotDirctoryName = HelperDatabase.GetRandomName();
            var snapshotDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDirectory(), snapshotDirctoryName);

            var options = new SyncOptions
            {
                SnapshotsDirectory = snapshotDirectory,
                BatchSize = 200
            };
            setup.Filters.Add("Customer", "CompanyName");

            var addressCustomerFilter = new SetupFilter("CustomerAddress");
            addressCustomerFilter.AddParameter("CompanyName", "Customer");
            addressCustomerFilter.AddJoin(Join.Left, "Customer").On("CustomerAddress", "CustomerId", "Customer", "CustomerId");
            addressCustomerFilter.AddWhere("CompanyName", "Customer", "CompanyName");
            setup.Filters.Add(addressCustomerFilter);

            var addressFilter = new SetupFilter("Address");
            addressFilter.AddParameter("CompanyName", "Customer");
            addressFilter.AddJoin(Join.Left, "CustomerAddress").On("CustomerAddress", "AddressId", "Address", "AddressId");
            addressFilter.AddJoin(Join.Left, "Customer").On("CustomerAddress", "CustomerId", "Customer", "CustomerId");
            addressFilter.AddWhere("CompanyName", "Customer", "CompanyName");
            setup.Filters.Add(addressFilter);

            var orderHeaderFilter = new SetupFilter("SalesOrderHeader");
            orderHeaderFilter.AddParameter("CompanyName", "Customer");
            orderHeaderFilter.AddJoin(Join.Left, "CustomerAddress").On("CustomerAddress", "CustomerId", "SalesOrderHeader", "CustomerId");
            orderHeaderFilter.AddJoin(Join.Left, "Customer").On("CustomerAddress", "CustomerId", "Customer", "CustomerId");
            orderHeaderFilter.AddWhere("CompanyName", "Customer", "CompanyName");
            setup.Filters.Add(orderHeaderFilter);

            var orderDetailsFilter = new SetupFilter("SalesOrderDetail");
            orderDetailsFilter.AddParameter("CompanyName", "Customer");
            orderDetailsFilter.AddJoin(Join.Left, "SalesOrderHeader").On("SalesOrderDetail", "SalesOrderID", "SalesOrderHeader", "SalesOrderID");
            orderDetailsFilter.AddJoin(Join.Left, "CustomerAddress").On("CustomerAddress", "CustomerId", "SalesOrderHeader", "CustomerId");
            orderDetailsFilter.AddJoin(Join.Left, "Customer").On("CustomerAddress", "CustomerId", "Customer", "CustomerId");
            orderDetailsFilter.AddWhere("CompanyName", "Customer", "CompanyName");
            setup.Filters.Add(orderDetailsFilter);


            var orchestrator = new RemoteOrchestrator(serverProvider, options);

            SyncParameters parameters = new SyncParameters();
            var p1 = new SyncParameter("CompanyName", "A Bike Store");
            parameters.Add(p1);

            var bi = await orchestrator.CreateSnapshotAsync(setup, parameters);

            var finalDirectoryFullName = Path.Combine(snapshotDirectory, SyncOptions.DefaultScopeName);

            Assert.NotNull(bi);
            Assert.Equal(finalDirectoryFullName, bi.DirectoryRoot);
            Assert.Equal("CompanyName_ABikeStore", bi.DirectoryName);
            Assert.NotEmpty(bi.BatchPartsInfo);
            Assert.Equal(16, bi.BatchPartsInfo.Count);

            // Get last batch 
            var lastBatch = bi.BatchPartsInfo.First(bi => bi.Index == 15);

            Assert.True(lastBatch.IsLastBatch);

            // Check summary.json exists.
            var summaryFile = Path.Combine(bi.GetDirectoryFullPath(), "summary.json");
            await using var summaryStream = File.OpenRead(summaryFile);
            var summaryObject = JsonDocument.Parse(summaryStream).RootElement;

            Assert.NotEqual(summaryObject.ValueKind, JsonValueKind.Null);
            string summaryDirname = summaryObject.GetProperty("dirname").GetString();
            Assert.NotNull(summaryDirname);
            Assert.Equal("CompanyName_ABikeStore", summaryDirname);

            string summaryDir = summaryObject.GetProperty("dir").GetString();
            Assert.NotNull(summaryDir);
            Assert.Equal(finalDirectoryFullName, summaryDir);

            var parts = summaryObject.GetProperty("parts");
            var partsEnumerator = parts.EnumerateArray();
            Assert.NotEmpty(partsEnumerator);
            Assert.Equal(16, partsEnumerator.Count());

            var firstPart = parts[0];
            Assert.NotNull(firstPart.GetProperty("file").GetString());
            Assert.Equal(0, firstPart.GetProperty("index").GetInt32());
            Assert.False(firstPart.GetProperty("last").GetBoolean());
        }

        [Fact]
        public async Task RemoteOrchestrator_CreateSnapshot_ShouldFail_If_MissingMandatoriesOptions()
        {
            var scopeName = "scopesnap";

            // snapshot directory
            var snapshotDirctoryName = HelperDatabase.GetRandomName();
            var snapshotDirectory = Path.Combine(Environment.CurrentDirectory, snapshotDirctoryName);

            var orchestrator = new RemoteOrchestrator(serverProvider, options);
            var se = await Assert.ThrowsAsync<SyncException>(() => orchestrator.CreateSnapshotAsync(scopeName, setup));

            Assert.Equal(SyncStage.SnapshotCreating, se.SyncStage);
            Assert.Equal("SnapshotMissingMandatariesOptionsException", se.TypeName);

            options = new SyncOptions { BatchSize = 2000 };
            orchestrator = new RemoteOrchestrator(serverProvider, options);
            se = await Assert.ThrowsAsync<SyncException>(() => orchestrator.CreateSnapshotAsync(scopeName, setup));

            Assert.Equal(SyncStage.SnapshotCreating, se.SyncStage);
            Assert.Equal("SnapshotMissingMandatariesOptionsException", se.TypeName);

            options = new SyncOptions { };
            orchestrator = new RemoteOrchestrator(serverProvider, options);
            se = await Assert.ThrowsAsync<SyncException>(() => orchestrator.CreateSnapshotAsync(scopeName, setup));

            Assert.Equal(SyncStage.SnapshotCreating, se.SyncStage);
            Assert.Equal("SnapshotMissingMandatariesOptionsException", se.TypeName);
        }
    }
}
