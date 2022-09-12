using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Sqlite;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.SqlServer.Manager;
using Dotmim.Sync.Tests.Core;
using Dotmim.Sync.Tests.Misc;
using Dotmim.Sync.Tests.Models;
using Dotmim.Sync.Web.Client;
using Dotmim.Sync.Web.Server;
using Microsoft.AspNetCore.Http;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
#if NET5_0 || NET6_0 || NETCOREAPP3_1
using MySqlConnector;
#elif NETCOREAPP2_1
using MySql.Data.MySqlClient;
#endif

using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Dotmim.Sync.Tests
{
    //[TestCaseOrderer("Dotmim.Sync.Tests.Misc.PriorityOrderer", "Dotmim.Sync.Tests")]
    public abstract class TcpConflictsTests : IClassFixture<HelperProvider>, IDisposable
    {
        private Stopwatch stopwatch;

        /// <summary>
        /// Gets the sync tables involved in the tests
        /// </summary>
        public abstract string[] Tables { get; }

        /// <summary>
        /// Gets the clients type we want to tests
        /// </summary>
        public abstract List<ProviderType> ClientsType { get; }

        /// <summary>
        /// Gets the server type we want to test
        /// </summary>
        public abstract ProviderType ServerType { get; }

        /// <summary>
        /// Get the server rows count
        /// </summary>
        public abstract int GetServerDatabaseRowsCount((string DatabaseName, ProviderType ProviderType, CoreProvider Provider) t);

        /// <summary>
        /// Create a provider
        /// </summary>
        public abstract CoreProvider CreateProvider(ProviderType providerType, string dbName);

        /// <summary>
        /// Create database, seed it, with or without schema
        /// </summary>
        /// <param name="t"></param>
        /// <param name="useSeeding"></param>
        /// <param name="useFallbackSchema"></param>
        public abstract Task EnsureDatabaseSchemaAndSeedAsync((string DatabaseName,
            ProviderType ProviderType, CoreProvider Provider) t, bool useSeeding = false, bool useFallbackSchema = false);

        /// <summary>
        /// Create an empty database
        /// </summary>
        public abstract Task CreateDatabaseAsync(ProviderType providerType, string dbName, bool recreateDb = true);

        // abstract fixture used to run the tests
        protected readonly HelperProvider fixture;

        // Current test running
        private ITest test;

        /// <summary>
        /// Gets the remote orchestrator and its database name
        /// </summary>
        public (string DatabaseName, ProviderType ProviderType, CoreProvider Provider) Server { get; private set; }

        /// <summary>
        /// Gets the dictionary of all local orchestrators with database name as key
        /// </summary>
        public List<(string DatabaseName, ProviderType ProviderType, CoreProvider Provider)> Clients { get; set; }

        /// <summary>
        /// Gets a bool indicating if we should generate the schema for tables
        /// </summary>
        public bool UseFallbackSchema => ServerType == ProviderType.Sql;

        public ITestOutputHelper Output { get; }

        /// <summary>
        /// For each test, Create a server database and some clients databases, depending on ProviderType provided in concrete class
        /// </summary>
        public TcpConflictsTests(HelperProvider fixture, ITestOutputHelper output)
        {
            // Getting the test running
            this.Output = output;
            var type = output.GetType();
            var testMember = type.GetField("test", BindingFlags.Instance | BindingFlags.NonPublic);
            this.test = (ITest)testMember.GetValue(output);
            this.stopwatch = Stopwatch.StartNew();

            this.fixture = fixture;

            // Since we are creating a lot of databases
            // each database will have its own pool
            // Droping database will not clear the pool associated
            // So clear the pools on every start of a new test
            SqlConnection.ClearAllPools();
            MySqlConnection.ClearAllPools();


            // get the server provider (and db created) without seed
            var serverDatabaseName = HelperDatabase.GetRandomName("tcp_sv_");

            // create remote orchestrator
            var serverProvider = this.CreateProvider(this.ServerType, serverDatabaseName);

            this.Server = (serverDatabaseName, this.ServerType, serverProvider);

            // Get all clients providers
            Clients = new List<(string DatabaseName, ProviderType ProviderType, CoreProvider provider)>(this.ClientsType.Count);

            // Generate Client database
            foreach (var clientType in this.ClientsType)
            {
                var dbCliName = HelperDatabase.GetRandomName("tcp_cli_");
                var localProvider = this.CreateProvider(clientType, dbCliName);
                this.Clients.Add((dbCliName, clientType, localProvider));
            }
        }

        /// <summary>
        /// Drop all databases used for the tests
        /// </summary>
        public void Dispose()
        {
            try
            {
                HelperDatabase.DropDatabase(this.ServerType, Server.DatabaseName);
                foreach (var client in Clients)
                    HelperDatabase.DropDatabase(client.ProviderType, client.DatabaseName);
            }
            catch (Exception) { }

            this.stopwatch.Stop();

            var str = $"{test.TestCase.DisplayName} : {this.stopwatch.Elapsed.Minutes}:{this.stopwatch.Elapsed.Seconds}.{this.stopwatch.Elapsed.Milliseconds}";
            Console.WriteLine(str);
            Debug.WriteLine(str);

        }

        private async Task CheckProductCategoryRows((string DatabaseName, ProviderType ProviderType, CoreProvider Provider) client, string nameShouldStartWith = null)
        {
            // check rows count on server and on each client
            using var ctx = new AdventureWorksContext(this.Server);
            // get all product categories
            var serverPC = await ctx.ProductCategory.AsNoTracking().ToListAsync();

            using var cliCtx = new AdventureWorksContext(client, this.UseFallbackSchema);
            // get all product categories
            var clientPC = await cliCtx.ProductCategory.AsNoTracking().ToListAsync();

            // check row count
            Assert.Equal(serverPC.Count, clientPC.Count);

            foreach (var cpc in clientPC)
            {
                var spc = serverPC.First(pc => pc.ProductCategoryId == cpc.ProductCategoryId);

                // check column value
                Assert.Equal(spc.ProductCategoryId, cpc.ProductCategoryId);
                Assert.Equal(spc.Name, cpc.Name);

                if (!string.IsNullOrEmpty(nameShouldStartWith))
                    Assert.StartsWith(nameShouldStartWith, cpc.Name);

            }
        }


        // ------------------------------------------------------------------------
        // Generate Unique Key failure
        // ------------------------------------------------------------------------
        private async Task Generate_Client_UniqueKeyError(SqlSyncProvider sqlSyncProvider)
        {
            var sqlConnection = new SqlConnection(sqlSyncProvider.ConnectionString);

            var subcatrandom = Path.GetRandomFileName();
            var categoryName = string.Concat("A_", string.Concat(subcatrandom.Where(c => char.IsLetter(c))).ToUpperInvariant());

            var cat1 = string.Concat("Z_", string.Concat(Path.GetRandomFileName().Where(c => char.IsLetter(c))).ToUpperInvariant());
            cat1 = cat1.Substring(0, Math.Min(cat1.Length, 12));

            var cat2 = string.Concat("Z_", string.Concat(Path.GetRandomFileName().Where(c => char.IsLetter(c))).ToUpperInvariant());
            cat2 = cat2.Substring(0, Math.Min(cat2.Length, 12));

            var pcName = Server.ProviderType == ProviderType.Sql ? "[SalesLT].[ProductCategory]" : "[ProductCategory]";

            var commandText = @$"Begin Tran
                            ALTER TABLE {pcName} NOCHECK CONSTRAINT ALL
                            INSERT {pcName} ([ProductCategoryID], [Name]) VALUES (N'{cat1}', N'{categoryName} Category');
                            INSERT {pcName} ([ProductCategoryID], [Name]) VALUES (N'{cat2}', N'{categoryName} Category');
                            ALTER TABLE {pcName} CHECK CONSTRAINT ALL
                            Commit Tran";

            var command = sqlConnection.CreateCommand();
            command.CommandText = commandText;
            sqlConnection.Open();
            await command.ExecuteNonQueryAsync();
            sqlConnection.Close();

        }

        private async Task Resolve_Client_UniqueKeyError(SqlSyncProvider sqlSyncProvider)
        {
            var sqlConnection = new SqlConnection(sqlSyncProvider.ConnectionString);

            var subcatrandom = Path.GetRandomFileName();
            var categoryName = string.Concat("A_", string.Concat(subcatrandom.Where(c => char.IsLetter(c))).ToUpperInvariant());

            var pcName = Server.ProviderType == ProviderType.Sql ? "[SalesLT].[ProductCategory]" : "[ProductCategory]";

            var commandText = @$"UPDATE {pcName} SET [Name] = '{categoryName}' + [ProductCategoryID];";

            var command = sqlConnection.CreateCommand();
            command.CommandText = commandText;
            sqlConnection.Open();
            await command.ExecuteNonQueryAsync();
            sqlConnection.Close();

        }


        private async Task Update_Client_UniqueKeyError(SqlSyncProvider sqlSyncProvider)
        {
            var sqlConnection = new SqlConnection(sqlSyncProvider.ConnectionString);

            var subcatrandom = Path.GetRandomFileName();
            var categoryName = string.Concat("A_", string.Concat(subcatrandom.Where(c => char.IsLetter(c))).ToUpperInvariant());

            var pcName = Server.ProviderType == ProviderType.Sql ? "[SalesLT].[ProductCategory]" : "[ProductCategory]";

            var commandText = @$"UPDATE {pcName} SET [Name] = [Name];";

            var command = sqlConnection.CreateCommand();
            command.CommandText = commandText;
            sqlConnection.Open();
            await command.ExecuteNonQueryAsync();
            sqlConnection.Close();

        }

        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Error_UniqueKey_OnSameTable_RaiseError(SyncOptions options)
        {
            // Only works for SQL

            // create a server schema without seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            foreach (var client in Clients)
            {
                if (client.ProviderType != ProviderType.Sql)
                    continue;

                // Get a random directory to be sure we are not conflicting with another test
                var directoryName = HelperDatabase.GetRandomName();
                options.BatchDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDirectory(), directoryName);

                // create empty client databases
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

                var agent = new SyncAgent(client.Provider, Server.Provider, options);
                await agent.SynchronizeAsync(Tables);

                await Generate_Client_UniqueKeyError(client.Provider as SqlSyncProvider);

                var exc = await Assert.ThrowsAsync<SyncException>(() => agent.SynchronizeAsync(Tables));

                Assert.NotNull(exc);

                var batchInfos = await agent.LocalOrchestrator.LoadBatchInfosAsync();

                Assert.Empty(batchInfos);

                exc = await Assert.ThrowsAsync<SyncException>(() => agent.SynchronizeAsync(Tables));

                Assert.Empty(batchInfos);
            }
        }

        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Error_UniqueKey_OnSameTable_ContinueOnError(SyncOptions options)
        {
            // Only works for SQL

            // create a server schema without seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            foreach (var client in Clients)
            {
                if (client.ProviderType != ProviderType.Sql)
                    continue;

                // Get a random directory to be sure we are not conflicting with another test
                var directoryName = HelperDatabase.GetRandomName();
                options.BatchDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDirectory(), directoryName);

                // create empty client databases
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

                var agent = new SyncAgent(client.Provider, Server.Provider, options);
                await agent.SynchronizeAsync(Tables);

                await Generate_Client_UniqueKeyError(client.Provider as SqlSyncProvider);

                agent.RemoteOrchestrator.OnApplyChangesErrorOccured(args =>
                {
                    // Continue On Error
                    args.Resolution = ErrorResolution.ContinueOnError;
                    Assert.NotNull(args.Exception);
                    Assert.NotNull(args.ErrorRow);
                    Assert.NotNull(args.SchemaTable);
                    Assert.Equal(SyncRowState.Modified, args.ApplyType);
                });

                var s = await agent.SynchronizeAsync(Tables);

                Assert.Equal(0, s.TotalChangesDownloadedFromServer);
                Assert.Equal(2, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalChangesAppliedOnServer);
                Assert.Equal(0, s.TotalChangesAppliedOnClient);
                Assert.Equal(0, s.TotalChangesFailedToApplyOnClient);
                Assert.Equal(1, s.TotalChangesFailedToApplyOnServer);
                Assert.Equal(0, s.TotalResolvedConflicts);

                var batchInfos = await agent.LocalOrchestrator.LoadBatchInfosAsync();

                Assert.NotNull(batchInfos);
                Assert.Single(batchInfos);
                Assert.Single(batchInfos[0].BatchPartsInfo);
                Assert.Contains("ERRORS", batchInfos[0].BatchPartsInfo[0].FileName);
                Assert.Equal("ProductCategory", batchInfos[0].BatchPartsInfo[0].TableName);

                var batchInfo = batchInfos[0];

                var syncTables = agent.LocalOrchestrator.LoadTablesFromBatchInfoAsync(batchInfo);

                await foreach (var syncTable in syncTables)
                {
                    Assert.Equal("ProductCategory", syncTable.TableName);
                    Assert.True(syncTable.HasRows);

                    Assert.Equal(SyncRowState.ApplyModifiedFailed, syncTable.Rows[0].RowState);
                }

                s = await agent.SynchronizeAsync(Tables);

                // Download 2 rows
                // But applied only 1
                // The other one is a failed inserted row
                Assert.Equal(0, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalChangesAppliedOnClient);
                Assert.Equal(0, s.TotalChangesAppliedOnServer);
                Assert.Equal(0, s.TotalChangesFailedToApplyOnClient);
                Assert.Equal(0, s.TotalChangesFailedToApplyOnServer);
                Assert.Equal(0, s.TotalResolvedConflicts);

                batchInfos = await agent.LocalOrchestrator.LoadBatchInfosAsync();

                Assert.NotNull(batchInfos);
                Assert.Single(batchInfos);
                Assert.Single(batchInfos[0].BatchPartsInfo);
                Assert.Contains("ERRORS", batchInfos[0].BatchPartsInfo[0].FileName);
                Assert.Equal("ProductCategory", batchInfos[0].BatchPartsInfo[0].TableName);

                batchInfo = batchInfos[0];

                syncTables = agent.LocalOrchestrator.LoadTablesFromBatchInfoAsync(batchInfo);

                await foreach (var syncTable in syncTables)
                {
                    Assert.Equal("ProductCategory", syncTable.TableName);
                    Assert.True(syncTable.HasRows);

                    Assert.Equal(SyncRowState.ApplyModifiedFailed, syncTable.Rows[0].RowState);
                }
            }
        }

        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Error_UniqueKey_OnSameTable_RetryOneMoreTimeAndThrowOnError(SyncOptions options)
        {
            // Only works for SQL

            // create a server schema without seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            foreach (var client in Clients)
            {
                if (client.ProviderType != ProviderType.Sql)
                    continue;

                // Get a random directory to be sure we are not conflicting with another test
                var directoryName = HelperDatabase.GetRandomName();
                options.BatchDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDirectory(), directoryName);

                // create empty client databases
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

                var agent = new SyncAgent(client.Provider, Server.Provider, options);
                await agent.SynchronizeAsync(Tables);

                await Generate_Client_UniqueKeyError(client.Provider as SqlSyncProvider);

                agent.RemoteOrchestrator.OnApplyChangesErrorOccured(args =>
                {
                    // Continue On Error
                    args.Resolution = ErrorResolution.RetryOneMoreTimeAndThrowOnError;
                    Assert.NotNull(args.Exception);
                    Assert.NotNull(args.ErrorRow);
                    Assert.NotNull(args.SchemaTable);
                    Assert.Equal(SyncRowState.Modified, args.ApplyType);
                });

                var exc = await Assert.ThrowsAsync<SyncException>(() => agent.SynchronizeAsync(Tables));
                Assert.NotNull(exc);
                var batchInfos = await agent.LocalOrchestrator.LoadBatchInfosAsync();
                Assert.Empty(batchInfos);

                exc = await Assert.ThrowsAsync<SyncException>(() => agent.SynchronizeAsync(Tables));
                Assert.NotNull(exc);
                batchInfos = await agent.LocalOrchestrator.LoadBatchInfosAsync();
                Assert.Empty(batchInfos);
            }
        }

        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Error_UniqueKey_OnSameTable_RetryOneMoreTimeAndContinueOnError(SyncOptions options)
        {
            // Only works for SQL

            // create a server schema without seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            foreach (var client in Clients)
            {
                if (client.ProviderType != ProviderType.Sql)
                    continue;

                // Get a random directory to be sure we are not conflicting with another test
                var directoryName = HelperDatabase.GetRandomName();
                options.BatchDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDirectory(), directoryName);

                // create empty client databases
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

                var agent = new SyncAgent(client.Provider, Server.Provider, options);
                await agent.SynchronizeAsync(Tables);

                await Generate_Client_UniqueKeyError(client.Provider as SqlSyncProvider);

                agent.RemoteOrchestrator.OnApplyChangesErrorOccured(args =>
                {
                    // Continue On Error
                    args.Resolution = ErrorResolution.RetryOneMoreTimeAndContinueOnError;
                    Assert.NotNull(args.Exception);
                    Assert.NotNull(args.ErrorRow);
                    Assert.NotNull(args.SchemaTable);
                    Assert.Equal(SyncRowState.Modified, args.ApplyType);
                });

                var s = await agent.SynchronizeAsync(Tables);

                Assert.Equal(0, s.TotalChangesDownloadedFromServer);
                Assert.Equal(2, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalChangesAppliedOnServer);
                Assert.Equal(0, s.TotalChangesAppliedOnClient);
                Assert.Equal(0, s.TotalChangesFailedToApplyOnClient);
                Assert.Equal(1, s.TotalChangesFailedToApplyOnServer);
                Assert.Equal(0, s.TotalResolvedConflicts);

                var batchInfos = await agent.LocalOrchestrator.LoadBatchInfosAsync();

                Assert.NotNull(batchInfos);
                Assert.Single(batchInfos);
                Assert.Single(batchInfos[0].BatchPartsInfo);
                Assert.Contains("ERRORS", batchInfos[0].BatchPartsInfo[0].FileName);
                Assert.Equal("ProductCategory", batchInfos[0].BatchPartsInfo[0].TableName);

                var batchInfo = batchInfos[0];

                var syncTables = agent.LocalOrchestrator.LoadTablesFromBatchInfoAsync(batchInfo);

                await foreach (var syncTable in syncTables)
                {
                    Assert.Equal("ProductCategory", syncTable.TableName);
                    Assert.True(syncTable.HasRows);

                    Assert.Equal(SyncRowState.ApplyModifiedFailed, syncTable.Rows[0].RowState);
                }
            }
        }

        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Error_UniqueKey_OnSameTable_RetryOnNextSync(SyncOptions options)
        {
            // Only works for SQL

            // create a server schema without seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            foreach (var client in Clients)
            {
                if (client.ProviderType != ProviderType.Sql)
                    continue;

                // Get a random directory to be sure we are not conflicting with another test
                var directoryName = HelperDatabase.GetRandomName();
                options.BatchDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDirectory(), directoryName);

                // create empty client databases
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

                var agent = new SyncAgent(client.Provider, Server.Provider, options);
                await agent.SynchronizeAsync(Tables);

                await Generate_Client_UniqueKeyError(client.Provider as SqlSyncProvider);

                agent.RemoteOrchestrator.OnApplyChangesErrorOccured(args =>
                {
                    // Continue On Error
                    args.Resolution = ErrorResolution.RetryOnNextSync;
                    Assert.NotNull(args.Exception);
                    Assert.NotNull(args.ErrorRow);
                    Assert.NotNull(args.SchemaTable);
                    Assert.Equal(SyncRowState.Modified, args.ApplyType);
                });

                var s = await agent.SynchronizeAsync(Tables);

                Assert.Equal(0, s.TotalChangesDownloadedFromServer);
                Assert.Equal(2, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalChangesAppliedOnServer);
                Assert.Equal(0, s.TotalChangesAppliedOnClient);
                Assert.Equal(0, s.TotalChangesFailedToApplyOnClient);
                Assert.Equal(1, s.TotalChangesFailedToApplyOnServer);
                Assert.Equal(0, s.TotalResolvedConflicts);

                var batchInfos = await agent.LocalOrchestrator.LoadBatchInfosAsync();

                Assert.NotNull(batchInfos);
                Assert.Single(batchInfos);
                Assert.Single(batchInfos[0].BatchPartsInfo);
                Assert.Contains("ERROR", batchInfos[0].BatchPartsInfo[0].FileName);
                Assert.Equal("ProductCategory", batchInfos[0].BatchPartsInfo[0].TableName);

                var batchInfo = batchInfos[0];

                var syncTables = agent.LocalOrchestrator.LoadTablesFromBatchInfoAsync(batchInfo);

                await foreach (var syncTable in syncTables)
                {
                    Assert.Equal("ProductCategory", syncTable.TableName);
                    Assert.True(syncTable.HasRows);

                    Assert.Equal(SyncRowState.RetryModifiedOnNextSync, syncTable.Rows[0].RowState);
                }

                s = await agent.SynchronizeAsync(Tables);

                Assert.Equal(0, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalChangesAppliedOnServer);
                Assert.Equal(0, s.TotalChangesAppliedOnClient);
                Assert.Equal(0, s.TotalChangesFailedToApplyOnClient);
                Assert.Equal(1, s.TotalChangesFailedToApplyOnServer);
                Assert.Equal(0, s.TotalResolvedConflicts);

                batchInfos = await agent.LocalOrchestrator.LoadBatchInfosAsync();

                Assert.NotNull(batchInfos);
                Assert.Single(batchInfos);
                Assert.Single(batchInfos[0].BatchPartsInfo);
                Assert.Contains("ERROR", batchInfos[0].BatchPartsInfo[0].FileName);
                Assert.Equal("ProductCategory", batchInfos[0].BatchPartsInfo[0].TableName);

                batchInfo = batchInfos[0];

                syncTables = agent.LocalOrchestrator.LoadTablesFromBatchInfoAsync(batchInfo);

                await foreach (var syncTable in syncTables)
                {
                    Assert.Equal("ProductCategory", syncTable.TableName);
                    Assert.True(syncTable.HasRows);

                    Assert.Equal(SyncRowState.RetryModifiedOnNextSync, syncTable.Rows[0].RowState);
                }
            }
        }



        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Error_UniqueKey_OnSameTable_RetryOnNextSync_ThenResolveClient(SyncOptions options)
        {
            // Only works for SQL

            // create a server schema without seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            foreach (var client in Clients)
            {
                if (client.ProviderType != ProviderType.Sql)
                    continue;

                // Get a random directory to be sure we are not conflicting with another test
                var directoryName = HelperDatabase.GetRandomName();
                options.BatchDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDirectory(), directoryName);

                // create empty client databases
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

                var agent = new SyncAgent(client.Provider, Server.Provider, options);
                await agent.SynchronizeAsync(Tables);

                await Generate_Client_UniqueKeyError(client.Provider as SqlSyncProvider);

                agent.RemoteOrchestrator.OnApplyChangesErrorOccured(args =>
                {
                    // Continue On Error
                    args.Resolution = ErrorResolution.RetryOnNextSync;
                    Assert.NotNull(args.Exception);
                    Assert.NotNull(args.ErrorRow);
                    Assert.NotNull(args.SchemaTable);
                    Assert.Equal(SyncRowState.Modified, args.ApplyType);
                });

                var s = await agent.SynchronizeAsync(Tables);

                Assert.Equal(0, s.TotalChangesDownloadedFromServer);
                Assert.Equal(2, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalChangesAppliedOnServer);
                Assert.Equal(0, s.TotalChangesAppliedOnClient);
                Assert.Equal(0, s.TotalChangesFailedToApplyOnClient);
                Assert.Equal(1, s.TotalChangesFailedToApplyOnServer);
                Assert.Equal(0, s.TotalResolvedConflicts);

                var batchInfos = await agent.LocalOrchestrator.LoadBatchInfosAsync();

                Assert.NotNull(batchInfos);
                Assert.Single(batchInfos);
                Assert.Single(batchInfos[0].BatchPartsInfo);
                Assert.Contains("ERROR", batchInfos[0].BatchPartsInfo[0].FileName);
                Assert.Equal("ProductCategory", batchInfos[0].BatchPartsInfo[0].TableName);

                var batchInfo = batchInfos[0];

                var syncTables = agent.LocalOrchestrator.LoadTablesFromBatchInfoAsync(batchInfo);

                await foreach (var syncTable in syncTables)
                {
                    Assert.Equal("ProductCategory", syncTable.TableName);
                    Assert.True(syncTable.HasRows);

                    Assert.Equal(SyncRowState.RetryModifiedOnNextSync, syncTable.Rows[0].RowState);
                }

                await Resolve_Client_UniqueKeyError(client.Provider as SqlSyncProvider);


                s = await agent.SynchronizeAsync(Tables);

                Assert.Equal(0, s.TotalChangesDownloadedFromServer);
                Assert.Equal(13, s.TotalChangesUploadedToServer);
                Assert.Equal(13, s.TotalChangesAppliedOnServer);
                Assert.Equal(0, s.TotalChangesAppliedOnClient);
                Assert.Equal(0, s.TotalChangesFailedToApplyOnClient);
                Assert.Equal(0, s.TotalChangesFailedToApplyOnServer);
                Assert.Equal(0, s.TotalResolvedConflicts);

                batchInfos = await agent.LocalOrchestrator.LoadBatchInfosAsync();

                Assert.Empty(batchInfos);
            }
        }


        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Error_UniqueKey_OnSameTable_RetryOneMoreTime_ThenResolveClient(SyncOptions options)
        {
            // Only works for SQL

            // create a server schema without seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            foreach (var client in Clients)
            {
                if (client.ProviderType != ProviderType.Sql)
                    continue;

                // Get a random directory to be sure we are not conflicting with another test
                var directoryName = HelperDatabase.GetRandomName();
                options.BatchDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDirectory(), directoryName);

                // create empty client databases
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

                var agent = new SyncAgent(client.Provider, Server.Provider, options);
                await agent.SynchronizeAsync(Tables);

                await Generate_Client_UniqueKeyError(client.Provider as SqlSyncProvider);

                agent.RemoteOrchestrator.OnApplyChangesErrorOccured(args =>
                {
                    // Continue On Error
                    args.Resolution = ErrorResolution.RetryOneMoreTimeAndThrowOnError;
                    Assert.NotNull(args.Exception);
                    Assert.NotNull(args.ErrorRow);
                    Assert.NotNull(args.SchemaTable);
                    Assert.Equal(SyncRowState.Modified, args.ApplyType);
                });

                var exc = await Assert.ThrowsAsync<SyncException>(() => agent.SynchronizeAsync(Tables));

                Assert.NotNull(exc);

                var batchInfos = await agent.LocalOrchestrator.LoadBatchInfosAsync();

                Assert.Empty(batchInfos);

                await Resolve_Client_UniqueKeyError(client.Provider as SqlSyncProvider);

                var s = await agent.SynchronizeAsync(Tables);
                
                batchInfos = await agent.LocalOrchestrator.LoadBatchInfosAsync();

                Assert.Empty(batchInfos);

                Assert.Equal(0, s.TotalChangesDownloadedFromServer);
                Assert.Equal(13, s.TotalChangesUploadedToServer);
                Assert.Equal(13, s.TotalChangesAppliedOnServer);
                Assert.Equal(0, s.TotalChangesAppliedOnClient);
                Assert.Equal(0, s.TotalChangesFailedToApplyOnClient);
                Assert.Equal(0, s.TotalChangesFailedToApplyOnServer);
                Assert.Equal(0, s.TotalResolvedConflicts);
            }
        }


        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Error_UniqueKey_OnSameTable_ContinueOnError_ThenResolveClient(SyncOptions options)
        {
            // Only works for SQL

            // create a server schema without seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            foreach (var client in Clients)
            {
                if (client.ProviderType != ProviderType.Sql)
                    continue;

                // Get a random directory to be sure we are not conflicting with another test
                var directoryName = HelperDatabase.GetRandomName();
                options.BatchDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDirectory(), directoryName);

                // create empty client databases
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

                var agent = new SyncAgent(client.Provider, Server.Provider, options);
                await agent.SynchronizeAsync(Tables);

                await Generate_Client_UniqueKeyError(client.Provider as SqlSyncProvider);

                agent.RemoteOrchestrator.OnApplyChangesErrorOccured(args =>
                {
                    // Continue On Error
                    args.Resolution = ErrorResolution.ContinueOnError;
                    Assert.NotNull(args.Exception);
                    Assert.NotNull(args.ErrorRow);
                    Assert.NotNull(args.SchemaTable);
                    Assert.Equal(SyncRowState.Modified, args.ApplyType);
                });

                var s = await agent.SynchronizeAsync(Tables);

                Assert.Equal(0, s.TotalChangesDownloadedFromServer);
                Assert.Equal(2, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalChangesAppliedOnServer);
                Assert.Equal(0, s.TotalChangesAppliedOnClient);
                Assert.Equal(0, s.TotalChangesFailedToApplyOnClient);
                Assert.Equal(1, s.TotalChangesFailedToApplyOnServer);
                Assert.Equal(0, s.TotalResolvedConflicts);

                var batchInfos = await agent.LocalOrchestrator.LoadBatchInfosAsync();

                Assert.NotNull(batchInfos);
                Assert.Single(batchInfos);
                Assert.Single(batchInfos[0].BatchPartsInfo);
                Assert.Contains("ERROR", batchInfos[0].BatchPartsInfo[0].FileName);
                Assert.Equal("ProductCategory", batchInfos[0].BatchPartsInfo[0].TableName);

                var batchInfo = batchInfos[0];

                var syncTables = agent.LocalOrchestrator.LoadTablesFromBatchInfoAsync(batchInfo);

                await foreach (var syncTable in syncTables)
                {
                    Assert.Equal("ProductCategory", syncTable.TableName);
                    Assert.True(syncTable.HasRows);

                    Assert.Equal(SyncRowState.ApplyModifiedFailed, syncTable.Rows[0].RowState);
                }

                await Resolve_Client_UniqueKeyError(client.Provider as SqlSyncProvider);

                s = await agent.SynchronizeAsync(Tables);

                Assert.Equal(0, s.TotalChangesDownloadedFromServer);
                Assert.Equal(13, s.TotalChangesUploadedToServer);
                Assert.Equal(13, s.TotalChangesAppliedOnServer);
                Assert.Equal(0, s.TotalChangesAppliedOnClient);
                Assert.Equal(0, s.TotalChangesFailedToApplyOnClient);
                Assert.Equal(0, s.TotalChangesFailedToApplyOnServer);
                Assert.Equal(0, s.TotalResolvedConflicts);

                batchInfos = await agent.LocalOrchestrator.LoadBatchInfosAsync();

                Assert.Empty(batchInfos);
            }
        }



        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Error_UniqueKey_OnSameTable_RetryOnNextSync_Twice_ThenResolveClient(SyncOptions options)
        {
            // Only works for SQL

            // create a server schema without seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            foreach (var client in Clients)
            {
                if (client.ProviderType != ProviderType.Sql)
                    continue;

                // Get a random directory to be sure we are not conflicting with another test
                var directoryName = HelperDatabase.GetRandomName();
                options.BatchDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDirectory(), directoryName);

                // create empty client databases
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

                var agent = new SyncAgent(client.Provider, Server.Provider, options);
                await agent.SynchronizeAsync(Tables);

                await Generate_Client_UniqueKeyError(client.Provider as SqlSyncProvider);

                agent.RemoteOrchestrator.OnApplyChangesErrorOccured(args =>
                {
                    // Continue On Error
                    args.Resolution = ErrorResolution.RetryOnNextSync;
                    Assert.NotNull(args.Exception);
                    Assert.NotNull(args.ErrorRow);
                    Assert.NotNull(args.SchemaTable);
                    Assert.Equal(SyncRowState.Modified, args.ApplyType);
                });

                var s = await agent.SynchronizeAsync(Tables);

                Assert.Equal(0, s.TotalChangesDownloadedFromServer);
                Assert.Equal(2, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalChangesAppliedOnServer);
                Assert.Equal(0, s.TotalChangesAppliedOnClient);
                Assert.Equal(0, s.TotalChangesFailedToApplyOnClient);
                Assert.Equal(1, s.TotalChangesFailedToApplyOnServer);
                Assert.Equal(0, s.TotalResolvedConflicts);

                var batchInfos = await agent.LocalOrchestrator.LoadBatchInfosAsync();

                Assert.NotNull(batchInfos);
                Assert.Single(batchInfos);
                Assert.Single(batchInfos[0].BatchPartsInfo);
                Assert.Contains("ERROR", batchInfos[0].BatchPartsInfo[0].FileName);
                Assert.Equal("ProductCategory", batchInfos[0].BatchPartsInfo[0].TableName);

                var batchInfo = batchInfos[0];

                var syncTables = agent.LocalOrchestrator.LoadTablesFromBatchInfoAsync(batchInfo);

                await foreach (var syncTable in syncTables)
                {
                    Assert.Equal("ProductCategory", syncTable.TableName);
                    Assert.True(syncTable.HasRows);

                    Assert.Equal(SyncRowState.RetryModifiedOnNextSync, syncTable.Rows[0].RowState);
                }

                await Update_Client_UniqueKeyError(client.Provider as SqlSyncProvider);

                s = await agent.SynchronizeAsync(Tables);

                Assert.Equal(0, s.TotalChangesDownloadedFromServer);
                Assert.Equal(13, s.TotalChangesUploadedToServer);
                Assert.Equal(12, s.TotalChangesAppliedOnServer);
                Assert.Equal(0, s.TotalChangesAppliedOnClient);
                Assert.Equal(0, s.TotalChangesFailedToApplyOnClient);
                Assert.Equal(2, s.TotalChangesFailedToApplyOnServer);
                Assert.Equal(0, s.TotalResolvedConflicts);

                batchInfos = await agent.LocalOrchestrator.LoadBatchInfosAsync();

                Assert.NotNull(batchInfos);
                Assert.Single(batchInfos);
                Assert.Single(batchInfos[0].BatchPartsInfo);
                Assert.Contains("ERRORS", batchInfos[0].BatchPartsInfo[0].FileName);
                Assert.Equal("ProductCategory", batchInfos[0].BatchPartsInfo[0].TableName);

                batchInfo = batchInfos[0];

                syncTables = agent.LocalOrchestrator.LoadTablesFromBatchInfoAsync(batchInfo);

                await foreach (var syncTable in syncTables)
                {
                    Assert.Equal("ProductCategory", syncTable.TableName);
                    Assert.True(syncTable.HasRows);

                    Assert.Equal(SyncRowState.RetryModifiedOnNextSync, syncTable.Rows[0].RowState);
                }



                await Resolve_Client_UniqueKeyError(client.Provider as SqlSyncProvider);

                s = await agent.SynchronizeAsync(Tables);

                Assert.Equal(0, s.TotalChangesDownloadedFromServer);
                Assert.Equal(13, s.TotalChangesUploadedToServer);
                Assert.Equal(13, s.TotalChangesAppliedOnServer);
                Assert.Equal(0, s.TotalChangesAppliedOnClient);
                Assert.Equal(0, s.TotalChangesFailedToApplyOnClient);
                Assert.Equal(0, s.TotalChangesFailedToApplyOnServer);
                Assert.Equal(0, s.TotalResolvedConflicts);


                batchInfos = await agent.LocalOrchestrator.LoadBatchInfosAsync();

                Assert.Empty(batchInfos);
            }
        }


        // ------------------------------------------------------------------------
        // Generate Foreign Key failure
        // ------------------------------------------------------------------------

        /// <summary>
        /// Generate an insert on both side; will be resolved as RemoteExistsLocalExists on both side
        /// </summary>
        private async Task Generate_ForeignKeyError()
        {
            using var ctx = new AdventureWorksContext(this.Server);
            ctx.Add(new ProductCategory
            {
                ProductCategoryId = "ZZZZ",
                Name = HelperDatabase.GetRandomName("SRV")
            });
            ctx.Add(new ProductCategory
            {
                ProductCategoryId = "AAAA",
                ParentProductCategoryId = "ZZZZ",
                Name = HelperDatabase.GetRandomName("SRV")
            });
            await ctx.SaveChangesAsync();

        }


        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Error_ForeignKey_OnSameTable_RaiseError(SyncOptions options)
        {
            // create a server schema without seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            await Generate_ForeignKeyError();

            foreach (var client in Clients)
            {
                // Get a random directory to be sure we are not conflicting with another test
                var directoryName = HelperDatabase.GetRandomName();
                options.BatchDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDirectoryName(), directoryName);

                // create empty client databases
                await this.EnsureDatabaseSchemaAndSeedAsync(client, false, UseFallbackSchema);

                // Disable bulk operations to generate the fk constraint failure
                client.Provider.UseBulkOperations = false;

                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                var exc = await Assert.ThrowsAsync<SyncException>(() => agent.SynchronizeAsync(Tables));

                Assert.NotNull(exc);

                var batchInfos = await agent.LocalOrchestrator.LoadBatchInfosAsync();

                Assert.Empty(batchInfos);
            }
        }

        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Error_ForeignKey_OnSameTable_ContinueOnError(SyncOptions options)
        {
            // create a server schema without seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            await Generate_ForeignKeyError();

            foreach (var client in Clients)
            {
                // Get a random directory to be sure we are not conflicting with another test
                var directoryName = HelperDatabase.GetRandomName();
                options.BatchDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDirectory(), directoryName);

                // create empty client databases
                await this.EnsureDatabaseSchemaAndSeedAsync(client, false, UseFallbackSchema);

                // Disable bulk operations to generate the fk constraint failure
                client.Provider.UseBulkOperations = false;

                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                agent.LocalOrchestrator.OnApplyChangesErrorOccured(args =>
                {
                    // Continue On Error
                    args.Resolution = ErrorResolution.ContinueOnError;
                    Assert.NotNull(args.Exception);
                    Assert.NotNull(args.ErrorRow);
                    Assert.NotNull(args.SchemaTable);
                    Assert.Equal(SyncRowState.Modified, args.ApplyType);
                });

                var s = await agent.SynchronizeAsync(Tables);

                // Download 2 rows
                // But applied only 1
                // The other one is a failed inserted row
                Assert.Equal(2, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalChangesAppliedOnClient);
                Assert.Equal(1, s.TotalChangesFailedToApplyOnClient);
                Assert.Equal(0, s.TotalResolvedConflicts);

                var batchInfos = await agent.LocalOrchestrator.LoadBatchInfosAsync();

                Assert.NotNull(batchInfos);
                Assert.Single(batchInfos);
                Assert.Single(batchInfos[0].BatchPartsInfo);
                Assert.Contains("ERRORS", batchInfos[0].BatchPartsInfo[0].FileName);
                Assert.Equal("ProductCategory", batchInfos[0].BatchPartsInfo[0].TableName);

                var batchInfo = batchInfos[0];

                var syncTables = agent.LocalOrchestrator.LoadTablesFromBatchInfoAsync(batchInfo);

                await foreach (var syncTable in syncTables)
                {
                    Assert.Equal("ProductCategory", syncTable.TableName);
                    Assert.True(syncTable.HasRows);

                    Assert.Equal(SyncRowState.ApplyModifiedFailed, syncTable.Rows[0].RowState);
                }

                s = await agent.SynchronizeAsync(Tables);

                Assert.Equal(0, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalChangesAppliedOnClient);
                Assert.Equal(0, s.TotalChangesFailedToApplyOnClient);
                Assert.Equal(0, s.TotalResolvedConflicts);

                batchInfos = await agent.LocalOrchestrator.LoadBatchInfosAsync();

                Assert.NotNull(batchInfos);
                Assert.Single(batchInfos);
                Assert.Single(batchInfos[0].BatchPartsInfo);
                Assert.Contains("ERRORS", batchInfos[0].BatchPartsInfo[0].FileName);
                Assert.Equal("ProductCategory", batchInfos[0].BatchPartsInfo[0].TableName);

                batchInfo = batchInfos[0];

                syncTables = agent.LocalOrchestrator.LoadTablesFromBatchInfoAsync(batchInfo);

                await foreach (var syncTable in syncTables)
                {
                    Assert.Equal("ProductCategory", syncTable.TableName);
                    Assert.True(syncTable.HasRows);

                    Assert.Equal(SyncRowState.ApplyModifiedFailed, syncTable.Rows[0].RowState);
                }

            }
        }

        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Error_ForeignKey_OnSameTable_RetryOneMoreTime(SyncOptions options)
        {
            // create a server schema without seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            await Generate_ForeignKeyError();

            foreach (var client in Clients)
            {

                // Get a random directory to be sure we are not conflicting with another test
                var directoryName = HelperDatabase.GetRandomName();
                options.BatchDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDirectory(), directoryName);

                // create empty client databases
                await this.EnsureDatabaseSchemaAndSeedAsync(client, false, UseFallbackSchema);

                // Disable bulk operations to generate the fk constraint failure
                client.Provider.UseBulkOperations = false;

                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                agent.LocalOrchestrator.OnApplyChangesErrorOccured(args =>
                {
                    // Continue On Error
                    args.Resolution = ErrorResolution.RetryOneMoreTimeAndThrowOnError;
                    Assert.NotNull(args.Exception);
                    Assert.NotNull(args.ErrorRow);
                    Assert.NotNull(args.SchemaTable);
                    Assert.Equal(SyncRowState.Modified, args.ApplyType);
                });

                var s = await agent.SynchronizeAsync(Tables);

                // Download 2 rows
                // But applied only 1
                // The other one is a failed inserted row
                Assert.Equal(2, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(2, s.TotalChangesAppliedOnClient);
                Assert.Equal(0, s.TotalChangesFailedToApplyOnClient);
                Assert.Equal(0, s.TotalResolvedConflicts);

                var batchInfos = await agent.LocalOrchestrator.LoadBatchInfosAsync();

                Assert.Empty(batchInfos);
            }
        }

        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Error_ForeignKey_OnSameTable_RetryOnNextSync(SyncOptions options)
        {
            // create a server schema without seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            await Generate_ForeignKeyError();

            foreach (var client in Clients)
            {
                // Get a random directory to be sure we are not conflicting with another test
                var directoryName = HelperDatabase.GetRandomName();
                options.BatchDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDirectoryName(), directoryName);

                // create empty client databases
                await this.EnsureDatabaseSchemaAndSeedAsync(client, false, UseFallbackSchema);

                // Disable bulk operations to generate the fk constraint failure
                client.Provider.UseBulkOperations = false;

                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                agent.LocalOrchestrator.OnApplyChangesErrorOccured(args =>
                {
                    // Continue On Error
                    args.Resolution = ErrorResolution.RetryOnNextSync;
                    Assert.NotNull(args.Exception);
                    Assert.NotNull(args.ErrorRow);
                    Assert.NotNull(args.SchemaTable);
                    Assert.Equal(SyncRowState.Modified, args.ApplyType);
                });

                var s = await agent.SynchronizeAsync(Tables);

                // Download 2 rows
                // But applied only 1
                // The other one is a failed inserted row
                Assert.Equal(2, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalChangesAppliedOnClient);
                Assert.Equal(1, s.TotalChangesFailedToApplyOnClient);
                Assert.Equal(0, s.TotalResolvedConflicts);

                var batchInfos = await agent.LocalOrchestrator.LoadBatchInfosAsync();

                Assert.NotNull(batchInfos);
                Assert.Single(batchInfos);
                Assert.Single(batchInfos[0].BatchPartsInfo);
                Assert.Contains("ERRORS", batchInfos[0].BatchPartsInfo[0].FileName);
                Assert.Equal("ProductCategory", batchInfos[0].BatchPartsInfo[0].TableName);

                var batchInfo = batchInfos[0];

                var syncTables = agent.LocalOrchestrator.LoadTablesFromBatchInfoAsync(batchInfo);

                await foreach (var syncTable in syncTables)
                {
                    Assert.Equal("ProductCategory", syncTable.TableName);
                    Assert.True(syncTable.HasRows);

                    Assert.Equal(SyncRowState.RetryModifiedOnNextSync, syncTable.Rows[0].RowState);
                }

                s = await agent.SynchronizeAsync(Tables);

                Assert.Equal(0, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalChangesAppliedOnClient);
                Assert.Equal(0, s.TotalChangesFailedToApplyOnClient);
                Assert.Equal(0, s.TotalResolvedConflicts);

                batchInfos = await agent.LocalOrchestrator.LoadBatchInfosAsync();

                Assert.Empty(batchInfos);
            }
        }

        // ------------------------------------------------------------------------
        // InsertClient - InsertServer
        // ------------------------------------------------------------------------

        /// <summary>
        /// Generate an insert on both side; will be resolved as RemoteExistsLocalExists on both side
        /// </summary>
        private async Task Generate_InsertClient_InsertServer((string DatabaseName, ProviderType ProviderType, CoreProvider Provider) client, SyncOptions options)
        {
            // create empty client databases
            await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // Execute a sync on all clients to initialize client and server schema 
            var agent = new SyncAgent(client.Provider, Server.Provider, options);

            // init both server and client
            await agent.SynchronizeAsync(Tables);

            // Insert the conflict product category on each client
            var productId = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 6);
            var productCategoryNameClient = HelperDatabase.GetRandomName("CLI");
            var productCategoryNameServer = HelperDatabase.GetRandomName("SRV");

            using (var ctx = new AdventureWorksContext(client, this.UseFallbackSchema))
            {
                ctx.Add(new ProductCategory
                {
                    ProductCategoryId = productId,
                    Name = productCategoryNameClient
                });
                await ctx.SaveChangesAsync();
            }

            using (var ctx = new AdventureWorksContext(this.Server))
            {
                ctx.Add(new ProductCategory
                {
                    ProductCategoryId = productId,
                    Name = productCategoryNameServer
                });
                await ctx.SaveChangesAsync();
            }

        }


        /// <summary>
        /// Generate a conflict when inserting one row on server and the same row on each client
        /// Server should wins the conflict since it's the default behavior
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Conflict_IC_IS_ServerShouldWins(SyncOptions options)
        {
            // create a server schema without seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // Execute a sync on all clients and check results
            // Each client will upload its row (conflicting)
            // then download the others client lines + the conflict (some Clients.count)
            foreach (var client in Clients)
            {
                await Generate_InsertClient_InsertServer(client, options);

                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                var s = await agent.SynchronizeAsync(Tables);

                Assert.Equal(1, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalResolvedConflicts);

                await CheckProductCategoryRows(client, "SRV");
            }

        }

        /// <summary>
        /// Generate a conflict when inserting one row on server and the same row on each client
        /// Server should wins the conflict since it's the default behavior
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Conflict_IC_IS_ServerShouldWins_CozHandler(SyncOptions options)
        {
            // create a server schema without seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // Execute a sync on all clients and check results
            // Each client will upload its row (conflicting)
            // then download the others client lines + the conflict (some Clients.count)
            foreach (var client in Clients)
            {
                await Generate_InsertClient_InsertServer(client, options);

                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                var localOrchestrator = agent.LocalOrchestrator;
                var remoteOrchestrator = agent.RemoteOrchestrator;

                // From client : Remote is server, Local is client
                localOrchestrator.OnApplyChangesConflictOccured(async acf =>
                {
                    // Check conflict is correctly set
                    var conflict = await acf.GetSyncConflictAsync();
                    var localRow = conflict.LocalRow;
                    var remoteRow = conflict.RemoteRow;

                    // remote is server; local is client
                    Assert.StartsWith("SRV", remoteRow["Name"].ToString());
                    Assert.StartsWith("CLI", localRow["Name"].ToString());

                    Assert.Equal(SyncRowState.Modified, conflict.RemoteRow.RowState);
                    Assert.Equal(SyncRowState.Modified, conflict.LocalRow.RowState);

                    // The conflict resolution is always the opposite from the one configured by options
                    Assert.Equal(ConflictResolution.ClientWins, acf.Resolution);
                    Assert.Equal(ConflictType.RemoteExistsLocalExists, conflict.Type);
                });

                // From Server : Remote is client, Local is server
                remoteOrchestrator.OnApplyChangesConflictOccured(async acf =>
                {
                    // Check conflict is correctly set
                    var conflict = await acf.GetSyncConflictAsync();
                    var localRow = conflict.LocalRow;
                    var remoteRow = conflict.RemoteRow;

                    // remote is client; local is server
                    Assert.StartsWith("CLI", remoteRow["Name"].ToString());
                    Assert.StartsWith("SRV", localRow["Name"].ToString());

                    Assert.Equal(SyncRowState.Modified, conflict.RemoteRow.RowState);
                    Assert.Equal(SyncRowState.Modified, conflict.LocalRow.RowState);

                    Assert.Equal(ConflictResolution.ServerWins, acf.Resolution);
                    Assert.Equal(ConflictType.RemoteExistsLocalExists, conflict.Type);
                });

                var s = await agent.SynchronizeAsync(Tables);

                Assert.Equal(1, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalResolvedConflicts);

                await CheckProductCategoryRows(client, "SRV");
            }

        }

        /// <summary>
        /// Generate a conflict when inserting one row on server and the same row on each client
        /// Client should wins the conflict because configuration set to ClientWins
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Conflict_IC_IS_ClientShouldWins_CozConfiguration(SyncOptions options)
        {
            // create a server schema without seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // Execute a sync on all clients and check results
            // Each client will upload its row (conflicting)
            // then download the others client lines (and not the conflict since it's resolved)
            foreach (var client in Clients)
            {
                await Generate_InsertClient_InsertServer(client, options);

                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                agent.Options.ConflictResolutionPolicy = ConflictResolutionPolicy.ClientWins;

                var s = await agent.SynchronizeAsync(Tables);

                Assert.Equal(0, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalResolvedConflicts);

                await CheckProductCategoryRows(client, "CLI");
            }
        }

        /// <summary>
        /// Generate a conflict when inserting one row on server and the same row on each client
        /// Client should wins the conflict because configuration set to ClientWins
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Conflict_IC_IS_ClientShouldWins_CozConfiguration_CozHandler(SyncOptions options)
        {
            // create a server schema without seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // Execute a sync on all clients and check results
            // Each client will upload its row (conflicting)
            // then download the others client lines (and not the conflict since it's resolved)
            foreach (var client in Clients)
            {
                await Generate_InsertClient_InsertServer(client, options);

                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                agent.Options.ConflictResolutionPolicy = ConflictResolutionPolicy.ClientWins;

                var localOrchestrator = agent.LocalOrchestrator;
                var remoteOrchestrator = agent.RemoteOrchestrator;

                // From client : Remote is server, Local is client
                localOrchestrator.OnApplyChangesConflictOccured(acf =>
                {
                    // Since we have a ClientWins resolution,
                    // We should NOT have any conflict raised on the client side
                    // Since the conflict has been resolver on server
                    // And Server forces applied the client row
                    // So far the client row is good and should not raise any conflict

                    throw new Exception("Should not happen !!");
                });

                // From Server : Remote is client, Local is server
                remoteOrchestrator.OnApplyChangesConflictOccured(async acf =>
                {
                    // Check conflict is correctly set
                    var conflict = await acf.GetSyncConflictAsync();
                    var localRow = conflict.LocalRow;
                    var remoteRow = conflict.RemoteRow;

                    Assert.Equal(SyncRowState.Modified, conflict.RemoteRow.RowState);
                    Assert.Equal(SyncRowState.Modified, conflict.LocalRow.RowState);

                    Assert.Equal(ConflictResolution.ClientWins, acf.Resolution);
                    Assert.Equal(ConflictType.RemoteExistsLocalExists, conflict.Type);
                });


                var s = await agent.SynchronizeAsync(Tables);

                Assert.Equal(0, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalResolvedConflicts);

                await CheckProductCategoryRows(client, "CLI");
            }
        }

        /// <summary>
        /// Generate a conflict when inserting one row on server and the same row on each client
        /// Client should wins the conflict because we have an event raised
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Conflict_IC_IS_ClientShouldWins_CozHandler(SyncOptions options)
        {
            // Create a server schema without seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // Execute a sync on all clients and check results
            // Each client will upload its row (conflicting)
            // then download the others client lines (and not the conflict since it's resolved)
            foreach (var client in Clients)
            {
                await Generate_InsertClient_InsertServer(client, options);

                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                var localOrchestrator = agent.LocalOrchestrator;
                var remoteOrchestrator = agent.RemoteOrchestrator;

                // From client : Remote is server, Local is client
                localOrchestrator.OnApplyChangesConflictOccured(acf =>
                {
                    // Since we have a ClientWins resolution,
                    // We should NOT have any conflict raised on the client side
                    // Since the conflict has been resolver on server
                    // And Server forces applied the client row
                    // So far the client row is good and should not raise any conflict

                    throw new Exception("Should not happen because ConflictResolution.ClientWins !!");
                });

                // From Server : Remote is client, Local is server
                remoteOrchestrator.OnApplyChangesConflictOccured(async acf =>
                {
                    // Check conflict is correctly set
                    var conflict = await acf.GetSyncConflictAsync();
                    var localRow = conflict.LocalRow;
                    var remoteRow = conflict.RemoteRow;

                    Assert.StartsWith("SRV", localRow["Name"].ToString());
                    Assert.StartsWith("CLI", remoteRow["Name"].ToString());

                    Assert.Equal(SyncRowState.Modified, conflict.RemoteRow.RowState);
                    Assert.Equal(SyncRowState.Modified, conflict.LocalRow.RowState);

                    Assert.Equal(ConflictResolution.ServerWins, acf.Resolution);
                    Assert.Equal(ConflictType.RemoteExistsLocalExists, conflict.Type);

                    // Client should wins
                    acf.Resolution = ConflictResolution.ClientWins;
                });

                var s = await agent.SynchronizeAsync(Tables);

                Assert.Equal(0, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalResolvedConflicts);

                await CheckProductCategoryRows(client, "CLI");
            }

        }

        // ------------------------------------------------------------------------
        // Update Client - Update Server
        // ------------------------------------------------------------------------

        /// <summary>
        /// Generate an update on both side; will be resolved as RemoteExistsLocalExists on both side
        /// </summary>
        private async Task<string> Generate_UC_US_Conflict((string DatabaseName, ProviderType ProviderType, CoreProvider Provider) client, SyncOptions options)
        {
            // create empty client databases
            await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // Conflict product category
            var conflictProductCategoryId = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 6);
            var productCategoryNameClient = "CLI BIKES " + HelperDatabase.GetRandomName();
            var productCategoryNameServer = "SRV BIKES " + HelperDatabase.GetRandomName();

            // Insert line on server
            using (var ctx = new AdventureWorksContext(this.Server))
            {
                ctx.Add(new ProductCategory { ProductCategoryId = conflictProductCategoryId, Name = "BIKES" });
                await ctx.SaveChangesAsync();
            }

            // Execute a sync on all clients to initialize client and server schema 
            var agent = new SyncAgent(client.Provider, Server.Provider, options);

            // Init both client and server
            await agent.SynchronizeAsync(Tables);

            // Update each client to generate an update conflict
            using (var ctx = new AdventureWorksContext(client, this.UseFallbackSchema))
            {
                var pc = ctx.ProductCategory.Find(conflictProductCategoryId);
                pc.Name = productCategoryNameClient;
                await ctx.SaveChangesAsync();
            }

            // Update server
            using (var ctx = new AdventureWorksContext(this.Server))
            {
                var pc = ctx.ProductCategory.Find(conflictProductCategoryId);
                pc.Name = productCategoryNameServer;
                await ctx.SaveChangesAsync();
            }

            return conflictProductCategoryId;
        }

        /// <summary>
        /// Generate a conflict when inserting one row on server and the same row on each client
        /// Server should wins the conflict because default behavior
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Conflict_UC_US_ServerShouldWins(SyncOptions options)
        {
            // create a server schema without seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // Execute a sync on all clients and check results
            // Each client will upload its row (conflicting)
            // then download the others client lines + conflict that should be ovewritten on client
            foreach (var client in Clients)
            {
                await Generate_UC_US_Conflict(client, options);

                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                var s = await agent.SynchronizeAsync(Tables);

                Assert.Equal(1, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalResolvedConflicts);

                await CheckProductCategoryRows(client, "SRV");
            }


        }

        /// <summary>
        /// Generate a conflict when inserting one row on server and the same row on each client
        /// Server should wins the conflict because default behavior
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Conflict_UC_US_ServerShouldWins_CozHandler(SyncOptions options)
        {
            // create a server schema without seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // Execute a sync on all clients and check results
            // Each client will upload its row (conflicting)
            // then download the others client lines + conflict that should be ovewritten on client
            foreach (var client in Clients)
            {
                await Generate_UC_US_Conflict(client, options);

                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                var localOrchestrator = agent.LocalOrchestrator;
                var remoteOrchestrator = agent.RemoteOrchestrator;

                // From client : Remote is server, Local is client
                localOrchestrator.OnApplyChangesConflictOccured(async acf =>
                {
                    // Check conflict is correctly set
                    var conflict = await acf.GetSyncConflictAsync();
                    var localRow = conflict.LocalRow;
                    var remoteRow = conflict.RemoteRow;

                    // remote is server; local is client
                    Assert.StartsWith("SRV", remoteRow["Name"].ToString());
                    Assert.StartsWith("CLI", localRow["Name"].ToString());

                    Assert.Equal(SyncRowState.Modified, conflict.RemoteRow.RowState);
                    Assert.Equal(SyncRowState.Modified, conflict.LocalRow.RowState);

                    // The conflict resolution is always the opposite from the one configured by options
                    Assert.Equal(ConflictResolution.ClientWins, acf.Resolution);
                    Assert.Equal(ConflictType.RemoteExistsLocalExists, conflict.Type);
                });

                // From Server : Remote is client, Local is server
                remoteOrchestrator.OnApplyChangesConflictOccured(async acf =>
                {
                    // Check conflict is correctly set
                    var conflict = await acf.GetSyncConflictAsync();
                    var localRow = conflict.LocalRow;
                    var remoteRow = conflict.RemoteRow;

                    // remote is client; local is server
                    Assert.StartsWith("CLI", remoteRow["Name"].ToString());
                    Assert.StartsWith("SRV", localRow["Name"].ToString());

                    Assert.Equal(SyncRowState.Modified, conflict.RemoteRow.RowState);
                    Assert.Equal(SyncRowState.Modified, conflict.LocalRow.RowState);

                    Assert.Equal(ConflictResolution.ServerWins, acf.Resolution);
                    Assert.Equal(ConflictType.RemoteExistsLocalExists, conflict.Type);
                });

                var s = await agent.SynchronizeAsync(Tables);

                Assert.Equal(1, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalResolvedConflicts);

                await CheckProductCategoryRows(client, "SRV");
            }


        }

        /// <summary>
        /// Generate a conflict when inserting one row on server and the same row on each client
        /// Server should wins the conflict because default behavior
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Conflict_UC_US_ClientShouldWins_CozConfiguration(SyncOptions options)
        {
            // create a server schema without seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // Execute a sync on all clients and check results
            // Each client will upload its row (conflicting)
            // then download the others client lines + conflict that should be ovewritten on client
            foreach (var client in Clients)
            {
                var id = await Generate_UC_US_Conflict(client, options);
                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                agent.Options.ConflictResolutionPolicy = ConflictResolutionPolicy.ClientWins;

                var s = await agent.SynchronizeAsync(Tables);

                Assert.Equal(0, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalResolvedConflicts);

                await CheckProductCategoryRows(client, "CLI");
            }
        }

        /// <summary>
        /// Generate a conflict when inserting one row on server and the same row on each client
        /// Server should wins the conflict because default behavior
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Conflict_UC_US_ClientShouldWins_CozConfiguration_CozHandler(SyncOptions options)
        {
            // create a server schema without seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // Execute a sync on all clients and check results
            // Each client will upload its row (conflicting)
            // then download the others client lines + conflict that should be ovewritten on client
            foreach (var client in Clients)
            {
                var id = await Generate_UC_US_Conflict(client, options);
                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                agent.Options.ConflictResolutionPolicy = ConflictResolutionPolicy.ClientWins;

                var localOrchestrator = agent.LocalOrchestrator;
                var remoteOrchestrator = agent.RemoteOrchestrator;

                // From client : Remote is server, Local is client
                localOrchestrator.OnApplyChangesConflictOccured(acf =>
                {
                    // Check conflict is correctly set
                    throw new Exception("Should not happen because ConflictResolution.ClientWins");
                });

                // From Server : Remote is client, Local is server
                remoteOrchestrator.OnApplyChangesConflictOccured(async acf =>
                {
                    // Check conflict is correctly set
                    var conflict = await acf.GetSyncConflictAsync();
                    var localRow = conflict.LocalRow;
                    var remoteRow = conflict.RemoteRow;

                    // remote is client; local is server
                    Assert.StartsWith("CLI", remoteRow["Name"].ToString());
                    Assert.StartsWith("SRV", localRow["Name"].ToString());

                    Assert.Equal(SyncRowState.Modified, conflict.RemoteRow.RowState);
                    Assert.Equal(SyncRowState.Modified, conflict.LocalRow.RowState);

                    Assert.Equal(ConflictResolution.ClientWins, acf.Resolution);
                    Assert.Equal(ConflictType.RemoteExistsLocalExists, conflict.Type);
                });

                var s = await agent.SynchronizeAsync(Tables);

                Assert.Equal(0, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalResolvedConflicts);

                await CheckProductCategoryRows(client, "CLI");
            }
        }

        /// <summary>
        /// Generate a conflict when inserting one row on server and the same row on each client
        /// Client should wins coz handler
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Conflict_UC_US_ClientShouldWins_CozHandler(SyncOptions options)
        {
            // create a server schema without seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // Execute a sync on all clients and check results
            // Each client will upload its row (conflicting)
            // then download the others client lines + conflict that should be ovewritten on client
            foreach (var client in Clients)
            {
                var id = await Generate_UC_US_Conflict(client, options);

                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                agent.RemoteOrchestrator.OnApplyChangesConflictOccured(async acf =>
                {
                    // Check conflict is correctly set
                    var conflict = await acf.GetSyncConflictAsync();
                    var localRow = conflict.LocalRow;
                    var remoteRow = conflict.RemoteRow;

                    Assert.Equal(ConflictType.RemoteExistsLocalExists, conflict.Type);

                    Assert.StartsWith("SRV", localRow["Name"].ToString());
                    Assert.StartsWith("CLI", remoteRow["Name"].ToString());

                    // Client should wins
                    acf.Resolution = ConflictResolution.ClientWins;
                });

                var s = await agent.SynchronizeAsync(Tables);

                Assert.Equal(0, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalResolvedConflicts);

                await CheckProductCategoryRows(client, "CLI");

            }
        }

        /// <summary>
        /// Generate a conflict when inserting one row on server and the same row on each client
        /// Client should wins coz handler
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Conflict_UC_US_Resolved_ByMerge(SyncOptions options)
        {
            // create a server schema without seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            foreach (var client in Clients)
            {
                var id = await Generate_UC_US_Conflict(client, options);

                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                var localOrchestrator = agent.LocalOrchestrator;
                var remoteOrchestrator = agent.RemoteOrchestrator;

                // From client : Remote is server, Local is client
                localOrchestrator.OnApplyChangesConflictOccured(async acf =>
                {
                    // Check conflict is correctly set
                    var conflict = await acf.GetSyncConflictAsync();
                    var localRow = conflict.LocalRow;
                    var remoteRow = conflict.RemoteRow;

                    // remote is server; local is client
                    Assert.StartsWith("BOTH", remoteRow["Name"].ToString());
                    Assert.StartsWith("CLI", localRow["Name"].ToString());

                    Assert.Equal(SyncRowState.Modified, conflict.RemoteRow.RowState);
                    Assert.Equal(SyncRowState.Modified, conflict.LocalRow.RowState);

                    // The conflict resolution is always the opposite from the one configured by options
                    Assert.Equal(ConflictResolution.ClientWins, acf.Resolution);
                    Assert.Equal(ConflictType.RemoteExistsLocalExists, conflict.Type);
                });

                // From Server : Remote is client, Local is server
                remoteOrchestrator.OnApplyChangesConflictOccured(async acf =>
                {
                    // Check conflict is correctly set
                    var conflict = await acf.GetSyncConflictAsync();
                    var localRow = conflict.LocalRow;
                    var remoteRow = conflict.RemoteRow;

                    // remote is client; local is server

                    Assert.StartsWith("SRV", localRow["Name"].ToString());
                    Assert.StartsWith("CLI", remoteRow["Name"].ToString());

                    Assert.Equal(SyncRowState.Modified, conflict.RemoteRow.RowState);
                    Assert.Equal(SyncRowState.Modified, conflict.LocalRow.RowState);

                    Assert.Equal(ConflictResolution.ServerWins, acf.Resolution);
                    Assert.Equal(ConflictType.RemoteExistsLocalExists, conflict.Type);

                    // Merge row
                    acf.Resolution = ConflictResolution.MergeRow;

                    Assert.NotNull(acf.FinalRow);

                    acf.FinalRow["Name"] = "BOTH BIKES" + HelperDatabase.GetRandomName();

                });


                var s = await agent.SynchronizeAsync(Tables);

                Assert.Equal(1, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalResolvedConflicts);

                await CheckProductCategoryRows(client, "BOTH");
            }
        }



        // ------------------------------------------------------------------------
        // Delete Client - Update Server
        // ------------------------------------------------------------------------

        /// <summary>
        /// Generate a delete on the client and an update on the server; will generate:
        /// - RemoteIsDeletedLocalExists from the Server POV
        /// - RemoteExistsLocalIsDeleted from the Client POV
        /// </summary>
        private async Task<string> Generate_DC_US_Conflict((string DatabaseName, ProviderType ProviderType, CoreProvider Provider) client, SyncOptions options)
        {
            var productId = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 6);
            var productCategoryName = HelperDatabase.GetRandomName("CLI");
            var productCategoryNameUpdated = HelperDatabase.GetRandomName("SRV");

            // create empty client databases
            await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // Insert a product category and sync it on all clients
            using (var ctx = new AdventureWorksContext(this.Server))
            {
                ctx.Add(new ProductCategory { ProductCategoryId = productId, Name = productCategoryName });
                await ctx.SaveChangesAsync();
            }

            // Execute a sync on all clients to re-initialize client and server schema 
            await new SyncAgent(client.Provider, Server.Provider, options).SynchronizeAsync(Tables);

            // Delete product category on client
            using (var ctx = new AdventureWorksContext(client, this.UseFallbackSchema))
            {
                var pcdel = ctx.ProductCategory.Single(pc => pc.ProductCategoryId == productId);
                ctx.ProductCategory.Remove(pcdel);
                await ctx.SaveChangesAsync();
            }

            // Update on Server
            using (var ctx = new AdventureWorksContext(this.Server))
            {
                var pcupdated = ctx.ProductCategory.Single(pc => pc.ProductCategoryId == productId);
                pcupdated.Name = productCategoryNameUpdated;
                await ctx.SaveChangesAsync();
            }

            return productId;
        }

        /// <summary>
        /// Generate a conflict when inserting one row on server and the same row on each client
        /// Server should wins the conflict since it's the default behavior
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Conflict_DC_US_ClientShouldWins(SyncOptions options)
        {
            // create a server schema without seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // Execute a sync on all clients and check results
            // Each client will upload its own deleted row (conflicting)
            // then download the updated row from server 
            foreach (var client in Clients)
            {
                var productId = await Generate_DC_US_Conflict(client, options);
                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                agent.Options.ConflictResolutionPolicy = ConflictResolutionPolicy.ClientWins;

                var s = await agent.SynchronizeAsync(Tables);

                Assert.Equal(0, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalResolvedConflicts);

                await CheckProductCategoryRows(client, "CLI");
            }


        }

        /// <summary>
        /// Generate a conflict when inserting one row on server and the same row on each client
        /// Server should wins the conflict since it's the default behavior
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Conflict_DC_US_ClientShouldWins_CozHandler(SyncOptions options)
        {
            // create a server schema without seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // Execute a sync on all clients and check results
            // Each client will upload its own deleted row (conflicting)
            // then download the updated row from server 
            foreach (var client in Clients)
            {
                var productId = await Generate_DC_US_Conflict(client, options);
                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                agent.Options.ConflictResolutionPolicy = ConflictResolutionPolicy.ClientWins;

                var localOrchestrator = agent.LocalOrchestrator;
                var remoteOrchestrator = agent.RemoteOrchestrator;

                // From client : Remote is server, Local is client
                localOrchestrator.OnApplyChangesConflictOccured(acf =>
                {
                    // Since we have a ClientWins resolution,
                    // We should NOT have any conflict raised on the client side
                    // Since the conflict has been resolver on server
                    // And Server forces applied the client row
                    // So far the client row is good and should not raise any conflict

                    throw new Exception("Should not happen !!");
                });

                // From Server : Remote is client, Local is server
                remoteOrchestrator.OnApplyChangesConflictOccured(async acf =>
                {
                    // Check conflict is correctly set
                    var conflict = await acf.GetSyncConflictAsync();
                    var localRow = conflict.LocalRow;
                    var remoteRow = conflict.RemoteRow;

                    Assert.Equal(SyncRowState.Deleted, conflict.RemoteRow.RowState);
                    Assert.Equal(SyncRowState.Modified, conflict.LocalRow.RowState);

                    Assert.Equal(ConflictResolution.ClientWins, acf.Resolution);
                    Assert.Equal(ConflictType.RemoteIsDeletedLocalExists, conflict.Type);

                    acf.Resolution = ConflictResolution.ClientWins;
                });

                var s = await agent.SynchronizeAsync(Tables);

                Assert.Equal(0, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalResolvedConflicts);

                await CheckProductCategoryRows(client, "CLI");
            }


        }

        /// <summary>
        /// Generate a conflict when inserting one row on server and the same row on each client
        /// Server should wins the conflict since it's the default behavior
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Conflict_DC_US_ServerShouldWins(SyncOptions options)
        {
            // create a server schema without seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            foreach (var client in Clients)
            {
                await Generate_DC_US_Conflict(client, options);
                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                var s = await agent.SynchronizeAsync(Tables);

                Assert.Equal(1, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalResolvedConflicts);

                await CheckProductCategoryRows(client, "SRV");
            }


        }

        /// <summary>
        /// Generate a conflict when inserting one row on server and the same row on each client
        /// Server should wins the conflict since it's the default behavior
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Conflict_DC_US_ServerShouldWins_CozHandler(SyncOptions options)
        {
            // create a server schema without seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            foreach (var client in Clients)
            {
                await Generate_DC_US_Conflict(client, options);
                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                var localOrchestrator = agent.LocalOrchestrator;
                var remoteOrchestrator = agent.RemoteOrchestrator;

                // From client : Remote is server, Local is client
                localOrchestrator.OnApplyChangesConflictOccured(async acf =>
                {
                    // Check conflict is correctly set
                    var conflict = await acf.GetSyncConflictAsync();
                    var localRow = conflict.LocalRow;
                    var remoteRow = conflict.RemoteRow;

                    Assert.Equal(SyncRowState.Modified, conflict.RemoteRow.RowState);
                    Assert.Equal(SyncRowState.Deleted, conflict.LocalRow.RowState);

                    Assert.Equal(ConflictResolution.ClientWins, acf.Resolution);
                    Assert.Equal(ConflictType.RemoteExistsLocalIsDeleted, conflict.Type);
                });

                // From Server : Remote is client, Local is server
                remoteOrchestrator.OnApplyChangesConflictOccured(async acf =>
                {
                    // Check conflict is correctly set
                    var conflict = await acf.GetSyncConflictAsync();
                    var localRow = conflict.LocalRow;
                    var remoteRow = conflict.RemoteRow;

                    Assert.Equal(SyncRowState.Deleted, conflict.RemoteRow.RowState);
                    Assert.Equal(SyncRowState.Modified, conflict.LocalRow.RowState);

                    Assert.Equal(ConflictResolution.ServerWins, acf.Resolution);
                    Assert.Equal(ConflictType.RemoteIsDeletedLocalExists, conflict.Type);
                });

                var s = await agent.SynchronizeAsync(Tables);

                Assert.Equal(1, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalResolvedConflicts);

                await CheckProductCategoryRows(client, "SRV");
            }


        }



        // ------------------------------------------------------------------------
        // Update Client When Outdated
        // ------------------------------------------------------------------------

        /// <summary>
        /// Generate an outdated conflict. Both lines exists on both side but server has cleaned metadatas
        /// </summary>
        private async Task Generate_UC_OUTDATED_Conflict((string DatabaseName, ProviderType ProviderType, CoreProvider Provider) client, SyncOptions options)
        {
            // create empty client databases
            await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            var setup = new SyncSetup(Tables);

            // coz of ProductCategory Parent Id Foreign Key Constraints
            // on Reset table in MySql
            options.DisableConstraintsOnApplyChanges = true;

            // Execute a sync to initialize client and server schema 
            var agent = new SyncAgent(client.Provider, Server.Provider, options);

            if (options.TransactionMode != TransactionMode.AllOrNothing && (client.ProviderType == ProviderType.MySql || client.ProviderType == ProviderType.MariaDB))
            {
                agent.LocalOrchestrator.OnGetCommand(async args =>
                {
                    if (args.CommandType == DbCommandType.Reset)
                    {
                        var scopeInfo = await agent.LocalOrchestrator.GetScopeInfoAsync(args.Connection, args.Transaction);
                        await agent.LocalOrchestrator.DisableConstraintsAsync(scopeInfo, args.Table.TableName, args.Table.SchemaName, args.Connection, args.Transaction);
                    }
                });
            }

            // Since we may have an Outdated situation due to previous client, go for a Reinitialize sync type
            await agent.SynchronizeAsync(setup, SyncType.Reinitialize);

            // Insert the conflict product category on each client
            var productId = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 6);
            var productCategoryNameClient = HelperDatabase.GetRandomName("CLI");

            using (var ctx = new AdventureWorksContext(client, this.UseFallbackSchema))
            {
                ctx.Add(new ProductCategory
                {
                    ProductCategoryId = productId,
                    Name = productCategoryNameClient
                });
                await ctx.SaveChangesAsync();
            }

            // Since we may have an Outdated situation due to previous client, go for a Reinitialize sync type
            var s = await agent.SynchronizeAsync(SyncType.ReinitializeWithUpload);

            // Generation of an outdated mark on the server
            var serverOrchestrator = new RemoteOrchestrator(Server.Provider, options);
            var ts = await serverOrchestrator.GetLocalTimestampAsync();
            await serverOrchestrator.DeleteMetadatasAsync(ts + 1);

        }

        /// <summary>
        /// Generate a conflict when inserting one row on client and server then server purged metadata.
        /// Should have an outdated situation, resolved by a reinitialize action
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Conflict_UC_OUTDATED_ServerShouldWins(SyncOptions options)
        {
            // create a server schema without seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            var cpt = 1;
            foreach (var client in Clients)
            {
                await Generate_UC_OUTDATED_Conflict(client, options);

                var setup = new SyncSetup(Tables);

                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                var localOrchestrator = agent.LocalOrchestrator;
                var remoteOrchestrator = agent.RemoteOrchestrator;

                // From client : Remote is server, Local is client
                localOrchestrator.OnApplyChangesConflictOccured(acf =>
                {
                    throw new Exception("Should not happen since we are reinitializing");

                });

                // From Server : Remote is client, Local is server
                remoteOrchestrator.OnApplyChangesConflictOccured(acf =>
                {
                    throw new Exception("Should not happen since we are reinitializing");
                });

                localOrchestrator.OnOutdated(oa =>
                {
                    oa.Action = OutdatedAction.ReinitializeWithUpload;
                });

                var s = await agent.SynchronizeAsync(setup);

                Assert.Equal(cpt, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);

                await CheckProductCategoryRows(client);

                cpt++;
            }

        }

        /// <summary>
        /// Generate a conflict when inserting one row on client and server then server purged metadata.
        /// Should have an outdated situation, resolved by a reinitialize action
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Conflict_UC_OUTDATED_ServerShouldWins_EvenIf_ResolutionIsClientWins(SyncOptions options)
        {

            // create a server schema without seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            var cpt = 1;
            foreach (var client in Clients)
            {
                await Generate_UC_OUTDATED_Conflict(client, options);

                var setup = new SyncSetup(Tables);

                options.ConflictResolutionPolicy = ConflictResolutionPolicy.ClientWins;
                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                var localOrchestrator = agent.LocalOrchestrator;
                var remoteOrchestrator = agent.RemoteOrchestrator;

                // From client : Remote is server, Local is client
                localOrchestrator.OnApplyChangesConflictOccured(acf =>
                {
                    throw new Exception("Should not happen since we are reinitializing");
                });

                // From Server : Remote is client, Local is server
                remoteOrchestrator.OnApplyChangesConflictOccured(acf =>
                {
                    throw new Exception("Should not happen since we are reinitializing");
                });

                localOrchestrator.OnOutdated(oa =>
                {
                    oa.Action = OutdatedAction.ReinitializeWithUpload;
                });

                var s = await agent.SynchronizeAsync(setup);

                Assert.Equal(cpt, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);

                await CheckProductCategoryRows(client);
                cpt++;

            }

        }




        // ------------------------------------------------------------------------
        // Update Client - Delete Server
        // ------------------------------------------------------------------------


        /// <summary>
        /// Generate an update on the client and delete on the server; will be resolved as:
        /// - RemoteExistsLocalIsDeleted from the server side POV
        /// - RemoteIsDeletedLocalExists from the client side POV
        /// </summary>
        /// <param name="options"></param>
        /// <returns></returns>
        private async Task<string> Generate_UC_DS_Conflict((string DatabaseName, ProviderType ProviderType, CoreProvider Provider) client, SyncOptions options)
        {
            var productId = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 6);
            var productCategoryName = HelperDatabase.GetRandomName("CLI");
            var productCategoryNameUpdated = HelperDatabase.GetRandomName("CLI_UPDATED");

            // create empty client database
            await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // Insert a product category and sync it on all clients
            using (var ctx = new AdventureWorksContext(this.Server))
            {
                ctx.Add(new ProductCategory { ProductCategoryId = productId, Name = productCategoryName });
                await ctx.SaveChangesAsync();
            }

            // Execute a sync to initialize client and server schema 
            await new SyncAgent(client.Provider, Server.Provider, options).SynchronizeAsync(Tables);

            // Update product category on each client
            using (var ctx = new AdventureWorksContext(client, this.UseFallbackSchema))
            {
                var pcupdated = ctx.ProductCategory.Single(pc => pc.ProductCategoryId == productId);
                pcupdated.Name = productCategoryNameUpdated;
                await ctx.SaveChangesAsync();
            }

            // Delete on Server
            using (var ctx = new AdventureWorksContext(this.Server))
            {
                var pcdel = ctx.ProductCategory.Single(pc => pc.ProductCategoryId == productId);
                ctx.ProductCategory.Remove(pcdel);
                await ctx.SaveChangesAsync();
            }

            return productId;
        }

        /// <summary>
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Conflict_UC_DS_ServerShouldWins(SyncOptions options)
        {
            // create a server schema without seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            foreach (var client in Clients)
            {
                await Generate_UC_DS_Conflict(client, options);

                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                var s = await agent.SynchronizeAsync(Tables);

                Assert.Equal(1, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalResolvedConflicts);

                await CheckProductCategoryRows(client);
            }

        }

        /// <summary>
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Conflict_UC_DS_ServerShouldWins_CozHandler(SyncOptions options)
        {
            // create a server schema without seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            foreach (var client in Clients)
            {
                await Generate_UC_DS_Conflict(client, options);

                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                var localOrchestrator = agent.LocalOrchestrator;
                var remoteOrchestrator = agent.RemoteOrchestrator;

                // From client : Remote is server, Local is client
                localOrchestrator.OnApplyChangesConflictOccured(async acf =>
                {
                    // Check conflict is correctly set
                    var conflict = await acf.GetSyncConflictAsync();
                    var localRow = conflict.LocalRow;
                    var remoteRow = conflict.RemoteRow;

                    // remote is server; local is client
                    Assert.StartsWith("CLI_UPDATED", localRow["Name"].ToString());

                    Assert.Equal(SyncRowState.Deleted, conflict.RemoteRow.RowState);
                    Assert.Equal(SyncRowState.Modified, conflict.LocalRow.RowState);

                    // The conflict resolution is always the opposite from the one configured by options
                    Assert.Equal(ConflictResolution.ClientWins, acf.Resolution);
                    Assert.Equal(ConflictType.RemoteIsDeletedLocalExists, conflict.Type);
                });

                // From Server : Remote is client, Local is server
                remoteOrchestrator.OnApplyChangesConflictOccured(async acf =>
                {
                    // Check conflict is correctly set
                    var conflict = await acf.GetSyncConflictAsync();
                    var localRow = conflict.LocalRow;
                    var remoteRow = conflict.RemoteRow;

                    // remote is client; local is server
                    Assert.StartsWith("CLI_UPDATED", remoteRow["Name"].ToString());

                    Assert.Equal(SyncRowState.Modified, conflict.RemoteRow.RowState);
                    Assert.Equal(SyncRowState.Deleted, conflict.LocalRow.RowState);

                    Assert.Equal(ConflictResolution.ServerWins, acf.Resolution);
                    Assert.Equal(ConflictType.RemoteExistsLocalIsDeleted, conflict.Type);
                });

                var s = await agent.SynchronizeAsync(Tables);

                Assert.Equal(1, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalResolvedConflicts);

                await CheckProductCategoryRows(client);
            }

        }

        /// <summary>
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Conflict_UC_DS_ClientShouldWins(SyncOptions options)
        {
            // create a server schema without seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            foreach (var client in Clients)
            {
                var productCategoryId = await Generate_UC_DS_Conflict(client, options);

                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                // Resolution is set to client side
                options.ConflictResolutionPolicy = ConflictResolutionPolicy.ClientWins;

                var s = await agent.SynchronizeAsync(Tables);

                Assert.Equal(0, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalResolvedConflicts);

                await CheckProductCategoryRows(client);
            }

        }


        /// <summary>
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Conflict_UC_DS_ClientShouldWins_CozHandler(SyncOptions options)
        {
            // create a server schema without seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            foreach (var client in Clients)
            {
                var productCategoryId = await Generate_UC_DS_Conflict(client, options);

                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                // Resolution is set to client side
                options.ConflictResolutionPolicy = ConflictResolutionPolicy.ClientWins;

                var localOrchestrator = agent.LocalOrchestrator;
                var remoteOrchestrator = agent.RemoteOrchestrator;

                // From client : Remote is server, Local is client
                localOrchestrator.OnApplyChangesConflictOccured(acf =>
                {
                    throw new Exception("Should not happen since Client is the winner of the conflict and conflict has been resolved on the server side");
                });

                // From Server : Remote is client, Local is server
                remoteOrchestrator.OnApplyChangesConflictOccured(async acf =>
                {
                    // Check conflict is correctly set
                    var conflict = await acf.GetSyncConflictAsync();
                    var localRow = conflict.LocalRow;
                    var remoteRow = conflict.RemoteRow;

                    // remote is client; local is server
                    Assert.StartsWith("CLI_UPDATED", remoteRow["Name"].ToString());

                    Assert.Equal(SyncRowState.Modified, conflict.RemoteRow.RowState);
                    Assert.Equal(SyncRowState.Deleted, conflict.LocalRow.RowState);

                    Assert.Equal(ConflictResolution.ClientWins, acf.Resolution);
                    Assert.Equal(ConflictType.RemoteExistsLocalIsDeleted, conflict.Type);
                });

                var s = await agent.SynchronizeAsync(Tables);

                Assert.Equal(0, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalResolvedConflicts);

                await CheckProductCategoryRows(client);
            }

        }


        // ------------------------------------------------------------------------
        // Delete Client - Delete Server
        // ------------------------------------------------------------------------


        /// <summary>
        /// Generate a deleted row on the server and on the client, it's resolved as:
        /// - RemoteIsDeletedLocalIsDeleted from both side POV
        /// </summary>
        private async Task Generate_DC_DS_Conflict((string DatabaseName, ProviderType ProviderType, CoreProvider Provider) client, SyncOptions options)
        {
            var productId = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 6);
            var productCategoryName = HelperDatabase.GetRandomName("CLI");
            var productCategoryNameUpdated = HelperDatabase.GetRandomName("SRV");

            // create empty client database
            await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // Insert a product category and sync it on all clients
            using (var ctx = new AdventureWorksContext(this.Server))
            {
                ctx.Add(new ProductCategory { ProductCategoryId = productId, Name = productCategoryName });
                await ctx.SaveChangesAsync();
            }

            // Execute a sync to initialize client and server schema 
            await new SyncAgent(client.Provider, Server.Provider, options).SynchronizeAsync(Tables);

            // Delete product category 
            using (var ctx = new AdventureWorksContext(client, this.UseFallbackSchema))
            {
                var pcdel = ctx.ProductCategory.Single(pc => pc.ProductCategoryId == productId);
                ctx.ProductCategory.Remove(pcdel);
                await ctx.SaveChangesAsync();
            }

            // Delete on Server
            using (var ctx = new AdventureWorksContext(this.Server))
            {
                var pcdel = ctx.ProductCategory.Single(pc => pc.ProductCategoryId == productId);
                ctx.ProductCategory.Remove(pcdel);
                await ctx.SaveChangesAsync();
            }
        }

        /// <summary>
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Conflict_DC_DS_ServerShouldWins(SyncOptions options)
        {
            // create a server schema without seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            foreach (var client in Clients)
            {
                await Generate_DC_DS_Conflict(client, options);

                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                var s = await agent.SynchronizeAsync(Tables);

                Assert.Equal(1, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalResolvedConflicts);

                await CheckProductCategoryRows(client);
            }

        }

        /// <summary>
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Conflict_DC_DS_ServerShouldWins_CozHandler(SyncOptions options)
        {
            // create a server schema without seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            foreach (var client in Clients)
            {
                await Generate_DC_DS_Conflict(client, options);

                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                var localOrchestrator = agent.LocalOrchestrator;
                var remoteOrchestrator = agent.RemoteOrchestrator;

                // From client : Remote is server, Local is client
                localOrchestrator.OnApplyChangesConflictOccured(async acf =>
                {
                    var conflict = await acf.GetSyncConflictAsync();
                    // Check conflict is correctly set
                    var localRow = conflict.LocalRow;
                    var remoteRow = conflict.RemoteRow;

                    Assert.Equal(SyncRowState.Deleted, conflict.RemoteRow.RowState);
                    Assert.Equal(SyncRowState.Deleted, conflict.LocalRow.RowState);

                    // The conflict resolution is always the opposite from the one configured by options
                    Assert.Equal(ConflictResolution.ClientWins, acf.Resolution);
                    Assert.Equal(ConflictType.RemoteIsDeletedLocalIsDeleted, conflict.Type);
                });

                // From Server : Remote is client, Local is server
                remoteOrchestrator.OnApplyChangesConflictOccured(async acf =>
                {
                    // Check conflict is correctly set
                    var conflict = await acf.GetSyncConflictAsync();
                    var localRow = conflict.LocalRow;
                    var remoteRow = conflict.RemoteRow;

                    Assert.Equal(SyncRowState.Deleted, conflict.RemoteRow.RowState);
                    Assert.Equal(SyncRowState.Deleted, conflict.LocalRow.RowState);

                    Assert.Equal(ConflictResolution.ServerWins, acf.Resolution);
                    Assert.Equal(ConflictType.RemoteIsDeletedLocalIsDeleted, conflict.Type);
                });

                var s = await agent.SynchronizeAsync(Tables);

                Assert.Equal(1, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalResolvedConflicts);

                await CheckProductCategoryRows(client);
            }

        }

        /// <summary>
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Conflict_DC_DS_ClientShouldWins(SyncOptions options)
        {
            // create a server schema without seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            foreach (var client in Clients)
            {
                await Generate_DC_DS_Conflict(client, options);

                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                options.ConflictResolutionPolicy = ConflictResolutionPolicy.ClientWins;

                var s = await agent.SynchronizeAsync(Tables);

                Assert.Equal(0, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalResolvedConflicts);

                await CheckProductCategoryRows(client);
            }

        }


        /// <summary>
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Conflict_DC_DS_ClientShouldWins_CozHandler(SyncOptions options)
        {
            // create a server schema without seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            foreach (var client in Clients)
            {
                await Generate_DC_DS_Conflict(client, options);

                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                options.ConflictResolutionPolicy = ConflictResolutionPolicy.ClientWins;

                var localOrchestrator = agent.LocalOrchestrator;
                var remoteOrchestrator = agent.RemoteOrchestrator;

                // From client : Remote is server, Local is client
                localOrchestrator.OnApplyChangesConflictOccured(acf =>
                {
                    throw new Exception("Should not happen since Client is the winner of the conflict and conflict has been resolved on the server side");
                });

                // From Server : Remote is client, Local is server
                remoteOrchestrator.OnApplyChangesConflictOccured(async acf =>
                {
                    // Check conflict is correctly set
                    var conflict = await acf.GetSyncConflictAsync();
                    var localRow = conflict.LocalRow;
                    var remoteRow = conflict.RemoteRow;

                    Assert.Equal(SyncRowState.Deleted, conflict.RemoteRow.RowState);
                    Assert.Equal(SyncRowState.Deleted, conflict.LocalRow.RowState);

                    Assert.Equal(ConflictResolution.ClientWins, acf.Resolution);
                    Assert.Equal(ConflictType.RemoteIsDeletedLocalIsDeleted, conflict.Type);
                });

                var s = await agent.SynchronizeAsync(Tables);

                Assert.Equal(0, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalResolvedConflicts);

                await CheckProductCategoryRows(client);
            }

        }


        // ------------------------------------------------------------------------
        // Delete Client - Not Exists Server
        // ------------------------------------------------------------------------

        /// <summary>
        /// Generate a deleted row on client, that does not exists on server, it's resolved as:
        ///  - RemoteIsDeletedLocalNotExists from the Server POV 
        ///  - RemoteNotExistsLocalIsDeleted from the Client POV, but it can't happen
        /// </summary>
        private async Task Generate_DC_NULLS_Conflict((string DatabaseName, ProviderType ProviderType, CoreProvider Provider) client, SyncOptions options)
        {
            var productId = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 6);
            var productCategoryName = HelperDatabase.GetRandomName("CLI");
            var productCategoryNameUpdated = HelperDatabase.GetRandomName("SRV");

            // create empty client database
            await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // Execute a sync on all clients to initialize client and server schema 
            await new SyncAgent(client.Provider, Server.Provider, options).SynchronizeAsync(Tables);


            // Insert a product category on all clients
            using (var ctx = new AdventureWorksContext(client, this.UseFallbackSchema))
            {
                ctx.Add(new ProductCategory { ProductCategoryId = productId, Name = productCategoryName });
                await ctx.SaveChangesAsync();
            }

            // Then delete it
            using (var ctx = new AdventureWorksContext(client, this.UseFallbackSchema))
            {
                var pcdel = ctx.ProductCategory.Single(pc => pc.ProductCategoryId == productId);
                ctx.ProductCategory.Remove(pcdel);
                await ctx.SaveChangesAsync();
            }

            // So far we have a row marked as deleted in the tracking table.
        }

        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Conflict_DC_NULLS_ServerShouldWins(SyncOptions options)
        {
            // create a server schema without seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            foreach (var client in Clients)
            {
                await Generate_DC_NULLS_Conflict(client, options);

                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                var s = await agent.SynchronizeAsync(Tables);

                Assert.Equal(0, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalResolvedConflicts);

                await CheckProductCategoryRows(client);
            }

        }

        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Conflict_DC_NULLS_ServerShouldWins_CozHandler(SyncOptions options)
        {
            // create a server schema without seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            foreach (var client in Clients)
            {
                await Generate_DC_NULLS_Conflict(client, options);

                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                var localOrchestrator = agent.LocalOrchestrator;
                var remoteOrchestrator = agent.RemoteOrchestrator;

                // From client : Remote is server, Local is client
                localOrchestrator.OnApplyChangesConflictOccured(acf =>
                {
                    throw new Exception("Even if it's a server win here, the server should not send back anything, since he has anything related to this line in its metadatas");
                });

                // From Server : Remote is client, Local is server
                remoteOrchestrator.OnApplyChangesConflictOccured(async acf =>
                {
                    // Check conflict is correctly set
                    var conflict = await acf.GetSyncConflictAsync();
                    var localRow = conflict.LocalRow;
                    var remoteRow = conflict.RemoteRow;

                    Assert.Equal(ConflictResolution.ServerWins, acf.Resolution);
                    Assert.Equal(ConflictType.RemoteIsDeletedLocalNotExists, conflict.Type);
                    Assert.Equal(SyncRowState.Deleted, conflict.RemoteRow.RowState);
                    Assert.Null(localRow);

                });

                var s = await agent.SynchronizeAsync(Tables);

                Assert.Equal(0, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalResolvedConflicts);

                await CheckProductCategoryRows(client);
            }

        }

        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Conflict_DC_NULLS_ClientShouldWins(SyncOptions options)
        {
            // create a server schema without seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            foreach (var client in Clients)
            {
                await Generate_DC_NULLS_Conflict(client, options);

                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                // Set conflict resolution to client
                options.ConflictResolutionPolicy = ConflictResolutionPolicy.ClientWins;

                var s = await agent.SynchronizeAsync(Tables);

                Assert.Equal(0, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalResolvedConflicts);

                await CheckProductCategoryRows(client);
            }

        }


        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Conflict_DC_NULLS_ClientShouldWins_CozHandler(SyncOptions options)
        {
            // create a server schema without seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            foreach (var client in Clients)
            {
                await Generate_DC_NULLS_Conflict(client, options);

                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                // Set conflict resolution to client
                options.ConflictResolutionPolicy = ConflictResolutionPolicy.ClientWins;

                var localOrchestrator = agent.LocalOrchestrator;
                var remoteOrchestrator = agent.RemoteOrchestrator;

                // From client : Remote is server, Local is client
                localOrchestrator.OnApplyChangesConflictOccured(acf =>
                {
                    throw new Exception("Should not happen since Client is the winner of the conflict and conflict has been resolved on the server side");
                });

                // From Server : Remote is client, Local is server
                remoteOrchestrator.OnApplyChangesConflictOccured(async acf =>
                {
                    // Check conflict is correctly set
                    var conflict = await acf.GetSyncConflictAsync();
                    var localRow = conflict.LocalRow;
                    var remoteRow = conflict.RemoteRow;

                    Assert.Equal(ConflictResolution.ClientWins, acf.Resolution);
                    Assert.Equal(ConflictType.RemoteIsDeletedLocalNotExists, conflict.Type);
                    Assert.Equal(SyncRowState.Deleted, conflict.RemoteRow.RowState);
                    Assert.Null(localRow);

                });

                var s = await agent.SynchronizeAsync(Tables);

                Assert.Equal(0, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalResolvedConflicts);

                await CheckProductCategoryRows(client);
            }

        }

        /// <summary>
        /// Generate a deleted row on Server, that does not exists on Client, it's resolved as:
        /// </summary>
        private async Task Generate_NULLC_DS_Conflict((string DatabaseName, ProviderType ProviderType, CoreProvider Provider) client, SyncOptions options)
        {
            var productId = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 6);
            var productCategoryName = HelperDatabase.GetRandomName("CLI");
            var productCategoryNameUpdated = HelperDatabase.GetRandomName("SRV");

            // create empty client database
            await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // Execute a sync on all clients to initialize client and server schema 
            await new SyncAgent(client.Provider, Server.Provider, options).SynchronizeAsync(Tables);

            // Insert a product category on server
            using (var ctx = new AdventureWorksContext(this.Server, this.UseFallbackSchema))
            {
                ctx.Add(new ProductCategory { ProductCategoryId = productId, Name = productCategoryName });
                await ctx.SaveChangesAsync();
            }

            // Then delete it
            using (var ctx = new AdventureWorksContext(this.Server, this.UseFallbackSchema))
            {
                var pcdel = ctx.ProductCategory.Single(pc => pc.ProductCategoryId == productId);
                ctx.ProductCategory.Remove(pcdel);
                await ctx.SaveChangesAsync();
            }

            // So far we have a row marked as deleted in the tracking table.
        }


        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Conflict_NULLC_DS_ServerShouldWins(SyncOptions options)
        {
            // create a server schema without seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            foreach (var client in Clients)
            {
                await Generate_NULLC_DS_Conflict(client, options);

                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                var s = await agent.SynchronizeAsync(Tables);

                Assert.Equal(1, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalChangesAppliedOnClient);
                Assert.Equal(1, s.TotalResolvedConflicts);

                await CheckProductCategoryRows(client);
            }

        }

        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Conflict_NULLC_DS_ServerShouldWins_CozHandler(SyncOptions options)
        {
            // create a server schema without seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            foreach (var client in Clients)
            {
                await Generate_NULLC_DS_Conflict(client, options);

                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                var localOrchestrator = agent.LocalOrchestrator;
                var remoteOrchestrator = agent.RemoteOrchestrator;

                // From client : Remote is server, Local is client
                localOrchestrator.OnApplyChangesConflictOccured(async acf =>
                {
                    // Check conflict is correctly set
                    var conflict = await acf.GetSyncConflictAsync();
                    var localRow = conflict.LocalRow;
                    var remoteRow = conflict.RemoteRow;

                    Assert.Equal(ConflictResolution.ClientWins, acf.Resolution);
                    Assert.Equal(ConflictType.RemoteIsDeletedLocalNotExists, conflict.Type);
                    Assert.Equal(SyncRowState.Deleted, conflict.RemoteRow.RowState);
                    Assert.Null(localRow);
                });

                // From Server : Remote is client, Local is server
                remoteOrchestrator.OnApplyChangesConflictOccured(acf =>
                {
                    throw new Exception("Should not happen since since client did not sent anything. SO far server will send back the deleted row as standard batch row");
                });

                var s = await agent.SynchronizeAsync(Tables);

                Assert.Equal(1, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalChangesAppliedOnClient);
                Assert.Equal(1, s.TotalResolvedConflicts);

                await CheckProductCategoryRows(client);
            }

        }

        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Conflict_NULLC_DS_ClientShouldWins(SyncOptions options)
        {
            // create a server schema without seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);


            foreach (var client in Clients)
            {
                await Generate_NULLC_DS_Conflict(client, options);

                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                // Set conflict resolution to client
                options.ConflictResolutionPolicy = ConflictResolutionPolicy.ClientWins;

                var s = await agent.SynchronizeAsync(Tables);

                Assert.Equal(1, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalChangesAppliedOnClient);
                Assert.Equal(1, s.TotalResolvedConflicts);


                await CheckProductCategoryRows(client);
            }

        }


        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Conflict_NULLC_DS_ClientShouldWins_CozHandler(SyncOptions options)
        {
            // create a server schema without seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);


            foreach (var client in Clients)
            {
                await Generate_NULLC_DS_Conflict(client, options);

                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                // Set conflict resolution to client
                options.ConflictResolutionPolicy = ConflictResolutionPolicy.ClientWins;

                var localOrchestrator = agent.LocalOrchestrator;
                var remoteOrchestrator = agent.RemoteOrchestrator;

                // From client : Remote is server, Local is client
                localOrchestrator.OnApplyChangesConflictOccured(async acf =>
                {
                    // Check conflict is correctly set
                    var conflict = await acf.GetSyncConflictAsync();
                    var localRow = conflict.LocalRow;
                    var remoteRow = conflict.RemoteRow;

                    Assert.Equal(ConflictResolution.ServerWins, acf.Resolution);
                    Assert.Equal(ConflictType.RemoteIsDeletedLocalNotExists, conflict.Type);
                    Assert.Equal(SyncRowState.Deleted, conflict.RemoteRow.RowState);
                    Assert.Null(localRow);
                });

                // From Server : Remote is client, Local is server
                remoteOrchestrator.OnApplyChangesConflictOccured(acf =>
                {
                    throw new Exception("Should not happen since since client did not sent anything. SO far server will send back the deleted row as standard batch row");
                });

                var s = await agent.SynchronizeAsync(Tables);

                Assert.Equal(1, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalChangesAppliedOnClient);
                Assert.Equal(1, s.TotalResolvedConflicts);


                await CheckProductCategoryRows(client);
            }

        }



        /// Generate a conflict when inserting one row on server and the same row on each client
        /// Server should wins the conflict because default behavior
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Conflict_UC_US_ClientChoosedTheWinner(SyncOptions options)
        {
            // create a server schema without seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // Execute a sync on all clients and check results
            // Each client will upload its row (conflicting)
            // then download the others client lines + conflict that should be ovewritten on client
            foreach (var client in Clients)
            {

                await Generate_UC_US_Conflict(client, options);

                var clientNameDecidedOnClientMachine = HelperDatabase.GetRandomName();

                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                var localOrchestrator = agent.LocalOrchestrator;
                var remoteOrchestrator = agent.RemoteOrchestrator;

                // From client : Remote is server, Local is client
                // From here, we are going to let the client decides who is the winner of the conflict
                localOrchestrator.OnApplyChangesConflictOccured(async acf =>
                {
                    // Check conflict is correctly set
                    var conflict = await acf.GetSyncConflictAsync();
                    var localRow = conflict.LocalRow;
                    var remoteRow = conflict.RemoteRow;

                    // remote is server; local is client
                    Assert.StartsWith("SRV", remoteRow["Name"].ToString());
                    Assert.StartsWith("CLI", localRow["Name"].ToString());

                    Assert.Equal(SyncRowState.Modified, conflict.RemoteRow.RowState);
                    Assert.Equal(SyncRowState.Modified, conflict.LocalRow.RowState);

                    // The conflict resolution is always the opposite from the one configured by options
                    Assert.Equal(ConflictResolution.ClientWins, acf.Resolution);
                    Assert.Equal(ConflictType.RemoteExistsLocalExists, conflict.Type);

                    // From that point, you can easily letting the client decides who is the winner
                    // You can do a merge or whatever
                    // Show a UI with the local / remote row and letting him decides what is the good row version
                    // for testing purpose; will just going to set name to some fancy UI_CLIENT... instead of CLI or SRV

                    // SHOW UI
                    // OH.... CLIENT DECIDED TO SET NAME TO /// clientNameDecidedOnClientMachine 

                    remoteRow["Name"] = clientNameDecidedOnClientMachine;
                    // Mandatory to override the winner registered in the tracking table
                    // Use with caution !
                    // To be sure the row will be marked as updated locally, the scope id should be set to null
                    acf.SenderScopeId = null;
                });

                // From Server : Remote is client, Local is server
                // From that point we do not do anything, letting the server to resolve the conflict and send back
                // the server row and client row conflicting to the client
                remoteOrchestrator.OnApplyChangesConflictOccured(async acf =>
                {
                    // Check conflict is correctly set
                    var conflict = await acf.GetSyncConflictAsync();
                    var localRow = conflict.LocalRow;
                    var remoteRow = conflict.RemoteRow;

                    // remote is client; local is server
                    Assert.StartsWith("CLI", remoteRow["Name"].ToString());
                    Assert.StartsWith("SRV", localRow["Name"].ToString());

                    Assert.Equal(SyncRowState.Modified, conflict.RemoteRow.RowState);
                    Assert.Equal(SyncRowState.Modified, conflict.LocalRow.RowState);

                    Assert.Equal(ConflictResolution.ServerWins, acf.Resolution);
                    Assert.Equal(ConflictType.RemoteExistsLocalExists, conflict.Type);
                });

                // First sync, we allow server to resolve the conflict and send back the result to client
                var s = await agent.SynchronizeAsync(Tables);

                Assert.Equal(1, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalResolvedConflicts);

                // From this point the Server row Name is "SRV...."
                // And the Client row NAME is "UI_CLIENT..."
                // Make a new sync to send "UI_CLIENT..." to Server

                s = await agent.SynchronizeAsync(Tables);


                Assert.Equal(0, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);

                // Check that the product category name has been correctly sended back to the server
                await CheckProductCategoryRows(client);

            }


        }

    }
}
