using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.MariaDB;
using Dotmim.Sync.MySql;
using Dotmim.Sync.PostgreSql;
using Dotmim.Sync.Sqlite;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.SqlServer.Manager;
using Dotmim.Sync.Tests.Core;
using Dotmim.Sync.Tests.Fixtures;
using Dotmim.Sync.Tests.Misc;
using Dotmim.Sync.Tests.Models;
using Dotmim.Sync.Web.Client;
using Dotmim.Sync.Web.Server;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Http;
using Microsoft.Data.SqlClient;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
#if NET6_0 || NET8_0 
using MySqlConnector;
#elif NETCOREAPP3_1
using MySql.Data.MySqlClient;
#endif

using Npgsql;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace Dotmim.Sync.Tests.IntegrationTests
{

    public abstract class TcpConflictsTests : DatabaseTest, IClassFixture<DatabaseServerFixture>, IDisposable
    {
        private CoreProvider serverProvider;
        private IEnumerable<CoreProvider> clientsProvider;
        private SyncSetup setup;

        public TcpConflictsTests(ITestOutputHelper output, DatabaseServerFixture fixture) : base(output, fixture)
        {
            serverProvider = GetServerProvider();
            clientsProvider = GetClientProviders();
            setup = GetSetup();
        }

        [Fact]
        public virtual async Task ErrorUniqueKeyOnSameTableRaiseAnError()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

            // make a first sync to init the two databases
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            // Adding two rows on server side, that are correct
            var str = HelperDatabase.GetRandomName().ToUpper()[..9];
            await serverProvider.AddProductCategoryAsync($"Z1{str}", name: $"Z1{str}");
            await serverProvider.AddProductCategoryAsync($"Z2{str}", name: $"Z2{str}");

            foreach (var clientProvider in clientsProvider)
            {
                var (clientProviderType, clientDatabaseName) = HelperDatabase.GetDatabaseType(clientProvider);

                // Get a random directory to be sure we are not conflicting with another test
                var directoryName = HelperDatabase.GetRandomName();
                options.BatchDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDirectory(), directoryName);
                // Enable constraints check
                options.DisableConstraintsOnApplyChanges = false;
                // Disable bulk operations to have the same results for SQL as others providers
                clientProvider.UseBulkOperations = false;

                var agent = new SyncAgent(clientProvider, serverProvider, options);

                // To generate a unique key constraint, will modify the batch part info on client just before load it.
                // Replacing $"Z1{str}" with $"Z2{str}" will generate a unique key constraint
                agent.LocalOrchestrator.OnBatchChangesApplying(args =>
                {
                    if (args.BatchPartInfo != null && args.State == SyncRowState.Modified && args.SchemaTable.TableName == "ProductCategory")
                    {
                        var fullPath = args.BatchInfo.GetBatchPartInfoFullPath(args.BatchPartInfo);

                        using var table = agent.LocalOrchestrator.LoadTableFromBatchPartInfo(fullPath);

                        foreach (var row in table.Rows)
                            if (row["ProductCategoryID"].ToString() == $"Z1{str}")
                                row["Name"] = $"Z2{str}";

                        agent.LocalOrchestrator.SaveTableToBatchPartInfoAsync(args.BatchInfo, args.BatchPartInfo, table);
                    }
                });


                var exc = await Assert.ThrowsAsync<SyncException>(() => agent.SynchronizeAsync(setup));
                Assert.NotNull(exc);

                var batchInfos = agent.LocalOrchestrator.LoadBatchInfos();
                Assert.Empty(batchInfos);
            }
        }

        [Fact]
        public virtual async Task ErrorUniqueKeyOnSameTableContinueOnError()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

            // make a first sync to init the two databases
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            // Adding two rows on server side, that are correct
            var str = HelperDatabase.GetRandomName().ToUpper()[..9];
            await serverProvider.AddProductCategoryAsync($"Z1{str}", name: $"Z1{str}");
            await serverProvider.AddProductCategoryAsync($"Z2{str}", name: $"Z2{str}");

            foreach (var clientProvider in clientsProvider)
            {
                // Get a random directory to be sure we are not conflicting with another test
                var directoryName = HelperDatabase.GetRandomName();
                options.BatchDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDirectory(), directoryName);
                // Enable constraints check
                options.DisableConstraintsOnApplyChanges = false;
                // Disable bulk operations to have the same results for SQL as others providers
                clientProvider.UseBulkOperations = false;

                var agent = new SyncAgent(clientProvider, serverProvider, options);

                // To generate a unique key constraint, will modify the batch part info on client just before load it.
                // Replacing $"Z1{str}" with $"Z2{str}" will generate a unique key constraint
                agent.LocalOrchestrator.OnBatchChangesApplying(args =>
                {
                    if (args.BatchPartInfo != null && args.State == SyncRowState.Modified && args.SchemaTable.TableName == "ProductCategory")
                    {
                        var fullPath = args.BatchInfo.GetBatchPartInfoFullPath(args.BatchPartInfo);

                        using var table = agent.LocalOrchestrator.LoadTableFromBatchPartInfo(fullPath);

                        foreach (var row in table.Rows)
                            if (row["ProductCategoryID"].ToString() == $"Z1{str}")
                                row["Name"] = $"Z2{str}";

                        agent.LocalOrchestrator.SaveTableToBatchPartInfoAsync(args.BatchInfo, args.BatchPartInfo, table);
                    }
                });

                agent.LocalOrchestrator.OnApplyChangesErrorOccured(args =>
                {
                    // Continue On Error
                    args.Resolution = ErrorResolution.ContinueOnError;
                    Assert.NotNull(args.Exception);
                    Assert.NotNull(args.ErrorRow);
                    Assert.NotNull(args.SchemaTable);
                    Assert.Equal(SyncRowState.Modified, args.ApplyType);
                });

                var s = await agent.SynchronizeAsync(setup);

                Assert.Equal(2, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalChangesAppliedOnServer);
                Assert.Equal(1, s.TotalChangesAppliedOnClient);
                Assert.Equal(1, s.TotalChangesFailedToApplyOnClient);
                Assert.Equal(0, s.TotalChangesFailedToApplyOnServer);
                Assert.Equal(0, s.TotalResolvedConflicts);

                var batchInfos = agent.LocalOrchestrator.LoadBatchInfos();

                Assert.NotNull(batchInfos);
                Assert.Single(batchInfos);
                Assert.Single(batchInfos[0].BatchPartsInfo);
                Assert.Contains("ERRORS", batchInfos[0].BatchPartsInfo[0].FileName);
                Assert.Equal("ProductCategory", batchInfos[0].BatchPartsInfo[0].TableName);

                var batchInfo = batchInfos[0];

                var syncTables = agent.LocalOrchestrator.LoadTablesFromBatchInfo(batchInfo);

                foreach (var syncTable in syncTables)
                {
                    Assert.Equal("ProductCategory", syncTable.TableName);
                    Assert.True(syncTable.HasRows);

                    Assert.Equal(SyncRowState.ApplyModifiedFailed, syncTable.Rows[0].RowState);
                }

                s = await agent.SynchronizeAsync(setup);

                Assert.Equal(0, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalChangesAppliedOnClient);
                Assert.Equal(0, s.TotalChangesAppliedOnServer);
                Assert.Equal(0, s.TotalChangesFailedToApplyOnClient);
                Assert.Equal(0, s.TotalChangesFailedToApplyOnServer);
                Assert.Equal(0, s.TotalResolvedConflicts);

                batchInfos = agent.LocalOrchestrator.LoadBatchInfos();

                Assert.NotNull(batchInfos);
                Assert.Single(batchInfos);
                Assert.Single(batchInfos[0].BatchPartsInfo);
                Assert.Contains("ERRORS", batchInfos[0].BatchPartsInfo[0].FileName);
                Assert.Equal("ProductCategory", batchInfos[0].BatchPartsInfo[0].TableName);

                batchInfo = batchInfos[0];

                syncTables = agent.LocalOrchestrator.LoadTablesFromBatchInfo(batchInfo);

                foreach (var syncTable in syncTables)
                {
                    Assert.Equal("ProductCategory", syncTable.TableName);
                    Assert.True(syncTable.HasRows);

                    Assert.Equal(SyncRowState.ApplyModifiedFailed, syncTable.Rows[0].RowState);
                }
            }
        }

        [Fact]
        public virtual async Task ErrorUniqueKeyOnSameTableRetryOneMoreTimeAndThrowOnError()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

            // make a first sync to init the two databases
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            // Adding two rows on server side, that are correct
            var str = HelperDatabase.GetRandomName().ToUpper()[..9];
            await serverProvider.AddProductCategoryAsync($"Z1{str}", name: $"Z1{str}");
            await serverProvider.AddProductCategoryAsync($"Z2{str}", name: $"Z2{str}");

            foreach (var clientProvider in clientsProvider)
            {
                // Get a random directory to be sure we are not conflicting with another test
                var directoryName = HelperDatabase.GetRandomName();
                options.BatchDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDirectory(), directoryName);
                // Enable constraints check
                options.DisableConstraintsOnApplyChanges = false;
                // Disable bulk operations to have the same results for SQL as others providers
                clientProvider.UseBulkOperations = false;

                var agent = new SyncAgent(clientProvider, serverProvider, options);

                // To generate a unique key constraint, will modify the batch part info on client just before load it.
                // Replacing $"Z1{str}" with $"Z2{str}" will generate a unique key constraint
                agent.LocalOrchestrator.OnBatchChangesApplying(args =>
                {
                    if (args.BatchPartInfo != null && args.State == SyncRowState.Modified && args.SchemaTable.TableName == "ProductCategory")
                    {
                        var fullPath = args.BatchInfo.GetBatchPartInfoFullPath(args.BatchPartInfo);

                        using var table = agent.LocalOrchestrator.LoadTableFromBatchPartInfo(fullPath);

                        foreach (var row in table.Rows)
                            if (row["ProductCategoryID"].ToString() == $"Z1{str}")
                                row["Name"] = $"Z2{str}";

                        agent.LocalOrchestrator.SaveTableToBatchPartInfoAsync(args.BatchInfo, args.BatchPartInfo, table);
                    }
                });

                agent.LocalOrchestrator.OnApplyChangesErrorOccured(args =>
                {
                    // Continue On Error
                    args.Resolution = ErrorResolution.RetryOneMoreTimeAndThrowOnError;
                    Assert.NotNull(args.Exception);
                    Assert.NotNull(args.ErrorRow);
                    Assert.NotNull(args.SchemaTable);
                    Assert.Equal(SyncRowState.Modified, args.ApplyType);
                });

                var exc = await Assert.ThrowsAsync<SyncException>(() => agent.SynchronizeAsync(setup));
                Assert.NotNull(exc);
                var batchInfos = agent.LocalOrchestrator.LoadBatchInfos();
                Assert.Empty(batchInfos);

                exc = await Assert.ThrowsAsync<SyncException>(() => agent.SynchronizeAsync(setup));
                Assert.NotNull(exc);
                batchInfos = agent.LocalOrchestrator.LoadBatchInfos();
                Assert.Empty(batchInfos);
            }
        }

        [Fact]
        public virtual async Task ErrorUniqueKeyOnSameTableRetryOneMoreTimeAndContinueOnError()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

            // make a first sync to init the two databases
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            // Adding two rows on server side, that are correct
            var str = HelperDatabase.GetRandomName().ToUpper()[..9];
            await serverProvider.AddProductCategoryAsync($"Z1{str}", name: $"Z1{str}");
            await serverProvider.AddProductCategoryAsync($"Z2{str}", name: $"Z2{str}");

            foreach (var clientProvider in clientsProvider)
            {
                // Get a random directory to be sure we are not conflicting with another test
                var directoryName = HelperDatabase.GetRandomName();
                options.BatchDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDirectory(), directoryName);
                // Enable constraints check
                options.DisableConstraintsOnApplyChanges = false;
                // Disable bulk operations to have the same results for SQL as others providers
                clientProvider.UseBulkOperations = false;

                var agent = new SyncAgent(clientProvider, serverProvider, options);

                // To generate a unique key constraint, will modify the batch part info on client just before load it.
                // Replacing $"Z1{str}" with $"Z2{str}" will generate a unique key constraint
                agent.LocalOrchestrator.OnBatchChangesApplying(args =>
                {
                    if (args.BatchPartInfo != null && args.State == SyncRowState.Modified && args.SchemaTable.TableName == "ProductCategory")
                    {
                        var fullPath = args.BatchInfo.GetBatchPartInfoFullPath(args.BatchPartInfo);

                        using var table = agent.LocalOrchestrator.LoadTableFromBatchPartInfo(fullPath);

                        foreach (var row in table.Rows)
                            if (row["ProductCategoryID"].ToString() == $"Z1{str}")
                                row["Name"] = $"Z2{str}";

                        agent.LocalOrchestrator.SaveTableToBatchPartInfoAsync(args.BatchInfo, args.BatchPartInfo, table);
                    }
                });


                agent.LocalOrchestrator.OnApplyChangesErrorOccured(args =>
                {
                    // Continue On Error
                    args.Resolution = ErrorResolution.RetryOneMoreTimeAndContinueOnError;
                    Assert.NotNull(args.Exception);
                    Assert.NotNull(args.ErrorRow);
                    Assert.NotNull(args.SchemaTable);
                    Assert.Equal(SyncRowState.Modified, args.ApplyType);
                });

                var s = await agent.SynchronizeAsync(setup);

                Assert.Equal(2, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalChangesAppliedOnServer);
                Assert.Equal(1, s.TotalChangesAppliedOnClient);
                Assert.Equal(1, s.TotalChangesFailedToApplyOnClient);
                Assert.Equal(0, s.TotalChangesFailedToApplyOnServer);
                Assert.Equal(0, s.TotalResolvedConflicts);

                var batchInfos = agent.LocalOrchestrator.LoadBatchInfos();

                Assert.NotNull(batchInfos);
                Assert.Single(batchInfos);
                Assert.Single(batchInfos[0].BatchPartsInfo);
                Assert.Contains("ERRORS", batchInfos[0].BatchPartsInfo[0].FileName);
                Assert.Equal("ProductCategory", batchInfos[0].BatchPartsInfo[0].TableName);

                var batchInfo = batchInfos[0];

                var syncTables = agent.LocalOrchestrator.LoadTablesFromBatchInfo(batchInfo);

                foreach (var syncTable in syncTables)
                {
                    Assert.Equal("ProductCategory", syncTable.TableName);
                    Assert.True(syncTable.HasRows);

                    Assert.Equal(SyncRowState.ApplyModifiedFailed, syncTable.Rows[0].RowState);
                }
            }
        }

        [Fact]
        public virtual async Task ErrorUniqueKeyOnSameTableRetryOnNextSync()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

            // make a first sync to init the two databases
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            // Adding two rows on server side, that are correct
            var str = HelperDatabase.GetRandomName().ToUpper()[..9];
            await serverProvider.AddProductCategoryAsync($"Z1{str}", name: $"Z1{str}");
            await serverProvider.AddProductCategoryAsync($"Z2{str}", name: $"Z2{str}");

            foreach (var clientProvider in clientsProvider)
            {
                // Get a random directory to be sure we are not conflicting with another test
                var directoryName = HelperDatabase.GetRandomName();
                options.BatchDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDirectory(), directoryName);
                // Enable constraints check
                options.DisableConstraintsOnApplyChanges = false;
                // Disable bulk operations to have the same results for SQL as others providers
                clientProvider.UseBulkOperations = false;

                var agent = new SyncAgent(clientProvider, serverProvider, options);

                // To generate a unique key constraint, will modify the batch part info on client just before load it.
                // Replacing $"Z1{str}" with $"Z2{str}" will generate a unique key constraint
                agent.LocalOrchestrator.OnBatchChangesApplying(args =>
                {
                    if (args.BatchPartInfo != null && args.State == SyncRowState.Modified && args.SchemaTable.TableName == "ProductCategory")
                    {
                        var fullPath = args.BatchInfo.GetBatchPartInfoFullPath(args.BatchPartInfo);

                        using var table = agent.LocalOrchestrator.LoadTableFromBatchPartInfo(fullPath);

                        foreach (var row in table.Rows)
                            if (row["ProductCategoryID"].ToString() == $"Z1{str}")
                                row["Name"] = $"Z2{str}";

                        agent.LocalOrchestrator.SaveTableToBatchPartInfoAsync(args.BatchInfo, args.BatchPartInfo, table);
                    }
                });

                agent.LocalOrchestrator.OnApplyChangesErrorOccured(args =>
                {
                    // Continue On Error
                    args.Resolution = ErrorResolution.RetryOnNextSync;
                    Assert.NotNull(args.Exception);
                    Assert.NotNull(args.ErrorRow);
                    Assert.NotNull(args.SchemaTable);
                    Assert.Equal(SyncRowState.Modified, args.ApplyType);
                });

                var s = await agent.SynchronizeAsync(setup);

                Assert.Equal(2, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalChangesAppliedOnServer);
                Assert.Equal(1, s.TotalChangesAppliedOnClient);
                Assert.Equal(1, s.TotalChangesFailedToApplyOnClient);
                Assert.Equal(0, s.TotalChangesFailedToApplyOnServer);
                Assert.Equal(0, s.TotalResolvedConflicts);

                var batchInfos = agent.LocalOrchestrator.LoadBatchInfos();

                Assert.NotNull(batchInfos);
                Assert.Single(batchInfos);
                Assert.Single(batchInfos[0].BatchPartsInfo);
                Assert.Contains("ERROR", batchInfos[0].BatchPartsInfo[0].FileName);
                Assert.Equal("ProductCategory", batchInfos[0].BatchPartsInfo[0].TableName);

                var batchInfo = batchInfos[0];

                var syncTables = agent.LocalOrchestrator.LoadTablesFromBatchInfo(batchInfo);

                foreach (var syncTable in syncTables)
                {
                    Assert.Equal("ProductCategory", syncTable.TableName);
                    Assert.True(syncTable.HasRows);

                    Assert.Equal(SyncRowState.RetryModifiedOnNextSync, syncTable.Rows[0].RowState);
                }

                s = await agent.SynchronizeAsync(setup);

                Assert.Equal(0, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalChangesAppliedOnServer);
                Assert.Equal(0, s.TotalChangesAppliedOnClient);
                Assert.Equal(1, s.TotalChangesFailedToApplyOnClient);
                Assert.Equal(0, s.TotalChangesFailedToApplyOnServer);
                Assert.Equal(0, s.TotalResolvedConflicts);

                batchInfos = agent.LocalOrchestrator.LoadBatchInfos();

                Assert.NotNull(batchInfos);
                Assert.Single(batchInfos);
                Assert.Single(batchInfos[0].BatchPartsInfo);
                Assert.Contains("ERROR", batchInfos[0].BatchPartsInfo[0].FileName);
                Assert.Equal("ProductCategory", batchInfos[0].BatchPartsInfo[0].TableName);

                batchInfo = batchInfos[0];

                syncTables = agent.LocalOrchestrator.LoadTablesFromBatchInfo(batchInfo);

                foreach (var syncTable in syncTables)
                {
                    Assert.Equal("ProductCategory", syncTable.TableName);
                    Assert.True(syncTable.HasRows);

                    Assert.Equal(SyncRowState.RetryModifiedOnNextSync, syncTable.Rows[0].RowState);
                }
            }
        }

        [Fact]
        public virtual async Task ErrorUniqueKeyOnSameTableRetryOnNextSyncWithResolve()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

            // make a first sync to init the two databases
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            // Adding two rows on server side, that are correct
            var str = HelperDatabase.GetRandomName().ToUpper()[..9];
            await serverProvider.AddProductCategoryAsync($"Z1{str}", name: $"Z1{str}");
            await serverProvider.AddProductCategoryAsync($"Z2{str}", name: $"Z2{str}");

            foreach (var clientProvider in clientsProvider)
            {
                // Get a random directory to be sure we are not conflicting with another test
                var directoryName = HelperDatabase.GetRandomName();
                options.BatchDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDirectory(), directoryName);
                // Enable constraints check
                options.DisableConstraintsOnApplyChanges = false;
                // Disable bulk operations to have the same results for SQL as others providers
                clientProvider.UseBulkOperations = false;

                var agent = new SyncAgent(clientProvider, serverProvider, options);

                // To generate a unique key constraint, will modify the batch part info on client just before load it.
                // Replacing $"Z1{str}" with $"Z2{str}" will generate a unique key constraint
                var interceptorId = agent.LocalOrchestrator.OnBatchChangesApplying(args =>
                {
                    if (args.BatchPartInfo != null && args.State == SyncRowState.Modified && args.SchemaTable.TableName == "ProductCategory")
                    {
                        var fullPath = args.BatchInfo.GetBatchPartInfoFullPath(args.BatchPartInfo);

                        using var table = agent.LocalOrchestrator.LoadTableFromBatchPartInfo(fullPath);

                        foreach (var row in table.Rows)
                            if (row["ProductCategoryID"].ToString() == $"Z2{str}")
                                row["Name"] = $"Z1{str}";

                        agent.LocalOrchestrator.SaveTableToBatchPartInfoAsync(args.BatchInfo, args.BatchPartInfo, table);
                    }
                });

                agent.LocalOrchestrator.OnApplyChangesErrorOccured(args =>
                {
                    // Continue On Error
                    args.Resolution = ErrorResolution.RetryOnNextSync;
                    Assert.NotNull(args.Exception);
                    Assert.NotNull(args.ErrorRow);
                    Assert.NotNull(args.SchemaTable);
                    Assert.Equal(SyncRowState.Modified, args.ApplyType);
                });

                var s = await agent.SynchronizeAsync(setup);

                Assert.Equal(2, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalChangesAppliedOnServer);
                Assert.Equal(1, s.TotalChangesAppliedOnClient);
                Assert.Equal(1, s.TotalChangesFailedToApplyOnClient);
                Assert.Equal(0, s.TotalChangesFailedToApplyOnServer);
                Assert.Equal(0, s.TotalResolvedConflicts);

                var batchInfos = agent.LocalOrchestrator.LoadBatchInfos();

                Assert.NotNull(batchInfos);
                Assert.Single(batchInfos);
                Assert.Single(batchInfos[0].BatchPartsInfo);
                Assert.Contains("ERROR", batchInfos[0].BatchPartsInfo[0].FileName);
                Assert.Equal("ProductCategory", batchInfos[0].BatchPartsInfo[0].TableName);

                var batchInfo = batchInfos[0];

                var syncTables = agent.LocalOrchestrator.LoadTablesFromBatchInfo(batchInfo);

                foreach (var syncTable in syncTables)
                {
                    Assert.Equal("ProductCategory", syncTable.TableName);
                    Assert.True(syncTable.HasRows);

                    Assert.Equal(SyncRowState.RetryModifiedOnNextSync, syncTable.Rows[0].RowState);
                }

                // To resolve the issue, just clear the interceptor
                agent.LocalOrchestrator.ClearInterceptors(interceptorId);

                // And then intercept again the error batch re applied to change the value
                agent.LocalOrchestrator.OnBatchChangesApplying(args =>
                {
                    if (args.BatchPartInfo != null && args.SchemaTable.TableName == "ProductCategory")
                    {
                        var fullPath = args.BatchInfo.GetBatchPartInfoFullPath(args.BatchPartInfo);

                        using var table = agent.LocalOrchestrator.LoadTableFromBatchPartInfo(fullPath);

                        foreach (var row in table.Rows)
                            if (row["ProductCategoryID"].ToString() == $"Z2{str}")
                                row["Name"] = $"Z2{str}";

                        agent.LocalOrchestrator.SaveTableToBatchPartInfoAsync(args.BatchInfo, args.BatchPartInfo, table);
                    }
                });

                s = await agent.SynchronizeAsync(setup);

                Assert.Equal(0, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalChangesAppliedOnServer);
                Assert.Equal(1, s.TotalChangesAppliedOnClient);
                Assert.Equal(0, s.TotalChangesFailedToApplyOnClient);
                Assert.Equal(0, s.TotalChangesFailedToApplyOnServer);
                Assert.Equal(0, s.TotalResolvedConflicts);

                batchInfos = agent.LocalOrchestrator.LoadBatchInfos();

                Assert.Empty(batchInfos);
            }
        }

        [Fact]
        public virtual async Task ErrorUniqueKeyOnSameTableRetryOnNextSyncThenResolveClientByDelete()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

            // make a first sync to init the two databases
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            var x = 1;
            var y = 2;
            foreach (var clientProvider in clientsProvider)
            {
                // reinit client
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

                // Adding two rows on server side, that are correct
                var str = HelperDatabase.GetRandomName().ToUpper()[..6];
                await serverProvider.AddProductCategoryAsync($"Z{x}{str}", name: $"Z{x}{str}");
                await serverProvider.AddProductCategoryAsync($"Z{y}{str}", name: $"Z{y}{str}");

                // Get a random directory to be sure we are not conflicting with another test
                var directoryName = HelperDatabase.GetRandomName();
                options.BatchDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDirectory(), directoryName);
                // Enable constraints check
                options.DisableConstraintsOnApplyChanges = false;
                // Disable bulk operations to have the same results for SQL as others providers
                clientProvider.UseBulkOperations = false;

                var agent = new SyncAgent(clientProvider, serverProvider, options);

                // To generate a unique key constraint, will modify the batch part info on client just before load it.
                // Replacing $"Z1{str}" with $"Z2{str}" will generate a unique key constraint
                var interceptorId = agent.LocalOrchestrator.OnBatchChangesApplying(args =>
                {
                    if (args.BatchPartInfo != null && args.State == SyncRowState.Modified && args.SchemaTable.TableName == "ProductCategory")
                    {
                        var fullPath = args.BatchInfo.GetBatchPartInfoFullPath(args.BatchPartInfo);

                        using var table = agent.LocalOrchestrator.LoadTableFromBatchPartInfo(fullPath);

                        foreach (var row in table.Rows)
                            if (row["ProductCategoryID"].ToString() == $"Z{y}{str}")
                                row["Name"] = $"Z{x}{str}";

                        agent.LocalOrchestrator.SaveTableToBatchPartInfoAsync(args.BatchInfo, args.BatchPartInfo, table);
                    }
                });

                agent.LocalOrchestrator.OnApplyChangesErrorOccured(args =>
                {
                    // Continue On Error
                    args.Resolution = ErrorResolution.RetryOnNextSync;
                    Assert.NotNull(args.Exception);
                    Assert.NotNull(args.ErrorRow);
                    Assert.NotNull(args.SchemaTable);
                    Assert.Equal(SyncRowState.Modified, args.ApplyType);
                });

                var s = await agent.SynchronizeAsync(setup);

                Assert.Equal(2, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalChangesAppliedOnServer);
                Assert.Equal(1, s.TotalChangesAppliedOnClient);
                Assert.Equal(1, s.TotalChangesFailedToApplyOnClient);
                Assert.Equal(0, s.TotalChangesFailedToApplyOnServer);
                Assert.Equal(0, s.TotalResolvedConflicts);

                var batchInfos = agent.LocalOrchestrator.LoadBatchInfos();

                Assert.NotNull(batchInfos);
                Assert.Single(batchInfos);
                Assert.Single(batchInfos[0].BatchPartsInfo);
                Assert.Contains("ERROR", batchInfos[0].BatchPartsInfo[0].FileName);
                Assert.Equal("ProductCategory", batchInfos[0].BatchPartsInfo[0].TableName);

                var batchInfo = batchInfos[0];

                var syncTables = agent.LocalOrchestrator.LoadTablesFromBatchInfo(batchInfo);

                foreach (var syncTable in syncTables)
                {
                    Assert.Equal("ProductCategory", syncTable.TableName);
                    Assert.True(syncTable.HasRows);

                    Assert.Equal(SyncRowState.RetryModifiedOnNextSync, syncTable.Rows[0].RowState);
                }

                // To resolve the issue, just clear the interceptor
                agent.LocalOrchestrator.ClearInterceptors(interceptorId);

                // And then delete the values on server side
                await serverProvider.DeleteProductCategoryAsync($"Z{y}{str}");

                s = await agent.SynchronizeAsync(setup);

                Assert.Equal(1, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalChangesAppliedOnServer);
                Assert.Equal(0, s.TotalChangesAppliedOnClient);
                Assert.Equal(0, s.TotalChangesFailedToApplyOnClient);
                Assert.Equal(0, s.TotalChangesFailedToApplyOnServer);
                Assert.Equal(1, s.TotalResolvedConflicts);

                batchInfos = agent.LocalOrchestrator.LoadBatchInfos();

                Assert.Empty(batchInfos);

                x = x + 2;
                y = y + 2;
            }
        }

        [Fact]
        public virtual async Task ErrorUniqueKeyOnSameTableRetryOnNextSyncThenResolveClientByUpdate()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

            // make a first sync to init the two databases
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            // Adding two rows on server side, that are correct
            var str = HelperDatabase.GetRandomName().ToUpper()[..9];
            await serverProvider.AddProductCategoryAsync($"Z1{str}", name: $"Z1{str}");
            await serverProvider.AddProductCategoryAsync($"Z2{str}", name: $"Z2{str}");

            foreach (var clientProvider in clientsProvider)
            {
                // Get a random directory to be sure we are not conflicting with another test
                var directoryName = HelperDatabase.GetRandomName();
                options.BatchDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDirectory(), directoryName);
                // Enable constraints check
                options.DisableConstraintsOnApplyChanges = false;
                // Disable bulk operations to have the same results for SQL as others providers
                clientProvider.UseBulkOperations = false;

                var agent = new SyncAgent(clientProvider, serverProvider, options);

                // To generate a unique key constraint, will modify the batch part info on client just before load it.
                // Replacing $"Z1{str}" with $"Z2{str}" will generate a unique key constraint
                var interceptorId = agent.LocalOrchestrator.OnBatchChangesApplying(args =>
                {
                    if (args.BatchPartInfo != null && args.State == SyncRowState.Modified && args.SchemaTable.TableName == "ProductCategory")
                    {
                        var fullPath = args.BatchInfo.GetBatchPartInfoFullPath(args.BatchPartInfo);

                        using var table = agent.LocalOrchestrator.LoadTableFromBatchPartInfo(fullPath);

                        foreach (var row in table.Rows)
                            if (row["ProductCategoryID"].ToString() == $"Z2{str}")
                                row["Name"] = $"Z1{str}";

                        agent.LocalOrchestrator.SaveTableToBatchPartInfoAsync(args.BatchInfo, args.BatchPartInfo, table);
                    }
                });

                agent.LocalOrchestrator.OnApplyChangesErrorOccured(args =>
                {
                    // Continue On Error
                    args.Resolution = ErrorResolution.RetryOnNextSync;
                    Assert.NotNull(args.Exception);
                    Assert.NotNull(args.ErrorRow);
                    Assert.NotNull(args.SchemaTable);
                    Assert.Equal(SyncRowState.Modified, args.ApplyType);
                });

                var s = await agent.SynchronizeAsync(setup);

                Assert.Equal(2, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalChangesAppliedOnServer);
                Assert.Equal(1, s.TotalChangesAppliedOnClient);
                Assert.Equal(1, s.TotalChangesFailedToApplyOnClient);
                Assert.Equal(0, s.TotalChangesFailedToApplyOnServer);
                Assert.Equal(0, s.TotalResolvedConflicts);

                var batchInfos = agent.LocalOrchestrator.LoadBatchInfos();

                Assert.NotNull(batchInfos);
                Assert.Single(batchInfos);
                Assert.Single(batchInfos[0].BatchPartsInfo);
                Assert.Contains("ERROR", batchInfos[0].BatchPartsInfo[0].FileName);
                Assert.Equal("ProductCategory", batchInfos[0].BatchPartsInfo[0].TableName);

                var batchInfo = batchInfos[0];

                var syncTables = agent.LocalOrchestrator.LoadTablesFromBatchInfo(batchInfo);

                foreach (var syncTable in syncTables)
                {
                    Assert.Equal("ProductCategory", syncTable.TableName);
                    Assert.True(syncTable.HasRows);

                    Assert.Equal(SyncRowState.RetryModifiedOnNextSync, syncTable.Rows[0].RowState);
                }

                // To resolve the issue, just clear the interceptor
                agent.LocalOrchestrator.ClearInterceptors(interceptorId);

                // And then delete the values on server side
                var pc = await serverProvider.GetProductCategoryAsync($"Z2{str}");
                pc.Name = $"Z2{str}";
                await serverProvider.UpdateProductCategoryAsync(pc);

                s = await agent.SynchronizeAsync(setup);

                Assert.Equal(1, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalChangesAppliedOnServer);
                Assert.Equal(1, s.TotalChangesAppliedOnClient);
                Assert.Equal(0, s.TotalChangesFailedToApplyOnClient);
                Assert.Equal(0, s.TotalChangesFailedToApplyOnServer);
                Assert.Equal(0, s.TotalResolvedConflicts);

                batchInfos = agent.LocalOrchestrator.LoadBatchInfos();

                Assert.Empty(batchInfos);
            }
        }



        //// ------------------------------------------------------------------------
        //// Foreign Key failure
        //// ------------------------------------------------------------------------


        [Fact]
        public virtual async Task ErrorForeignKeyOnSameTableRaiseError()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

            // make a first sync to init the two databases
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            await serverProvider.AddProductCategoryAsync("ZZZZ");
            await serverProvider.AddProductCategoryAsync("AAAA", "ZZZZ");

            foreach (var clientProvider in clientsProvider)
            {
                // Get a random directory to be sure we are not conflicting with another test
                var directoryName = HelperDatabase.GetRandomName();
                options.BatchDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDirectoryName(), directoryName);
                // enablig constraints check
                options.DisableConstraintsOnApplyChanges = false;
                // Disable bulk operations to have the same results for SQL as others providers
                clientProvider.UseBulkOperations = false;

                var agent = new SyncAgent(clientProvider, serverProvider, options);

                // Generate the foreignkey error
                agent.LocalOrchestrator.OnRowsChangesApplying(args =>
                {
                    if (args.SyncRows == null || args.SyncRows.Count <= 0)
                        return;
                    var row = args.SyncRows[0];
                    if (row["ParentProductCategoryId"] != null && row["ParentProductCategoryId"].ToString() == "ZZZZ")
                        row["ParentProductCategoryId"] = "BBBBB";
                });

                var exc = await Assert.ThrowsAsync<SyncException>(() => agent.SynchronizeAsync(setup));

                Assert.NotNull(exc);

                var batchInfos = agent.LocalOrchestrator.LoadBatchInfos();

                Assert.Empty(batchInfos);
            }
        }

        [Fact]
        public virtual async Task ErrorForeignKeyOnSameTableContinueOnError()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

            // make a first sync to init the two databases
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            await serverProvider.AddProductCategoryAsync("ZZZZ");
            await serverProvider.AddProductCategoryAsync("AAAA", "ZZZZ");

            foreach (var clientProvider in clientsProvider)
            {
                // Get a random directory to be sure we are not conflicting with another test
                var directoryName = HelperDatabase.GetRandomName();
                options.BatchDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDirectory(), directoryName);
                // enablig constraints check
                options.DisableConstraintsOnApplyChanges = false;
                // Disable bulk operations to have the same results for SQL as others providers
                clientProvider.UseBulkOperations = false;

                var agent = new SyncAgent(clientProvider, serverProvider, options);

                // Generate error on foreign key on second row
                agent.LocalOrchestrator.OnRowsChangesApplying(args =>
                {
                    if (args.SyncRows == null || args.SyncRows.Count <= 0)
                        return;
                    var row = args.SyncRows[0];
                    if (row["ParentProductCategoryId"] != null && row["ParentProductCategoryId"].ToString() == "ZZZZ")
                        row["ParentProductCategoryId"] = "BBBBB";
                });

                agent.LocalOrchestrator.OnApplyChangesErrorOccured(args =>
                {
                    // Continue On Error
                    args.Resolution = ErrorResolution.ContinueOnError;
                    Assert.NotNull(args.Exception);
                    Assert.NotNull(args.ErrorRow);
                    Assert.NotNull(args.SchemaTable);
                    Assert.Equal(SyncRowState.Modified, args.ApplyType);
                });

                var s = await agent.SynchronizeAsync(setup);

                // Download 2 rows
                // But applied only 1
                // The other one is a failed inserted row
                Assert.Equal(2, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalChangesAppliedOnClient);
                Assert.Equal(1, s.TotalChangesFailedToApplyOnClient);
                Assert.Equal(0, s.TotalResolvedConflicts);

                var batchInfos = agent.LocalOrchestrator.LoadBatchInfos();

                Assert.NotNull(batchInfos);
                Assert.Single(batchInfos);
                Assert.Single(batchInfos[0].BatchPartsInfo);
                Assert.Contains("ERRORS", batchInfos[0].BatchPartsInfo[0].FileName);
                Assert.Equal("ProductCategory", batchInfos[0].BatchPartsInfo[0].TableName);

                var batchInfo = batchInfos[0];

                var syncTables = agent.LocalOrchestrator.LoadTablesFromBatchInfo(batchInfo);

                foreach (var syncTable in syncTables)
                {
                    Assert.Equal("ProductCategory", syncTable.TableName);
                    Assert.True(syncTable.HasRows);

                    Assert.Equal(SyncRowState.ApplyModifiedFailed, syncTable.Rows[0].RowState);
                }

                s = await agent.SynchronizeAsync(setup);

                Assert.Equal(0, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalChangesAppliedOnClient);
                Assert.Equal(0, s.TotalChangesFailedToApplyOnClient);
                Assert.Equal(0, s.TotalResolvedConflicts);

                batchInfos = agent.LocalOrchestrator.LoadBatchInfos();

                Assert.NotNull(batchInfos);
                Assert.Single(batchInfos);
                Assert.Single(batchInfos[0].BatchPartsInfo);
                Assert.Contains("ERRORS", batchInfos[0].BatchPartsInfo[0].FileName);
                Assert.Equal("ProductCategory", batchInfos[0].BatchPartsInfo[0].TableName);

                batchInfo = batchInfos[0];

                syncTables = agent.LocalOrchestrator.LoadTablesFromBatchInfo(batchInfo);

                foreach (var syncTable in syncTables)
                {
                    Assert.Equal("ProductCategory", syncTable.TableName);
                    Assert.True(syncTable.HasRows);

                    Assert.Equal(SyncRowState.ApplyModifiedFailed, syncTable.Rows[0].RowState);
                }

            }
        }

        [Fact]
        public virtual async Task ErrorForeignKeyOnSameTableContinueOnErrorUsingSyncOptions()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

            // make a first sync to init the two databases
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            await serverProvider.AddProductCategoryAsync("ZZZZ");
            await serverProvider.AddProductCategoryAsync("AAAA", "ZZZZ");

            foreach (var clientProvider in clientsProvider)
            {
                // Get a random directory to be sure we are not conflicting with another test
                var directoryName = HelperDatabase.GetRandomName();
                options.BatchDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDirectory(), directoryName);
                // enablig constraints check
                options.DisableConstraintsOnApplyChanges = false;
                // set error policy
                options.ErrorResolutionPolicy = ErrorResolution.ContinueOnError;
                // Disable bulk operations to have the same results for SQL as others providers
                clientProvider.UseBulkOperations = false;

                var agent = new SyncAgent(clientProvider, serverProvider, options);

                // Generate error on foreign key on second row
                agent.LocalOrchestrator.OnRowsChangesApplying(args =>
                {
                    if (args.SyncRows == null || args.SyncRows.Count <= 0)
                        return;
                    var row = args.SyncRows[0];
                    if (row["ParentProductCategoryId"] != null && row["ParentProductCategoryId"].ToString() == "ZZZZ")
                        row["ParentProductCategoryId"] = "BBBBB";
                });

                var s = await agent.SynchronizeAsync(setup);

                // Download 2 rows
                // But applied only 1
                // The other one is a failed inserted row
                Assert.Equal(2, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalChangesAppliedOnClient);
                Assert.Equal(1, s.TotalChangesFailedToApplyOnClient);
                Assert.Equal(0, s.TotalResolvedConflicts);

                var batchInfos = agent.LocalOrchestrator.LoadBatchInfos();

                Assert.NotNull(batchInfos);
                Assert.Single(batchInfos);
                Assert.Single(batchInfos[0].BatchPartsInfo);
                Assert.Contains("ERRORS", batchInfos[0].BatchPartsInfo[0].FileName);
                Assert.Equal("ProductCategory", batchInfos[0].BatchPartsInfo[0].TableName);

                var batchInfo = batchInfos[0];

                var syncTables = agent.LocalOrchestrator.LoadTablesFromBatchInfo(batchInfo);

                foreach (var syncTable in syncTables)
                {
                    Assert.Equal("ProductCategory", syncTable.TableName);
                    Assert.True(syncTable.HasRows);

                    Assert.Equal(SyncRowState.ApplyModifiedFailed, syncTable.Rows[0].RowState);
                }

                s = await agent.SynchronizeAsync(setup);

                Assert.Equal(0, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalChangesAppliedOnClient);
                Assert.Equal(0, s.TotalChangesFailedToApplyOnClient);
                Assert.Equal(0, s.TotalResolvedConflicts);

                batchInfos = agent.LocalOrchestrator.LoadBatchInfos();

                Assert.NotNull(batchInfos);
                Assert.Single(batchInfos);
                Assert.Single(batchInfos[0].BatchPartsInfo);
                Assert.Contains("ERRORS", batchInfos[0].BatchPartsInfo[0].FileName);
                Assert.Equal("ProductCategory", batchInfos[0].BatchPartsInfo[0].TableName);

                batchInfo = batchInfos[0];

                syncTables = agent.LocalOrchestrator.LoadTablesFromBatchInfo(batchInfo);

                foreach (var syncTable in syncTables)
                {
                    Assert.Equal("ProductCategory", syncTable.TableName);
                    Assert.True(syncTable.HasRows);

                    Assert.Equal(SyncRowState.ApplyModifiedFailed, syncTable.Rows[0].RowState);
                }

            }
        }

        [Fact]
        public virtual async Task ErrorForeignKeyOnSameTableRetryOneMoreTime()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

            // make a first sync to init the two databases
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            await serverProvider.AddProductCategoryAsync("ZZZZ");
            await serverProvider.AddProductCategoryAsync("AAAA", "ZZZZ");

            foreach (var clientProvider in clientsProvider)
            {
                // Get a random directory to be sure we are not conflicting with another test
                var directoryName = HelperDatabase.GetRandomName();
                options.BatchDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDirectory(), directoryName);
                // enablig constraints check
                options.DisableConstraintsOnApplyChanges = false;
                // set error policy
                options.ErrorResolutionPolicy = ErrorResolution.ContinueOnError;
                // Disable bulk operations to have the same results for SQL as others providers
                clientProvider.UseBulkOperations = false;

                var agent = new SyncAgent(clientProvider, serverProvider, options);

                // As OnRowsChangesApplying will be called 2 times, we only apply tricky change one time
                var rowChanged = false;

                // Generate the foreignkey error
                agent.LocalOrchestrator.OnRowsChangesApplying(args =>
                {
                    if (args.SyncRows == null || args.SyncRows.Count <= 0)
                        return;

                    var row = args.SyncRows[0];

                    if (row["ParentProductCategoryId"] != null && row["ParentProductCategoryId"].ToString() == "ZZZZ")
                    {
                        // We need to change the row only one time
                        if (rowChanged)
                            return;

                        row["ParentProductCategoryId"] = "BBBBB";
                        rowChanged = true;
                    }
                });

                // Once error has been raised, we change back the row to the initial value
                // to let a chance to apply again at the end
                agent.LocalOrchestrator.OnRowsChangesApplied(args =>
                {
                    if (args.SyncRows == null || args.SyncRows.Count <= 0)
                        return;

                    var row = args.SyncRows[0];

                    if (row["ParentProductCategoryId"] != null && row["ParentProductCategoryId"].ToString() == "BBBBB")
                    {
                        row["ParentProductCategoryId"] = "ZZZZ";
                        rowChanged = true;
                    }
                });

                agent.LocalOrchestrator.OnApplyChangesErrorOccured(args =>
                {
                    // Continue On Error
                    args.Resolution = ErrorResolution.RetryOneMoreTimeAndThrowOnError;
                    Assert.NotNull(args.Exception);
                    Assert.NotNull(args.ErrorRow);
                    Assert.NotNull(args.SchemaTable);
                    Assert.Equal(SyncRowState.Modified, args.ApplyType);
                });

                var s = await agent.SynchronizeAsync(setup);

                // Download 2 rows
                // But applied only 1
                // The other one is a failed inserted row
                Assert.Equal(2, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(2, s.TotalChangesAppliedOnClient);
                Assert.Equal(0, s.TotalChangesFailedToApplyOnClient);
                Assert.Equal(0, s.TotalResolvedConflicts);

                var batchInfos = agent.LocalOrchestrator.LoadBatchInfos();

                Assert.Empty(batchInfos);
            }
        }

        //// ------------------------------------------------------------------------
        //// Transient Errors
        //// ------------------------------------------------------------------------


        private void HookExceptionToTransient(Exception ex, ProviderType providerType)
        {
            if (providerType == ProviderType.Sql)
            {
                var exception = ex as SqlException;
                while (ex != null)
                {
                    if (ex is SqlException sqlException)
                    {
                        var error = sqlException.Errors[0];
                        var errorType = typeof(SqlError);
                        var errorNumber = errorType.GetField("_number", BindingFlags.Default | BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
                        errorNumber.SetValue(error, 64);
                        break;

                    }
                    else
                    {
                        ex = ex.InnerException;
                    }
                }

            }
            if (providerType == ProviderType.Sqlite)
            {
                var exception = ex as SqlException;
                while (ex != null)
                {
                    if (ex is SqliteException sqliteException)
                    {
                        var errorType = typeof(SqliteException);
                        var errorNumber = errorType.GetRuntimeFields().FirstOrDefault(f => f.Name.Contains("SqliteErrorCode"));
                        errorNumber.SetValue(sqliteException, 11);
                        break;

                    }
                    else
                    {
                        ex = ex.InnerException;
                    }
                }
            }
        }

        [Fact]
        public virtual async Task ErrorTransientOnServerWithoutTransactionShouldWork()
        {
            var (serverProviderType, _) = HelperDatabase.GetDatabaseType(serverProvider);

            if (serverProviderType != ProviderType.Sql)
                return;

            var options = new SyncOptions
            {
                DisableConstraintsOnApplyChanges = true,
                TransactionMode = TransactionMode.None
            };

            // make a first sync to init the two databases
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            var upload = 0;
            foreach (var clientProvider in clientsProvider)
            {
                var (clientProviderType, clientDatabaseName) = HelperDatabase.GetDatabaseType(clientProvider);

                // Adding one row
                var str = HelperDatabase.GetRandomName().ToUpper()[..9];
                await clientProvider.AddProductCategoryAsync($"Z1{str}", name: $"Z1{str}");

                var agent = new SyncAgent(clientProvider, serverProvider, options);

                var transientErrorHappened = false;
                string commandText = null;
                agent.RemoteOrchestrator.OnTransientErrorOccured(args =>
                {
                    transientErrorHappened = true;
                });

                // generating a transient error
                agent.RemoteOrchestrator.OnRowsChangesApplying(args =>
                {
                    if (!transientErrorHappened)
                    {
                        commandText = args.Command.CommandText;
                        args.Command.CommandText = "spNonExistantProcedure";
                    }
                });

                agent.RemoteOrchestrator.OnRowsChangesApplied(args =>
                {
                    if (args.Exception == null)
                        return;

                    HookExceptionToTransient(args.Exception, serverProviderType);
                });


                var r = await agent.SynchronizeAsync(setup);
                Assert.Equal(1, r.TotalChangesUploadedToServer);
                Assert.Equal(1, r.TotalChangesAppliedOnServer);
                Assert.Equal(upload, r.TotalChangesDownloadedFromServer);
                Assert.Equal(upload, r.TotalChangesAppliedOnClient);
                upload++;
                Assert.True(transientErrorHappened);
            }
        }

        [Fact]
        public virtual async Task ErrorTransientOnServerOnFullTransactionShouldWork()
        {
            var (serverProviderType, _) = HelperDatabase.GetDatabaseType(serverProvider);

            if (serverProviderType != ProviderType.Sql)
                return;

            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

            // make a first sync to init the two databases
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            var upload = 0;
            foreach (var clientProvider in clientsProvider)
            {
                var (clientProviderType, clientDatabaseName) = HelperDatabase.GetDatabaseType(clientProvider);

                // Adding one row
                var str = HelperDatabase.GetRandomName().ToUpper()[..9];
                await clientProvider.AddProductCategoryAsync($"Z1{str}", name: $"Z1{str}");

                var agent = new SyncAgent(clientProvider, serverProvider, options);

                var transientErrorHappened = false;
                string commandText = null;
                agent.RemoteOrchestrator.OnTransientErrorOccured(args =>
                {
                    transientErrorHappened = true;
                });

                // generating a transient error
                agent.RemoteOrchestrator.OnRowsChangesApplying(args =>
                {
                    if (!transientErrorHappened)
                    {
                        commandText = args.Command.CommandText;
                        args.Command.CommandText = "spNonExistantProcedure";
                    }
                });

                agent.RemoteOrchestrator.OnRowsChangesApplied(args =>
                {
                    if (args.Exception == null)
                        return;

                    HookExceptionToTransient(args.Exception, serverProviderType);
                });


                var r = await agent.SynchronizeAsync(setup);
                Assert.Equal(1, r.TotalChangesUploadedToServer);
                Assert.Equal(1, r.TotalChangesAppliedOnServer);
                Assert.Equal(upload, r.TotalChangesDownloadedFromServer);
                Assert.Equal(upload, r.TotalChangesAppliedOnClient);
                upload++;
                Assert.True(transientErrorHappened);
            }
        }

        [Fact]
        public virtual async Task ErrorTransientOnServerOnBatchTransactionShouldWork()
        {
            var (serverProviderType, _) = HelperDatabase.GetDatabaseType(serverProvider);

            if (serverProviderType != ProviderType.Sql)
                return;

            var options = new SyncOptions
            {
                DisableConstraintsOnApplyChanges = true,
                TransactionMode = TransactionMode.PerBatch
            };

            // make a first sync to init the two databases
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            var upload = 0;
            foreach (var clientProvider in clientsProvider)
            {
                var (clientProviderType, clientDatabaseName) = HelperDatabase.GetDatabaseType(clientProvider);

                // Adding one row
                var str = HelperDatabase.GetRandomName().ToUpper()[..9];
                await clientProvider.AddProductCategoryAsync($"Z1{str}", name: $"Z1{str}");

                var agent = new SyncAgent(clientProvider, serverProvider, options);

                var transientErrorHappened = false;
                string commandText = null;
                agent.RemoteOrchestrator.OnTransientErrorOccured(args =>
                {
                    transientErrorHappened = true;
                });

                // generating a transient error
                agent.RemoteOrchestrator.OnRowsChangesApplying(args =>
                {
                    if (!transientErrorHappened)
                    {
                        commandText = args.Command.CommandText;
                        args.Command.CommandText = "spNonExistantProcedure";
                    }
                });

                agent.RemoteOrchestrator.OnRowsChangesApplied(args =>
                {
                    if (args.Exception == null)
                        return;

                    HookExceptionToTransient(args.Exception, serverProviderType);
                });


                var r = await agent.SynchronizeAsync(setup);
                Assert.Equal(1, r.TotalChangesUploadedToServer);
                Assert.Equal(1, r.TotalChangesAppliedOnServer);
                Assert.Equal(upload, r.TotalChangesDownloadedFromServer);
                Assert.Equal(upload, r.TotalChangesAppliedOnClient);
                upload++;
                Assert.True(transientErrorHappened);
            }
        }

        [Fact]
        public virtual async Task ErrorTransientOnServerOnFullTransactionLineByLineShouldWork()
        {
            var (serverProviderType, _) = HelperDatabase.GetDatabaseType(serverProvider);

            if (serverProviderType != ProviderType.Sql)
                return;

            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

            // make a first sync to init the two databases
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            var upload = 0;
            foreach (var clientProvider in clientsProvider)
            {
                var (clientProviderType, clientDatabaseName) = HelperDatabase.GetDatabaseType(clientProvider);

                // Adding one row
                var str = HelperDatabase.GetRandomName().ToUpper()[..9];
                await clientProvider.AddProductCategoryAsync($"Z1{str}", name: $"Z1{str}");

                // Disabling bulk operations
                serverProvider.UseBulkOperations = false;

                var agent = new SyncAgent(clientProvider, serverProvider, options);

                var transientErrorHappened = false;
                string commandText = null;
                agent.RemoteOrchestrator.OnTransientErrorOccured(args =>
                {
                    transientErrorHappened = true;
                });

                // generating a transient error
                agent.RemoteOrchestrator.OnRowsChangesApplying(args =>
                {
                    if (!transientErrorHappened)
                    {
                        commandText = args.Command.CommandText;
                        args.Command.CommandText = "spNonExistantProcedure";
                    }
                });

                agent.RemoteOrchestrator.OnRowsChangesApplied(args =>
                {
                    if (args.Exception == null)
                        return;
                    HookExceptionToTransient(args.Exception, serverProviderType);
                });


                var r = await agent.SynchronizeAsync(setup);
                Assert.Equal(1, r.TotalChangesUploadedToServer);
                Assert.Equal(1, r.TotalChangesAppliedOnServer);
                Assert.Equal(upload, r.TotalChangesDownloadedFromServer);
                Assert.Equal(upload, r.TotalChangesAppliedOnClient);
                upload++;
                Assert.True(transientErrorHappened);
            }
        }

        [Fact]
        public virtual async Task ErrorTransientOnServerOnBatchTransactionLineByLineShouldWork()
        {
            var (serverProviderType, _) = HelperDatabase.GetDatabaseType(serverProvider);

            if (serverProviderType != ProviderType.Sql)
                return;

            var options = new SyncOptions
            {
                DisableConstraintsOnApplyChanges = true,
                TransactionMode = TransactionMode.PerBatch
            };

            // make a first sync to init the two databases
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            var download = 0;
            foreach (var clientProvider in clientsProvider)
            {
                var (clientProviderType, clientDatabaseName) = HelperDatabase.GetDatabaseType(clientProvider);

                // Adding 10 rows
                await clientProvider.AddProductCategoryAsync();
                await clientProvider.AddProductCategoryAsync();
                await clientProvider.AddProductCategoryAsync();
                await clientProvider.AddProductCategoryAsync();
                await clientProvider.AddProductCategoryAsync();
                await clientProvider.AddProductCategoryAsync();
                await clientProvider.AddProductCategoryAsync();
                await clientProvider.AddProductCategoryAsync();
                await clientProvider.AddProductCategoryAsync();
                await clientProvider.AddProductCategoryAsync();

                // Disabling bulk operations
                serverProvider.UseBulkOperations = false;

                var agent = new SyncAgent(clientProvider, serverProvider, options);

                var transientErrorHappened = false;
                string commandText = null;
                int transientErrorsCount = 0;
                int applyingCount = 0;
                agent.RemoteOrchestrator.OnTransientErrorOccured(args =>
                {
                    transientErrorsCount++;
                    transientErrorHappened = true;
                });

                // generating a transient error
                agent.RemoteOrchestrator.OnRowsChangesApplying(args =>
                {
                    applyingCount++;

                    // simulate a transient error on the second apply
                    if (applyingCount == 2 && !transientErrorHappened)
                    {
                        commandText = args.Command.CommandText;
                        args.Command.CommandText = "spNonExistantProcedure";
                    }
                });

                agent.RemoteOrchestrator.OnRowsChangesApplied(args =>
                {
                    if (args.Exception == null)
                        return;

                    HookExceptionToTransient(args.Exception, serverProviderType);
                });


                var r = await agent.SynchronizeAsync(setup);
                Assert.Equal(10, r.TotalChangesUploadedToServer);
                Assert.Equal(10, r.TotalChangesAppliedOnServer);
                Assert.Equal(download, r.TotalChangesDownloadedFromServer);
                Assert.Equal(download, r.TotalChangesAppliedOnClient);
                download += 10;
                Assert.True(transientErrorHappened);
            }
        }

        [Fact]
        public virtual async Task ErrorTransientOnClientWithoutTransactionShouldWork()
        {
            var (serverProviderType, _) = HelperDatabase.GetDatabaseType(serverProvider);

            if (serverProviderType != ProviderType.Sql)
                return;

            var options = new SyncOptions
            {
                DisableConstraintsOnApplyChanges = true,
                TransactionMode = TransactionMode.None
            };

            // make a first sync to init the two databases
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            // Adding one row
            await serverProvider.AddProductCategoryAsync();

            foreach (var clientProvider in clientsProvider)
            {
                var (clientProviderType, clientDatabaseName) = HelperDatabase.GetDatabaseType(clientProvider);
                var agent = new SyncAgent(clientProvider, serverProvider, options);

                var transientErrorHappened = false;
                string commandText = null;

                agent.LocalOrchestrator.OnTransientErrorOccured(args =>
                {
                    transientErrorHappened = true;
                });

                // generating a transient error
                agent.LocalOrchestrator.OnRowsChangesApplying(args =>
                {
                    if (!transientErrorHappened)
                    {
                        commandText = args.Command.CommandText;
                        args.Command.CommandText = "spNonExistantProcedure";
                    }
                });

                agent.LocalOrchestrator.OnRowsChangesApplied(args =>
                {
                    if (args.Exception == null)
                        return;

                    HookExceptionToTransient(args.Exception.InnerException, clientProviderType);
                });


                var r = await agent.SynchronizeAsync(setup);
                Assert.Equal(0, r.TotalChangesUploadedToServer);
                Assert.Equal(0, r.TotalChangesAppliedOnServer);
                Assert.Equal(1, r.TotalChangesDownloadedFromServer);
                Assert.Equal(1, r.TotalChangesAppliedOnClient);
                Assert.True(transientErrorHappened);
            }
        }

        [Fact]
        public virtual async Task ErrorTransientOnClientOnFullTransactionShouldWork()
        {
            var (serverProviderType, _) = HelperDatabase.GetDatabaseType(serverProvider);

            if (serverProviderType != ProviderType.Sql)
                return;

            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

            // make a first sync to init the two databases
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            // Adding one row
            await serverProvider.AddProductCategoryAsync();

            foreach (var clientProvider in clientsProvider)
            {
                var (clientProviderType, clientDatabaseName) = HelperDatabase.GetDatabaseType(clientProvider);
                var agent = new SyncAgent(clientProvider, serverProvider, options);

                var transientErrorHappened = false;
                string commandText = null;

                agent.LocalOrchestrator.OnTransientErrorOccured(args =>
                {
                    transientErrorHappened = true;
                });

                // generating a transient error
                agent.LocalOrchestrator.OnRowsChangesApplying(args =>
                {
                    if (!transientErrorHappened)
                    {
                        commandText = args.Command.CommandText;
                        args.Command.CommandText = "spNonExistantProcedure";
                    }
                });

                agent.LocalOrchestrator.OnRowsChangesApplied(args =>
                {
                    if (args.Exception == null)
                        return;

                    HookExceptionToTransient(args.Exception, clientProviderType);
                });


                var r = await agent.SynchronizeAsync(setup);
                Assert.Equal(0, r.TotalChangesUploadedToServer);
                Assert.Equal(0, r.TotalChangesAppliedOnServer);
                Assert.Equal(1, r.TotalChangesDownloadedFromServer);
                Assert.Equal(1, r.TotalChangesAppliedOnClient);
                Assert.True(transientErrorHappened);
            }
        }

        [Fact]
        public virtual async Task ErrorTransientOnClientOnBatchTransactionShouldWork()
        {
            var (serverProviderType, _) = HelperDatabase.GetDatabaseType(serverProvider);

            if (serverProviderType != ProviderType.Sql)
                return;

            var options = new SyncOptions
            {
                DisableConstraintsOnApplyChanges = true,
                TransactionMode = TransactionMode.PerBatch
            };

            // make a first sync to init the two databases
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            // Adding one row
            await serverProvider.AddProductCategoryAsync();

            foreach (var clientProvider in clientsProvider)
            {
                var (clientProviderType, clientDatabaseName) = HelperDatabase.GetDatabaseType(clientProvider);
                var agent = new SyncAgent(clientProvider, serverProvider, options);

                var transientErrorHappened = false;
                string commandText = null;

                agent.LocalOrchestrator.OnTransientErrorOccured(args =>
                {
                    transientErrorHappened = true;
                });

                // generating a transient error
                agent.LocalOrchestrator.OnRowsChangesApplying(args =>
                {
                    if (!transientErrorHappened)
                    {
                        commandText = args.Command.CommandText;
                        args.Command.CommandText = "spNonExistantProcedure";
                    }
                });

                agent.LocalOrchestrator.OnRowsChangesApplied(args =>
                {
                    if (args.Exception == null)
                        return;

                    HookExceptionToTransient(args.Exception, clientProviderType);
                });


                var r = await agent.SynchronizeAsync(setup);
                Assert.Equal(0, r.TotalChangesUploadedToServer);
                Assert.Equal(0, r.TotalChangesAppliedOnServer);
                Assert.Equal(1, r.TotalChangesDownloadedFromServer);
                Assert.Equal(1, r.TotalChangesAppliedOnClient);
                Assert.True(transientErrorHappened);
            }
        }

        [Fact]
        public virtual async Task ErrorTransientOnClientOnFullTransactionLineByLineShouldWork()
        {
            var (serverProviderType, _) = HelperDatabase.GetDatabaseType(serverProvider);

            if (serverProviderType != ProviderType.Sql)
                return;

            var options = new SyncOptions
            {
                DisableConstraintsOnApplyChanges = true,
            };

            // make a first sync to init the two databases
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            // Adding one row
            await serverProvider.AddProductCategoryAsync();

            foreach (var clientProvider in clientsProvider)
            {
                var (clientProviderType, clientDatabaseName) = HelperDatabase.GetDatabaseType(clientProvider);

                // disable bulk operation
                clientProvider.UseBulkOperations = false;

                var agent = new SyncAgent(clientProvider, serverProvider, options);

                var transientErrorHappened = false;
                string commandText = null;

                agent.LocalOrchestrator.OnTransientErrorOccured(args =>
                {
                    transientErrorHappened = true;
                });

                // generating a transient error
                agent.LocalOrchestrator.OnRowsChangesApplying(args =>
                {
                    if (!transientErrorHappened)
                    {
                        commandText = args.Command.CommandText;
                        args.Command.CommandText = "spNonExistantProcedure";
                    }
                });

                agent.LocalOrchestrator.OnRowsChangesApplied(args =>
                {
                    if (args.Exception == null)
                        return;

                    HookExceptionToTransient(args.Exception, clientProviderType);
                });


                var r = await agent.SynchronizeAsync(setup);
                Assert.Equal(0, r.TotalChangesUploadedToServer);
                Assert.Equal(0, r.TotalChangesAppliedOnServer);
                Assert.Equal(1, r.TotalChangesDownloadedFromServer);
                Assert.Equal(1, r.TotalChangesAppliedOnClient);
                Assert.True(transientErrorHappened);
            }
        }

        [Fact]
        public virtual async Task ErrorTransientOnClientOnBatchTransactionLineByLineShouldWork()
        {
            var (serverProviderType, _) = HelperDatabase.GetDatabaseType(serverProvider);

            if (serverProviderType != ProviderType.Sql)
                return;

            var options = new SyncOptions
            {
                DisableConstraintsOnApplyChanges = true,
                TransactionMode = TransactionMode.PerBatch
            };

            // make a first sync to init the two databases
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            // Adding one row
            await serverProvider.AddProductCategoryAsync();

            foreach (var clientProvider in clientsProvider)
            {
                var (clientProviderType, clientDatabaseName) = HelperDatabase.GetDatabaseType(clientProvider);

                // disable bulk operation
                clientProvider.UseBulkOperations = false;

                var agent = new SyncAgent(clientProvider, serverProvider, options);

                var transientErrorHappened = false;
                string commandText = null;

                agent.LocalOrchestrator.OnTransientErrorOccured(args =>
                {
                    transientErrorHappened = true;
                });

                // generating a transient error
                agent.LocalOrchestrator.OnRowsChangesApplying(args =>
                {
                    if (!transientErrorHappened)
                    {
                        commandText = args.Command.CommandText;
                        args.Command.CommandText = "spNonExistantProcedure";
                    }
                });

                agent.LocalOrchestrator.OnRowsChangesApplied(args =>
                {
                    if (args.Exception == null)
                        return;

                    HookExceptionToTransient(args.Exception, clientProviderType);
                });


                var r = await agent.SynchronizeAsync(setup);
                Assert.Equal(0, r.TotalChangesUploadedToServer);
                Assert.Equal(0, r.TotalChangesAppliedOnServer);
                Assert.Equal(1, r.TotalChangesDownloadedFromServer);
                Assert.Equal(1, r.TotalChangesAppliedOnClient);
                Assert.True(transientErrorHappened);
            }
        }


        // ------------------------------------------------------------------------
        // InsertClient - InsertServer
        // ------------------------------------------------------------------------

        /// <summary>
        /// Generate a conflict when inserting one row on server and the same row on each client
        /// Server should wins the conflict since it's the default behavior
        /// </summary>
        [Fact]
        public virtual async Task Conflict_IC_IS_ServerShouldWins()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

            // make a first sync to init the two databases
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            var productCategoryNameClient = HelperDatabase.GetRandomName("CLI");
            var productCategoryNameServer = HelperDatabase.GetRandomName("SRV");
            var productId = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 6);
            await serverProvider.AddProductCategoryAsync(productId, name: productCategoryNameServer);

            // Execute a sync on all clients and check results
            // Each client will upload its row (conflicting)
            // then download the others client lines + the conflict (some Clients.count)
            foreach (var clientProvider in clientsProvider)
            {
                await clientProvider.AddProductCategoryAsync(productId, name: productCategoryNameClient);

                var agent = new SyncAgent(clientProvider, serverProvider, options);

                var s = await agent.SynchronizeAsync(setup);

                Assert.Equal(1, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesAppliedOnClient);
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalChangesAppliedOnServer);
                Assert.Equal(1, s.TotalResolvedConflicts);

                var pcClient = await clientProvider.GetProductCategoryAsync(productId);
                var pcServer = await clientProvider.GetProductCategoryAsync(productId);

                Assert.Equal(pcServer.Name, pcClient.Name);
                Assert.StartsWith("SRV", pcClient.Name);
            }

        }

        /// <summary>
        /// Generate a conflict when inserting one row on server and the same row on each client
        /// Server should wins the conflict since it's the default behavior
        /// </summary>
        [Fact]
        public virtual async Task Conflict_IC_IS_ServerShouldWins_CozHandler()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

            // make a first sync to init the two databases
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            var productCategoryNameClient = HelperDatabase.GetRandomName("CLI");
            var productCategoryNameServer = HelperDatabase.GetRandomName("SRV");
            var productId = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 6);
            await serverProvider.AddProductCategoryAsync(productId, name: productCategoryNameServer);

            // Execute a sync on all clients and check results
            // Each client will upload its row (conflicting)
            // then download the others client lines + the conflict (some Clients.count)
            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, serverProvider, options);

                await clientProvider.AddProductCategoryAsync(productId, name: productCategoryNameClient);

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

                var s = await agent.SynchronizeAsync(setup);

                Assert.Equal(1, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalResolvedConflicts);

                var pcClient = await clientProvider.GetProductCategoryAsync(productId);
                var pcServer = await clientProvider.GetProductCategoryAsync(productId);

                Assert.Equal(pcServer.Name, pcClient.Name);
                Assert.StartsWith("SRV", pcClient.Name);
            }

        }

        /// <summary>
        /// Generate a conflict when inserting one row on server and the same row on each client
        /// Client should wins the conflict because configuration set to ClientWins
        /// </summary>
        [Fact]
        public virtual async Task Conflict_IC_IS_ClientShouldWins_CozConfiguration()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

            // make a first sync to init the two databases
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            // Execute a sync on all clients and check results
            // Each client will upload its row (conflicting)
            // then download the others client lines (and not the conflict since it's resolved)
            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, serverProvider, options);

                // Reinit 
                await agent.SynchronizeAsync();

                var productCategoryNameClient = HelperDatabase.GetRandomName("CLI");
                var productCategoryNameServer = HelperDatabase.GetRandomName("SRV");
                var productId = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 6);
                await serverProvider.AddProductCategoryAsync(productId, name: productCategoryNameServer);
                await clientProvider.AddProductCategoryAsync(productId, name: productCategoryNameClient);

                agent.Options.ConflictResolutionPolicy = ConflictResolutionPolicy.ClientWins;

                var s = await agent.SynchronizeAsync(setup);

                Assert.Equal(0, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalResolvedConflicts);

                var pcClient = await clientProvider.GetProductCategoryAsync(productId);
                var pcServer = await clientProvider.GetProductCategoryAsync(productId);

                Assert.Equal(pcServer.Name, pcClient.Name);
                Assert.StartsWith("CLI", pcClient.Name);
            }
        }

        /// <summary>
        /// Generate a conflict when inserting one row on server and the same row on each client
        /// Client should wins the conflict because configuration set to ClientWins
        /// </summary>
        [Fact]
        public virtual async Task Conflict_IC_IS_ClientShouldWins_CozConfiguration_CozHandler()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

            // make a first sync to init the two databases
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            // Execute a sync on all clients and check results
            // Each client will upload its row (conflicting)
            // then download the others client lines (and not the conflict since it's resolved)
            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, serverProvider, options);

                // Reinit 
                await agent.SynchronizeAsync();

                var productCategoryNameClient = HelperDatabase.GetRandomName("CLI");
                var productCategoryNameServer = HelperDatabase.GetRandomName("SRV");
                var productId = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 6);
                await serverProvider.AddProductCategoryAsync(productId, name: productCategoryNameServer);
                await clientProvider.AddProductCategoryAsync(productId, name: productCategoryNameClient);

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

                    Assert.Equal(ConflictResolution.ServerWins, acf.Resolution);
                    Assert.Equal(ConflictType.RemoteExistsLocalExists, conflict.Type);
                    acf.Resolution = ConflictResolution.ClientWins;
                });


                var s = await agent.SynchronizeAsync(setup);

                Assert.Equal(0, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalResolvedConflicts);

                var pcClient = await clientProvider.GetProductCategoryAsync(productId);
                var pcServer = await clientProvider.GetProductCategoryAsync(productId);

                Assert.Equal(pcServer.Name, pcClient.Name);
                Assert.StartsWith("CLI", pcClient.Name);
            }
        }

        /// <summary>
        /// Generate a conflict when inserting one row on server and the same row on each client
        /// Client should wins the conflict because we have an event raised
        /// </summary>
        [Fact]
        public virtual async Task Conflict_IC_IS_ClientShouldWins_CozHandler()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

            // make a first sync to init the two databases
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            // Execute a sync on all clients and check results
            // Each client will upload its row (conflicting)
            // then download the others client lines (and not the conflict since it's resolved)
            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, serverProvider, options);

                // Reinit 
                await agent.SynchronizeAsync();

                var productCategoryNameClient = HelperDatabase.GetRandomName("CLI");
                var productCategoryNameServer = HelperDatabase.GetRandomName("SRV");
                var productId = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 6);
                await serverProvider.AddProductCategoryAsync(productId, name: productCategoryNameServer);
                await clientProvider.AddProductCategoryAsync(productId, name: productCategoryNameClient);

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

                var s = await agent.SynchronizeAsync(setup);

                Assert.Equal(0, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalResolvedConflicts);

                var pcClient = await clientProvider.GetProductCategoryAsync(productId);
                var pcServer = await clientProvider.GetProductCategoryAsync(productId);

                Assert.Equal(pcServer.Name, pcClient.Name);
                Assert.StartsWith("CLI", pcClient.Name);
            }

        }

        // ------------------------------------------------------------------------
        // Update Client - Update Server
        // ------------------------------------------------------------------------

        /// <summary>
        /// Generate an update on both side; will be resolved as RemoteExistsLocalExists on both side
        /// </summary>
        private async Task<string> Generate_UC_US_Conflict(SyncAgent agent)
        {
            // Conflict product category
            var conflictProductCategoryId = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 6);
            var productCategoryNameClient = "CLI BIKES " + HelperDatabase.GetRandomName();
            var productCategoryNameServer = "SRV BIKES " + HelperDatabase.GetRandomName();

            await agent.RemoteOrchestrator.Provider.AddProductCategoryAsync(conflictProductCategoryId);

            // Init both client and server
            await agent.SynchronizeAsync(setup);

            // Generate an update conflict
            var pc = await agent.RemoteOrchestrator.Provider.GetProductCategoryAsync(conflictProductCategoryId);
            pc.Name = productCategoryNameServer;
            await agent.RemoteOrchestrator.Provider.UpdateProductCategoryAsync(pc);

            pc = await agent.LocalOrchestrator.Provider.GetProductCategoryAsync(conflictProductCategoryId);
            pc.Name = productCategoryNameClient;
            await agent.LocalOrchestrator.Provider.UpdateProductCategoryAsync(pc);

            return conflictProductCategoryId;
        }

        /// <summary>
        /// Generate a conflict when inserting one row on server and the same row on each client
        /// Server should wins the conflict because default behavior
        /// </summary>
        [Fact]
        public virtual async Task Conflict_UC_US_ServerShouldWins()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

            // make a first sync to init the two databases
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            // Execute a sync on all clients and check results
            // Each client will upload its row (conflicting)
            // then download the others client lines + conflict that should be ovewritten on client
            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, serverProvider, options);

                var productCategoryId = await Generate_UC_US_Conflict(agent);

                var s = await agent.SynchronizeAsync(setup);

                Assert.Equal(1, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalResolvedConflicts);

                var pcClient = await clientProvider.GetProductCategoryAsync(productCategoryId);
                var pcServer = await clientProvider.GetProductCategoryAsync(productCategoryId);

                Assert.Equal(pcServer.Name, pcClient.Name);
                Assert.StartsWith("SRV", pcClient.Name);

            }


        }

        /// <summary>
        /// Generate a conflict when inserting one row on server and the same row on each client
        /// Server should wins the conflict because default behavior
        /// </summary>
        [Fact]
        public virtual async Task Conflict_UC_US_ServerShouldWins_CozHandler()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

            // make a first sync to init the two databases
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, serverProvider, options);

                var productCategoryId = await Generate_UC_US_Conflict(agent);

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

                var s = await agent.SynchronizeAsync(setup);

                Assert.Equal(1, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalResolvedConflicts);

                var pcClient = await clientProvider.GetProductCategoryAsync(productCategoryId);
                var pcServer = await clientProvider.GetProductCategoryAsync(productCategoryId);

                Assert.Equal(pcServer.Name, pcClient.Name);
                Assert.StartsWith("SRV", pcClient.Name);
            }


        }

        /// <summary>
        /// Generate a conflict when inserting one row on server and the same row on each client
        /// Server should wins the conflict because default behavior
        /// </summary>
        [Fact]
        public virtual async Task Conflict_UC_US_ClientShouldWins_CozConfiguration()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

            // make a first sync to init the two databases
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, serverProvider, options);

                var productCategoryId = await Generate_UC_US_Conflict(agent);

                agent.Options.ConflictResolutionPolicy = ConflictResolutionPolicy.ClientWins;

                var s = await agent.SynchronizeAsync(setup);

                Assert.Equal(0, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalResolvedConflicts);

                var pcClient = await clientProvider.GetProductCategoryAsync(productCategoryId);
                var pcServer = await clientProvider.GetProductCategoryAsync(productCategoryId);

                Assert.Equal(pcServer.Name, pcClient.Name);
                Assert.StartsWith("CLI", pcClient.Name);
            }
        }

        /// <summary>
        /// Generate a conflict when inserting one row on server and the same row on each client
        /// Server should wins the conflict because default behavior
        /// </summary>
        [Fact]
        public virtual async Task Conflict_UC_US_ClientShouldWins_CozConfiguration_CozHandler()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

            // make a first sync to init the two databases
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            // Execute a sync on all clients and check results
            // Each client will upload its row (conflicting)
            // then download the others client lines + conflict that should be ovewritten on client
            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, serverProvider, options);

                var productCategoryId = await Generate_UC_US_Conflict(agent);

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

                var s = await agent.SynchronizeAsync(setup);

                Assert.Equal(0, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalResolvedConflicts);

                var pcClient = await clientProvider.GetProductCategoryAsync(productCategoryId);
                var pcServer = await clientProvider.GetProductCategoryAsync(productCategoryId);

                Assert.Equal(pcServer.Name, pcClient.Name);
                Assert.StartsWith("CLI", pcClient.Name);
            }
        }

        /// <summary>
        /// Generate a conflict when inserting one row on server and the same row on each client
        /// Client should wins coz handler
        /// </summary>
        [Fact]
        public virtual async Task Conflict_UC_US_ClientShouldWins_CozHandler()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

            // make a first sync to init the two databases
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, serverProvider, options);

                var productCategoryId = await Generate_UC_US_Conflict(agent);

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

                var s = await agent.SynchronizeAsync(setup);

                Assert.Equal(0, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalResolvedConflicts);

                var pcClient = await clientProvider.GetProductCategoryAsync(productCategoryId);
                var pcServer = await clientProvider.GetProductCategoryAsync(productCategoryId);

                Assert.Equal(pcServer.Name, pcClient.Name);
                Assert.StartsWith("CLI", pcClient.Name);
            }
        }

        /// <summary>
        /// Generate a conflict when inserting one row on server and the same row on each client
        /// Client should wins coz handler
        /// </summary>
        [Fact]
        public virtual async Task Conflict_UC_US_Resolved_ByMerge()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

            // make a first sync to init the two databases
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, serverProvider, options);

                var productCategoryId = await Generate_UC_US_Conflict(agent);

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


                var s = await agent.SynchronizeAsync(setup);

                Assert.Equal(1, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalResolvedConflicts);

                var pcClient = await clientProvider.GetProductCategoryAsync(productCategoryId);
                var pcServer = await clientProvider.GetProductCategoryAsync(productCategoryId);

                Assert.Equal(pcServer.Name, pcClient.Name);
                Assert.StartsWith("BOTH", pcClient.Name);
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
        private async Task<string> Generate_DC_US_Conflict(SyncAgent agent)
        {

            // Conflict product category
            var conflictProductCategoryId = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 6);
            var productCategoryNameClient = "CLI BIKES " + HelperDatabase.GetRandomName();
            var productCategoryNameServer = "SRV BIKES " + HelperDatabase.GetRandomName();

            await agent.RemoteOrchestrator.Provider.AddProductCategoryAsync(conflictProductCategoryId);

            // Init both client and server
            await agent.SynchronizeAsync(setup);

            // Generate an delete update conflict
            // Update on server
            var pc = await agent.RemoteOrchestrator.Provider.GetProductCategoryAsync(conflictProductCategoryId);
            pc.Name = productCategoryNameServer;
            await agent.RemoteOrchestrator.Provider.UpdateProductCategoryAsync(pc);

            // Delete on client
            await agent.LocalOrchestrator.Provider.DeleteProductCategoryAsync(conflictProductCategoryId);

            return conflictProductCategoryId;
        }

        [Fact]
        public virtual async Task Conflict_DC_US_ClientShouldWins()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

            // make a first sync to init the two databases
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, serverProvider, options);
                var productCategoryId = await Generate_DC_US_Conflict(agent);

                agent.Options.ConflictResolutionPolicy = ConflictResolutionPolicy.ClientWins;

                var s = await agent.SynchronizeAsync(setup);

                Assert.Equal(0, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalResolvedConflicts);

                var pcClient = await clientProvider.GetProductCategoryAsync(productCategoryId);
                Assert.Null(pcClient);
                var pcServer = await clientProvider.GetProductCategoryAsync(productCategoryId);
                Assert.Null(pcServer);
            }
        }

        [Fact]
        public virtual async Task Conflict_DC_US_ClientShouldWins_CozHandler()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

            // make a first sync to init the two databases
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, serverProvider, options);
                var productCategoryId = await Generate_DC_US_Conflict(agent);

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

                    Assert.Equal(ConflictResolution.ServerWins, acf.Resolution);
                    Assert.Equal(ConflictType.RemoteIsDeletedLocalExists, conflict.Type);

                    acf.Resolution = ConflictResolution.ClientWins;
                });

                var s = await agent.SynchronizeAsync(setup);

                Assert.Equal(0, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalResolvedConflicts);

                var pcClient = await clientProvider.GetProductCategoryAsync(productCategoryId);
                Assert.Null(pcClient);
                var pcServer = await clientProvider.GetProductCategoryAsync(productCategoryId);
                Assert.Null(pcServer);
            }
        }


        [Fact]
        public virtual async Task Conflict_DC_US_ServerShouldWins()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

            // make a first sync to init the two databases
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, serverProvider, options);
                var productCategoryId = await Generate_DC_US_Conflict(agent);

                var s = await agent.SynchronizeAsync(setup);

                Assert.Equal(1, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalResolvedConflicts);

                var pcClient = await clientProvider.GetProductCategoryAsync(productCategoryId);
                var pcServer = await clientProvider.GetProductCategoryAsync(productCategoryId);

                Assert.Equal(pcServer.Name, pcClient.Name);
                Assert.StartsWith("SRV", pcClient.Name);
            }
        }

        [Fact]
        public virtual async Task Conflict_DC_US_ServerShouldWins_CozHandler()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

            // make a first sync to init the two databases
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, serverProvider, options);
                var productCategoryId = await Generate_DC_US_Conflict(agent);

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

                var s = await agent.SynchronizeAsync(setup);

                Assert.Equal(1, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalResolvedConflicts);

                var pcClient = await clientProvider.GetProductCategoryAsync(productCategoryId);
                var pcServer = await clientProvider.GetProductCategoryAsync(productCategoryId);

                Assert.Equal(pcServer.Name, pcClient.Name);
                Assert.StartsWith("SRV", pcClient.Name);
            }
        }

        // ------------------------------------------------------------------------
        // Update Client When Outdated
        // ------------------------------------------------------------------------

        /// <summary>
        /// Generate an outdated conflict. Both lines exists on both side but server has cleaned metadatas
        /// </summary>
        private async Task Generate_UC_OUTDATED_Conflict(SyncAgent agent)
        {
            // Insert the conflict product category on each client
            var productId = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 6);
            var productCategoryNameClient = HelperDatabase.GetRandomName("CLI");

            await agent.LocalOrchestrator.Provider.AddProductCategoryAsync(productId, name: productCategoryNameClient);

            // Since we may have an Outdated situation due to previous client, go for a Reinitialize sync type
            await agent.SynchronizeAsync(SyncType.ReinitializeWithUpload);

            // Generation of an outdated mark on the server
            var ts = await agent.RemoteOrchestrator.GetLocalTimestampAsync();
            await agent.RemoteOrchestrator.DeleteMetadatasAsync(ts + 1);
        }


        [Fact]
        public virtual async Task Conflict_UC_OUTDATED_ServerShouldWins()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

            // make a first sync to init the two databases
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, serverProvider, options);

                await Generate_UC_OUTDATED_Conflict(agent);

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

                var rowsCount = serverProvider.GetDatabaseRowsCount();

                Assert.Equal(rowsCount, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);
            }
        }

        [Fact]
        public virtual async Task Conflict_UC_OUTDATED_ServerShouldWins_EvenIf_ResolutionIsClientWins()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

            // make a first sync to init the two databases
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            foreach (var clientProvider in clientsProvider)
            {
                options.ConflictResolutionPolicy = ConflictResolutionPolicy.ClientWins;

                var agent = new SyncAgent(clientProvider, serverProvider, options);

                await Generate_UC_OUTDATED_Conflict(agent);

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

                var rowsCount = serverProvider.GetDatabaseRowsCount();

                Assert.Equal(rowsCount, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);
            }
        }

        //// ------------------------------------------------------------------------
        //// Update Client - Delete Server
        //// ------------------------------------------------------------------------


        /// <summary>
        /// Generate an update on the client and delete on the server; will be resolved as:
        /// - RemoteExistsLocalIsDeleted from the server side POV
        /// - RemoteIsDeletedLocalExists from the client side POV
        /// </summary>
        private async Task<string> Generate_UC_DS_Conflict(SyncAgent agent)
        {
            var productId = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 6);
            var productCategoryName = HelperDatabase.GetRandomName("CLI");
            var productCategoryNameUpdated = HelperDatabase.GetRandomName("CLI_UPDATED");

            // Insert a product category and sync it on all clients
            await agent.RemoteOrchestrator.Provider.AddProductCategoryAsync(productId, name: productCategoryName);

            // Execute a sync to initialize client and server schema 
            await agent.SynchronizeAsync();

            // Update product category on each client
            var pc = await agent.LocalOrchestrator.Provider.GetProductCategoryAsync(productId);
            pc.Name = productCategoryNameUpdated;
            await agent.LocalOrchestrator.Provider.UpdateProductCategoryAsync(pc);

            // Delete on Server
            await agent.RemoteOrchestrator.Provider.DeleteProductCategoryAsync(productId);

            return productId;
        }

        [Fact]
        public virtual async Task Conflict_UC_DS_ServerShouldWins()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

            // make a first sync to init the two databases
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, serverProvider, options);

                var productCategoryId = await Generate_UC_DS_Conflict(agent);

                var s = await agent.SynchronizeAsync(setup);

                Assert.Equal(1, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalResolvedConflicts);

                var pcClient = await clientProvider.GetProductCategoryAsync(productCategoryId);
                Assert.Null(pcClient);
                var pcServer = await clientProvider.GetProductCategoryAsync(productCategoryId);
                Assert.Null(pcServer);
            }

        }

        [Fact]
        public virtual async Task Conflict_UC_DS_ServerShouldWins_CozHandler()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

            // make a first sync to init the two databases
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, serverProvider, options);

                var productCategoryId = await Generate_UC_DS_Conflict(agent);

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

                var s = await agent.SynchronizeAsync(setup);

                Assert.Equal(1, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalResolvedConflicts);

                var pcClient = await clientProvider.GetProductCategoryAsync(productCategoryId);
                Assert.Null(pcClient);
                var pcServer = await clientProvider.GetProductCategoryAsync(productCategoryId);
                Assert.Null(pcServer);
            }

        }

        [Fact]
        public virtual async Task Conflict_UC_DS_ClientShouldWins()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

            // make a first sync to init the two databases
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, serverProvider, options);
                var productCategoryId = await Generate_UC_DS_Conflict(agent);

                // Resolution is set to client side
                options.ConflictResolutionPolicy = ConflictResolutionPolicy.ClientWins;

                var s = await agent.SynchronizeAsync(setup);

                Assert.Equal(0, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalResolvedConflicts);

                var pcClient = await clientProvider.GetProductCategoryAsync(productCategoryId);
                Assert.NotNull(pcClient);
                var pcServer = await clientProvider.GetProductCategoryAsync(productCategoryId);
                Assert.NotNull(pcServer);
            }

        }

        [Fact]
        public virtual async Task Conflict_UC_DS_ClientShouldWins_CozHandler()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

            // make a first sync to init the two databases
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, serverProvider, options);
                var productCategoryId = await Generate_UC_DS_Conflict(agent);

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

                    Assert.Equal(ConflictResolution.ServerWins, acf.Resolution);
                    Assert.Equal(ConflictType.RemoteExistsLocalIsDeleted, conflict.Type);

                    acf.Resolution = ConflictResolution.ClientWins;
                });

                var s = await agent.SynchronizeAsync(setup);

                Assert.Equal(0, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalResolvedConflicts);

                var pcClient = await clientProvider.GetProductCategoryAsync(productCategoryId);
                Assert.NotNull(pcClient);
                var pcServer = await clientProvider.GetProductCategoryAsync(productCategoryId);
                Assert.NotNull(pcServer);
            }

        }

        // ------------------------------------------------------------------------
        // Delete Client - Delete Server
        // ------------------------------------------------------------------------

        /// <summary>
        /// Generate a deleted row on the server and on the client, it's resolved as:
        /// - RemoteIsDeletedLocalIsDeleted from both side POV
        /// </summary>
        private async Task<string> Generate_DC_DS_Conflict(SyncAgent agent)
        {
            var productId = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 6);

            // Insert a product category and sync it on all clients
            await agent.RemoteOrchestrator.Provider.AddProductCategoryAsync(productId);

            // Execute a sync to initialize client and server schema 
            await agent.SynchronizeAsync();

            // Delete on client
            await agent.LocalOrchestrator.Provider.DeleteProductCategoryAsync(productId);

            // Delete on Server
            await agent.RemoteOrchestrator.Provider.DeleteProductCategoryAsync(productId);

            return productId;
        }

        [Fact]
        public virtual async Task Conflict_DC_DS_ServerShouldWins()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

            // make a first sync to init the two databases
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, serverProvider, options);
                var productCategoryId = await Generate_DC_DS_Conflict(agent);

                var s = await agent.SynchronizeAsync(setup);

                Assert.Equal(1, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalResolvedConflicts);

                var pcClient = await clientProvider.GetProductCategoryAsync(productCategoryId);
                Assert.Null(pcClient);
                var pcServer = await clientProvider.GetProductCategoryAsync(productCategoryId);
                Assert.Null(pcServer);
            }
        }

        [Fact]
        public virtual async Task Conflict_DC_DS_ServerShouldWins_CozHandler()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

            // make a first sync to init the two databases
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, serverProvider, options);
                var productCategoryId = await Generate_DC_DS_Conflict(agent);

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

                var s = await agent.SynchronizeAsync(setup);

                Assert.Equal(1, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalResolvedConflicts);

                var pcClient = await clientProvider.GetProductCategoryAsync(productCategoryId);
                Assert.Null(pcClient);
                var pcServer = await clientProvider.GetProductCategoryAsync(productCategoryId);
                Assert.Null(pcServer);
            }

        }

        [Fact]
        public virtual async Task Conflict_DC_DS_ClientShouldWins()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

            // make a first sync to init the two databases
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, serverProvider, options);
                var productCategoryId = await Generate_DC_DS_Conflict(agent);

                options.ConflictResolutionPolicy = ConflictResolutionPolicy.ClientWins;

                var s = await agent.SynchronizeAsync(setup);

                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalChangesAppliedOnServer);
                Assert.Equal(1, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesAppliedOnClient);
                Assert.Equal(1, s.TotalResolvedConflicts);

                var pcClient = await clientProvider.GetProductCategoryAsync(productCategoryId);
                Assert.Null(pcClient);
                var pcServer = await clientProvider.GetProductCategoryAsync(productCategoryId);
                Assert.Null(pcServer);
            }
        }

        [Fact]
        public virtual async Task Conflict_DC_DS_ClientShouldWins_CozHandler()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

            // make a first sync to init the two databases
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, serverProvider, options);

                var productCategoryId = await Generate_DC_DS_Conflict(agent);

                var localOrchestrator = agent.LocalOrchestrator;
                var remoteOrchestrator = agent.RemoteOrchestrator;

                // From client : Remote is server, Local is client
                localOrchestrator.OnApplyChangesConflictOccured(acf =>
                {
                    Debug.WriteLine("Should not happen since Client is the winner of the conflict and conflict has been resolved on the server side");
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

                    acf.Resolution = ConflictResolution.ClientWins;
                });

                var s = await agent.SynchronizeAsync(setup);

                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalChangesAppliedOnServer);
                Assert.Equal(1, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesAppliedOnClient);
                Assert.Equal(1, s.TotalResolvedConflicts);

                var pcClient = await clientProvider.GetProductCategoryAsync(productCategoryId);
                Assert.Null(pcClient);
                var pcServer = await clientProvider.GetProductCategoryAsync(productCategoryId);
                Assert.Null(pcServer);
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
        private async Task<string> Generate_DC_NULLS_Conflict(SyncAgent agent)
        {
            // Insert a product category on  clients
            var productCategory = await agent.LocalOrchestrator.Provider.AddProductCategoryAsync();
            // Then delete it
            await agent.LocalOrchestrator.Provider.DeleteProductCategoryAsync(productCategory.ProductCategoryId);
            // So far we have a row marked as deleted in the tracking table.
            return productCategory.ProductCategoryId;
        }

        [Fact]
        public virtual async Task Conflict_DC_NULLS_ServerShouldWins()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

            // make a first sync to init the two databases
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, serverProvider, options);
                var productCategoryId = await Generate_DC_NULLS_Conflict(agent);

                var s = await agent.SynchronizeAsync(setup);

                Assert.Equal(0, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalResolvedConflicts);

                var pcClient = await clientProvider.GetProductCategoryAsync(productCategoryId);
                Assert.Null(pcClient);
                var pcServer = await clientProvider.GetProductCategoryAsync(productCategoryId);
                Assert.Null(pcServer);
            }

        }

        [Fact]
        public virtual async Task Conflict_DC_NULLS_ServerShouldWins_CozHandler()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

            // make a first sync to init the two databases
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, serverProvider, options);
                var productCategoryId = await Generate_DC_NULLS_Conflict(agent);

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

                var s = await agent.SynchronizeAsync(setup);

                Assert.Equal(0, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalResolvedConflicts);

                var pcClient = await clientProvider.GetProductCategoryAsync(productCategoryId);
                Assert.Null(pcClient);
                var pcServer = await clientProvider.GetProductCategoryAsync(productCategoryId);
                Assert.Null(pcServer);
            }

        }

        [Fact]
        public virtual async Task Conflict_DC_NULLS_ClientShouldWins()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

            // make a first sync to init the two databases
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            // since we have no interceptor, we don't know what kind of conflict (delete - no rows)
            // is happening. So server will try to apply the delete and will add the metadata
            // then it's normal to download this delete on next client
            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, serverProvider, options);
                var productCategoryId = await Generate_DC_NULLS_Conflict(agent);

                // Set conflict resolution to client
                options.ConflictResolutionPolicy = ConflictResolutionPolicy.ClientWins;

                var s = await agent.SynchronizeAsync(setup);

                Assert.Equal(0, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalResolvedConflicts);

                var pcClient = await clientProvider.GetProductCategoryAsync(productCategoryId);
                Assert.Null(pcClient);
                var pcServer = await clientProvider.GetProductCategoryAsync(productCategoryId);
                Assert.Null(pcServer);
            }
        }

        [Fact]
        public virtual async Task Conflict_DC_NULLS_ClientShouldWins_CozHandler()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

            // make a first sync to init the two databases
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, serverProvider, options);
                var productCategoryId = await Generate_DC_NULLS_Conflict(agent);

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

                    Assert.Equal(ConflictResolution.ServerWins, acf.Resolution);
                    Assert.Equal(ConflictType.RemoteIsDeletedLocalNotExists, conflict.Type);
                    Assert.Equal(SyncRowState.Deleted, conflict.RemoteRow.RowState);
                    Assert.Null(localRow);
                    acf.Resolution = ConflictResolution.ClientWins;

                });

                var s = await agent.SynchronizeAsync(setup);

                Assert.Equal(0, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalResolvedConflicts);

                var pcClient = await clientProvider.GetProductCategoryAsync(productCategoryId);
                Assert.Null(pcClient);
                var pcServer = await clientProvider.GetProductCategoryAsync(productCategoryId);
                Assert.Null(pcServer);
            }

        }

        /// <summary>
        /// Generate a deleted row on Server, that does not exists on Client, it's resolved as:
        /// </summary>
        private async Task<string> Generate_NULLC_DS_Conflict(SyncAgent agent)
        {
            var productCategory = await agent.RemoteOrchestrator.Provider.AddProductCategoryAsync();
            // Then delete it
            await agent.RemoteOrchestrator.Provider.DeleteProductCategoryAsync(productCategory.ProductCategoryId);
            // So far we have a row marked as deleted in the tracking table.
            return productCategory.ProductCategoryId;
        }


        [Fact]
        public virtual async Task Conflict_NULLC_DS_ServerShouldWins()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

            // make a first sync to init the two databases
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            var download = 1;
            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, serverProvider, options);
                var productCategoryId = await Generate_NULLC_DS_Conflict(agent);

                var s = await agent.SynchronizeAsync(setup);

                Assert.Equal(download, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalChangesAppliedOnClient);
                Assert.Equal(download, s.TotalResolvedConflicts);

                var pcClient = await clientProvider.GetProductCategoryAsync(productCategoryId);
                Assert.Null(pcClient);
                var pcServer = await clientProvider.GetProductCategoryAsync(productCategoryId);
                Assert.Null(pcServer);
                download++;
            }

        }

        [Fact]
        public virtual async Task Conflict_NULLC_DS_ServerShouldWins_CozHandler()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

            // make a first sync to init the two databases
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            var download = 1;
            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, serverProvider, options);
                var productCategoryId = await Generate_NULLC_DS_Conflict(agent);

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

                var s = await agent.SynchronizeAsync(setup);

                Assert.Equal(download, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalChangesAppliedOnClient);
                Assert.Equal(download, s.TotalResolvedConflicts);

                var pcClient = await clientProvider.GetProductCategoryAsync(productCategoryId);
                Assert.Null(pcClient);
                var pcServer = await clientProvider.GetProductCategoryAsync(productCategoryId);
                Assert.Null(pcServer);
                download++;
            }
        }

        [Fact]
        public virtual async Task Conflict_NULLC_DS_ClientShouldWins()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

            // make a first sync to init the two databases
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            var download = 1;
            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, serverProvider, options);
                var productCategoryId = await Generate_NULLC_DS_Conflict(agent);

                // Set conflict resolution to client
                options.ConflictResolutionPolicy = ConflictResolutionPolicy.ClientWins;

                var s = await agent.SynchronizeAsync(setup);

                Assert.Equal(download, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalChangesAppliedOnClient);
                Assert.Equal(download, s.TotalResolvedConflicts);

                var pcClient = await clientProvider.GetProductCategoryAsync(productCategoryId);
                Assert.Null(pcClient);
                var pcServer = await clientProvider.GetProductCategoryAsync(productCategoryId);
                Assert.Null(pcServer);
                download++;
            }

        }

        [Fact]
        public virtual async Task Conflict_NULLC_DS_ClientShouldWins_CozHandler()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

            // make a first sync to init the two databases
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            var download = 1;
            foreach (var clientProvider in clientsProvider)
            {
                var agent = new SyncAgent(clientProvider, serverProvider, options);
                var productCategoryId = await Generate_NULLC_DS_Conflict(agent);

                var localOrchestrator = agent.LocalOrchestrator;
                var remoteOrchestrator = agent.RemoteOrchestrator;

                // Set conflict resolution to client
                options.ConflictResolutionPolicy = ConflictResolutionPolicy.ClientWins;

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

                var s = await agent.SynchronizeAsync(setup);

                Assert.Equal(download, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalChangesAppliedOnClient);
                Assert.Equal(download, s.TotalResolvedConflicts);

                var pcClient = await clientProvider.GetProductCategoryAsync(productCategoryId);
                Assert.Null(pcClient);
                var pcServer = await clientProvider.GetProductCategoryAsync(productCategoryId);
                Assert.Null(pcServer);
                download++;
            }
        }

        /// Generate a conflict when inserting one row on server and the same row on each client
        /// Server should wins the conflict because default behavior
        /// </summary>
        [Fact]
        public virtual async Task Conflict_UC_US_ClientChoosedTheWinner()
        {
            var options = new SyncOptions { DisableConstraintsOnApplyChanges = true };

            // make a first sync to init the two databases
            foreach (var clientProvider in clientsProvider)
                await new SyncAgent(clientProvider, serverProvider, options).SynchronizeAsync(setup);

            foreach (var clientProvider in clientsProvider)
            {
                var clientNameDecidedOnClientMachine = HelperDatabase.GetRandomName();

                var agent = new SyncAgent(clientProvider, serverProvider, options);
                var productCategoryId = await Generate_UC_US_Conflict(agent);

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
                var s = await agent.SynchronizeAsync(setup);

                Assert.Equal(1, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(1, s.TotalResolvedConflicts);

                // From this point the Server row Name is "SRV...."
                // And the Client row NAME is "UI_CLIENT..."
                // Make a new sync to send "UI_CLIENT..." to Server

                s = await agent.SynchronizeAsync(setup);


                Assert.Equal(0, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);

                var pcClient = await clientProvider.GetProductCategoryAsync(productCategoryId);
                Assert.Equal(clientNameDecidedOnClientMachine, pcClient.Name);
                var pcServer = await serverProvider.GetProductCategoryAsync(productCategoryId);
                Assert.Equal(clientNameDecidedOnClientMachine, pcServer.Name);
            }
        }
    }
}
