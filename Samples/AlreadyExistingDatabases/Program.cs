﻿using Dotmim.Sync;
using Dotmim.Sync.Sqlite;
using Dotmim.Sync.SqlServer;
using System;
using System.Threading.Tasks;

namespace AlreadyExstingDatabases
{
    internal class Program
    {
        private static string serverConnectionString = $"Data Source=(localdb)\\mssqllocaldb; Initial Catalog=AdventureWorks;Integrated Security=true;";

        private static async Task Main()
        {
            // Database script used for this sample : https://github.com/Mimetis/Dotmim.Sync/blob/master/CreateAdventureWorks.sql
            var serverProvider = new SqlSyncProvider(serverConnectionString);

            // Second provider is using plain old Sql Server provider, relying on triggers and tracking tables to create the sync environment
            var clientProvider = new SqliteSyncProvider("adv.db");

            // Setup two tables with datas in each table
            await SetupDatabasesAsync(serverProvider, clientProvider).ConfigureAwait(false);

            // SynchronizeAsync: Double SynchronizeAsync with UpdateUntrackedRowsAsync
            await SynchronizeAsync(serverProvider, clientProvider).ConfigureAwait(false);
        }

        private static async Task SynchronizeAsync(SqlSyncProvider serverProvider, SqliteSyncProvider clientProvider)
        {
            // Tables involved in the sync process:
            var tables = new string[] { "ServiceTickets" };

            // Creating an agent that will handle all the process
            var agent = new SyncAgent(clientProvider, serverProvider);

            // Launch the sync process
            var s1 = await agent.SynchronizeAsync(tables).ConfigureAwait(false);

            // This first sync did not upload the client rows.
            // We only have rows from server that have been downloaded
            // The important step here is to have setup the sync (triggers / tracking tables ...)
            Console.WriteLine(s1);

            // Now we can "mark" original clients rows as "to be uploaded"
            await agent.LocalOrchestrator.UpdateUntrackedRowsAsync().ConfigureAwait(false);

            // Then we can make a new synchronize to upload these rows to server
            // Launch the sync process
            var s2 = await agent.SynchronizeAsync().ConfigureAwait(false);
            Console.WriteLine(s2);
        }

        /// <summary>
        /// Create the tables in each database
        /// Add some datas in each database
        /// Performs an optional DeprovisionAsync, to be sure we are starting from scratch.
        /// </summary>
        private static async Task SetupDatabasesAsync(SqlSyncProvider serverProvider, SqliteSyncProvider clientProvider)
        {
            // Add some datas in both
            var serverConnection = serverProvider.CreateConnection();
            await Helper.CreateSqlServerServiceTicketsTableAsync(serverConnection).ConfigureAwait(false);
            var clientConnection = clientProvider.CreateConnection();
            await Helper.CreateSqliteServiceTicketsTableAsync(clientConnection).ConfigureAwait(false);

            // Creating an agent that will handle all the process
            var agent = new SyncAgent(clientProvider, serverProvider);

            // Be sure we don't have an already existing sync setup.  (from previous run)
            await agent.LocalOrchestrator.DropAllAsync().ConfigureAwait(false);
            await agent.RemoteOrchestrator.DropAllAsync().ConfigureAwait(false);

            // Be sure we don't have existing rows (from previous run)
            await Helper.DropRowsAsync(serverConnection).ConfigureAwait(false);
            await Helper.DropRowsAsync(clientConnection).ConfigureAwait(false);

            // Add rows
            await Helper.AddRowsAsync(serverConnection).ConfigureAwait(false);
            await Helper.AddRowsAsync(clientConnection).ConfigureAwait(false);
        }
    }
}