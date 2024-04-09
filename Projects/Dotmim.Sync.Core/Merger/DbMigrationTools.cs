using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    //public class DbMigrationTools
    //{
    //    private CoreProvider provider;
    //    private readonly SyncOptions newOptions;
    //    private SyncSetup newSetup;
    //    private readonly string currentScopeInfoTableName;

    //    public DbMigrationTools() { }

    //    public DbMigrationTools(CoreProvider provider, SyncOptions options, SyncSetup setup, string currentScopeInfoTableName = null)
    //    {
    //        this.provider = provider;
    //        this.newOptions = options;
    //        this.newSetup = setup;
    //        this.currentScopeInfoTableName = currentScopeInfoTableName;
    //    }


    //    public Task MigrateAsync(SyncContext context)
    //    {

    //        return null;
    //        //DbTransaction transaction = null;

    //        //using (var connection = this.provider.CreateConnection())
    //        //{
    //        //    // Encapsulate in a try catch for a better exception handling
    //        //    // Especially whe called from web proxy
    //        //    try
    //        //    {
    //        //        await connection.OpenAsync().ConfigureAwait(false);

    //        //        // Let provider knows a connection is opened
    //        //        this.provider.OnConnectionOpened(connection);

    //        //        await this.provider.InterceptAsync(new ConnectionOpenArgs(context, connection)).ConfigureAwait(false);

    //        //        // Create a transaction
    //        //        using (transaction = connection.BeginTransaction(this.provider.IsolationLevel))
    //        //        {

    //        //            await this.provider.InterceptAsync(new TransactionOpenArgs(context, connection, transaction)).ConfigureAwait(false);


    //        //            // actual scope info table name
    //        //            var scopeInfoTableName = string.IsNullOrEmpty(this.currentScopeInfoTableName) ? this.newOptions.ScopeInfoTableName : this.currentScopeInfoTableName;

    //        //            // create a temp sync context
    //        //            ScopeInfo localScope = null;

    //        //            var scopeBuilder = this.provider.GetScopeBuilder();
    //        //            var scopeInfoBuilder = scopeBuilder.CreateScopeInfoBuilder(scopeInfoTableName, connection, transaction);

    //        //            // if current scope table name does not exists, it's probably first sync. so return
    //        //            if (scopeInfoBuilder.NeedToCreateClientScopeInfoTable())
    //        //                return;

    //        //            // Get scope
    //        //            (context, localScope) = await this.provider.GetClientScopeAsync(
    //        //                                context, this.newOptions.ScopeName,
    //        //                                connection, transaction, CancellationToken.None).ConfigureAwait(false);

    //        //            // Get current schema saved in local database
    //        //            if (localScope == null || string.IsNullOrEmpty(localScope.Schema))
    //        //                return;

    //        //            var currentSchema = JsonsSerializer.Deserialize<SyncSet>(localScope.Schema);

    //        //            // Create new schema based on new setup
    //        //            var newSchema = this.provider.ReadSchema(this.newSetup, connection, transaction);


    //        //            // Get Tables that are NOT in new schema anymore
    //        //            var deletedSyncTables = currentSchema.Tables.Where(currentTable => !newSchema.Tables.Any(newTable => newTable == currentTable));

    //        //            foreach (var dSyncTable in deletedSyncTables)
    //        //            {
    //        //                // get builder
    //        //                var delBuilder = this.provider.GetTableBuilder(dSyncTable);

    //        //                // Delete all stored procedures
    //        //                delBuilder.DropProcedures(connection, transaction);

    //        //                // Delete all triggers
    //        //                delBuilder.DropTriggers(connection, transaction);

    //        //                // Delete tracking table
    //        //                delBuilder.DropTrackingTable(connection, transaction);
    //        //            }

    //        //            // Get Tables that are completely new
    //        //            var addSyncTables = newSchema.Tables.Where(newTable => !currentSchema.Tables.Any(currentTable => newTable == currentTable));

    //        //            foreach (var aSyncTable in addSyncTables)
    //        //            {
    //        //                // get builder
    //        //                var addBuilder = this.provider.GetTableBuilder(aSyncTable);

    //        //                // Create table if not exists
    //        //                addBuilder.CreateTable(connection, transaction);

    //        //                // Create tracking table
    //        //                addBuilder.CreateTrackingTable(connection, transaction);

    //        //                // Create triggers
    //        //                addBuilder.CreateTriggers(connection, transaction);

    //        //                // Create stored procedures
    //        //                addBuilder.CreateStoredProcedures(connection, transaction);
    //        //            }

    //        //            var editSyncTables = newSchema.Tables.Where(newTable => currentSchema.Tables.Any(currentTable => newTable == currentTable));

    //        //            foreach (var eSyncTable in editSyncTables)
    //        //            {
    //        //                var cSyncTable = currentSchema.Tables.First(t => t == eSyncTable);

    //        //                var migrationTable = new DbMigrationTable(this.provider, cSyncTable, eSyncTable, true);
    //        //                migrationTable.Compare();
    //        //                //migrationTable.Apply(connection, transaction);
    //        //            }

    //        //            await this.provider.InterceptAsync(new TransactionCommitArgs(null, connection, transaction)).ConfigureAwait(false);
    //        //            transaction.Commit();
    //        //        }
    //        //    }
    //        //    catch (Exception ex)
    //        //    {

    //        //        var syncException = new SyncException(ex, context.SyncStage);

    //        //        // try to let the provider enrich the exception
    //        //        this.provider.EnsureSyncException(syncException);
    //        //        syncException.Side = SyncExceptionSide.ClientSide;
    //        //        throw syncException;
    //        //    }
    //        //    finally
    //        //    {
    //        //        if (transaction != null)
    //        //            transaction.Dispose();

    //        //        if (connection != null && connection.State == ConnectionState.Open)
    //        //            connection.Close();

    //        //        await this.provider.InterceptAsync(new ConnectionCloseArgs(context, connection, transaction)).ConfigureAwait(false);

    //        //        // Let provider knows a connection is closed
    //        //        this.provider.OnConnectionClosed(connection);
    //        //    }

    //        //}



    //    }



    //    public void Apply(DbConnection connection, DbTransaction transaction = null)
    //    {

    //        //var alreadyOpened = connection.State != ConnectionState.Closed;

    //        //if (!alreadyOpened)
    //        //    connection.Open();

    //        //// get builder
    //        //var newBuilder = this.provider.GetTableBuilder(this.newTable);
    //        //var currentBuilder = this.provider.GetTableBuilder(this.currentTable);

    //        //if (NeedRecreateTrackingTable)
    //        //{
    //        //    // drop triggers, stored procedures, tvp, tracking table
    //        //    currentBuilder.DropTriggers(connection, transaction);
    //        //    currentBuilder.DropProcedures(connection, transaction);
    //        //    currentBuilder.DropTrackingTable(connection, transaction);

    //        //    newBuilder.CreateTrackingTable(connection, transaction);
    //        //    newBuilder.CreateStoredProcedures(connection, transaction);
    //        //    newBuilder.CreateTriggers(connection, transaction);

    //        //    this.NeedRecreateStoredProcedures = false;
    //        //    this.NeedRecreateTriggers = false;
    //        //    this.NeedRecreateTrackingTable = false;
    //        //    this.NeedRenameTrackingTable = false;
    //        //}

    //        //if (NeedRecreateStoredProcedures)
    //        //{
    //        //    currentBuilder.DropProcedures(connection, transaction);
    //        //    newBuilder.CreateStoredProcedures(connection, transaction);

    //        //    this.NeedRecreateStoredProcedures = false;
    //        //}

    //        //if (NeedRecreateTriggers)
    //        //{
    //        //    currentBuilder.DropTriggers(connection, transaction);
    //        //    newBuilder.CreateTriggers(connection, transaction);

    //        //    this.NeedRecreateTriggers = false;
    //        //}

    //        //if (!alreadyOpened)
    //        //    connection.Close();

    //    }


    //}
}
