using System;
using System.Threading.Tasks;

namespace Dotmim.Sync
{

    public static class InterceptorsExtensions
    {

        private static void SetInterceptor<T>(this BaseOrchestrator orchestrator, Func<T, Task> func) where T : ProgressArgs 
            => orchestrator.On(func);

        private static void SetInterceptor<T>(this BaseOrchestrator orchestrator, Action<T> action) where T : ProgressArgs 
            => orchestrator.On(action);

        /// <summary>
        /// Intercept the provider action whenever a connection is opened
        /// </summary>
        public static void OnConnectionOpen(this BaseOrchestrator orchestrator, Func<ConnectionOpenedArgs, Task> func)
            => orchestrator.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider action whenever a connection is opened
        /// </summary>
        public static void OnConnectionOpen(this BaseOrchestrator orchestrator, Action<ConnectionOpenedArgs> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Occurs when trying to reconnect to a database
        /// </summary>
        public static void OnReConnect(this BaseOrchestrator orchestrator, Func<ReConnectArgs, Task> func)
            => orchestrator.SetInterceptor(func);

        /// <summary>
        /// Occurs when trying to reconnect to a database
        /// </summary>
        public static void OnReConnect(this BaseOrchestrator orchestrator, Action<ReConnectArgs> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider action whenever a transaction is opened
        /// </summary>
        public static void OnTransactionOpen(this BaseOrchestrator orchestrator, Action<TransactionOpenedArgs> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider action whenever a transaction is opened
        /// </summary>
        public static void OnTransactionOpen(this BaseOrchestrator orchestrator, Func<ConnectionOpenedArgs, Task> func)
            => orchestrator.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider action whenever a connection is closed
        /// </summary>
        public static void OnConnectionClose(this BaseOrchestrator orchestrator, Func<ConnectionClosedArgs, Task> func)
            => orchestrator.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider action whenever a connection is closed
        /// </summary>
        public static void OnConnectionClose(this BaseOrchestrator orchestrator, Action<ConnectionClosedArgs> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider action whenever a transaction is commit
        /// </summary>
        public static void OnTransactionCommit(this BaseOrchestrator orchestrator, Action<TransactionCommitArgs> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider action whenever a transaction is commit
        /// </summary>
        public static void OnTransactionCommit(this BaseOrchestrator orchestrator, Func<TransactionCommitArgs, Task> func)
            => orchestrator.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider action when session begin is called
        /// </summary>
        public static void OnOutdated(this BaseOrchestrator orchestrator, Func<OutdatedArgs, Task> func)
            => orchestrator.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider action when session begin is called
        /// </summary>
        public static void OnOutdated(this BaseOrchestrator orchestrator, Action<OutdatedArgs> action)
            => orchestrator.SetInterceptor(action);


        /// <summary>
        /// Intercept the orchestrator when creating a snapshot
        /// </summary>
        public static void OnSnapshotCreating(this BaseOrchestrator orchestrator, Func<SnapshotCreatingArgs, Task> func)
            => orchestrator.SetInterceptor(func);

        /// <summary>
        /// Intercept the orchestrator when creating a snapshot
        /// </summary>
        public static void OnSnapshotCreating(this BaseOrchestrator orchestrator, Action<SnapshotCreatingArgs> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the orchestrator when a snapshot has been created
        /// </summary>
        public static void OnSnapshotCreated(this BaseOrchestrator orchestrator, Func<SnapshotCreatedArgs, Task> func)
            => orchestrator.SetInterceptor(func);

        /// <summary>
        /// Intercept the orchestrator when a snapshot has been created
        /// </summary>
        public static void OnSnapshotCreated(this BaseOrchestrator orchestrator, Action<SnapshotCreatedArgs> action)
            => orchestrator.SetInterceptor(action);


        /// <summary>
        /// Intercept the orchestrator when migrating a Setup
        /// </summary>
        public static void OnDatabaseMigrating(this BaseOrchestrator orchestrator, Func<DatabaseMigratingArgs, Task> func)
            => orchestrator.SetInterceptor(func);

        /// <summary>
        /// Intercept the orchestrator when migrating a Setup
        /// </summary>
        public static void OnDatabaseMigrating(this BaseOrchestrator orchestrator, Action<DatabaseMigratingArgs> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the orchestrator when a Setup has been migrated
        /// </summary>
        public static void OnDatabaseMigrated(this BaseOrchestrator orchestrator, Func<DatabaseMigratedArgs, Task> func)
            => orchestrator.SetInterceptor(func);

        /// <summary>
        /// Intercept the orchestrator when a Setup has been migrated
        /// </summary>
        public static void OnDatabaseMigrated(this BaseOrchestrator orchestrator, Action<DatabaseMigratedArgs> action)
            => orchestrator.SetInterceptor(action);


        /// <summary>
        /// Occurs just before saving a serialized set to disk
        /// </summary>
        public static void OnSerializingSet(this BaseOrchestrator orchestrator, Func<SerializingSetArgs, Task> func)
            => orchestrator.SetInterceptor(func);

        /// <summary>
        /// Occurs just before saving a serialized set to disk
        /// </summary>
        public static void OnSerializingSet(this BaseOrchestrator orchestrator, Action<SerializingSetArgs> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Occurs just after loading a serialized set from disk
        /// </summary>
        public static void OnDeserializingSet(this BaseOrchestrator orchestrator, Func<DeserializingSetArgs, Task> func)
            => orchestrator.SetInterceptor(func);

        /// <summary>
        /// Occurs just after loading a serialized set from disk
        /// </summary>
        public static void OnDeserializingSet(this BaseOrchestrator orchestrator, Action<DeserializingSetArgs> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider when an apply change is failing
        /// </summary>
        public static void OnApplyChangesFailed(this BaseOrchestrator orchestrator, Func<ApplyChangesFailedArgs, Task> func)
            => orchestrator.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider when an apply change is failing
        /// </summary>
        public static void OnApplyChangesFailed(this BaseOrchestrator orchestrator, Action<ApplyChangesFailedArgs> action)
            => orchestrator.SetInterceptor(action);


        /// <summary>
        /// Intercept the provider action when session begin is called
        /// </summary>
        public static void OnSessionBegin(this BaseOrchestrator orchestrator, Func<SessionBeginArgs, Task> func)
            => orchestrator.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider action when session begin is called
        /// </summary>
        public static void OnSessionBegin(this BaseOrchestrator orchestrator, Action<SessionBeginArgs> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider action when session end is called
        /// </summary>
        public static void OnSessionEnd(this BaseOrchestrator orchestrator, Func<SessionEndArgs, Task> func)
            => orchestrator.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider action when session end is called
        /// </summary>
        public static void OnSessionEnd(this BaseOrchestrator orchestrator, Action<SessionEndArgs> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider action when a scope is about to be loaded from client database
        /// </summary>
        public static void OnScopeLoading(this BaseOrchestrator orchestrator, Func<ScopeLoadingArgs, Task> func)
            => orchestrator.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider action when a scope is about to be loaded from client database
        /// </summary>
        public static void OnScopeLoading(this BaseOrchestrator orchestrator, Action<ScopeLoadingArgs> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider action when a scope is loaded from client database
        /// </summary>
        public static void OnScopeLoaded<T>(this BaseOrchestrator orchestrator, Func<ScopeLoadedArgs<ScopeInfo>, Task> func)
            => orchestrator.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider action when a scope is loaded from client database
        /// </summary>
        public static void OnScopeLoaded(this BaseOrchestrator orchestrator, Action<ScopeLoadedArgs<ScopeInfo>> action)
            => orchestrator.SetInterceptor(action);


        /// <summary>
        /// Intercept the provider action when a scope is about to be loaded from server database
        /// </summary>
        public static void OnServerScopeLoading(this BaseOrchestrator orchestrator, Func<ScopeLoadingArgs, Task> func)
            => orchestrator.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider action when a scope is about to be loaded from ServerScope database
        /// </summary>
        public static void OnServerScopeScopeLoading(this BaseOrchestrator orchestrator, Action<ScopeLoadingArgs> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider action when a scope is loaded from Server database
        /// </summary>
        public static void OnServerScopeLoaded(this BaseOrchestrator orchestrator, Func<ScopeLoadedArgs<ServerScopeInfo>, Task> func)
            => orchestrator.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider action when a scope is loaded from Server database
        /// </summary>
        public static void OnServerScopeLoaded(this BaseOrchestrator orchestrator, Action<ScopeLoadedArgs<ServerScopeInfo>> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider when schema is readed
        /// </summary>
        public static void OnSchemaRead(this BaseOrchestrator orchestrator, Func<SchemaArgs, Task> func)
            => orchestrator.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider when schema is readed
        /// </summary>
        public static void OnSchemaRead(this BaseOrchestrator orchestrator, Action<SchemaArgs> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider before it begins a database deprovisioning
        /// </summary>
        public static void OnDatabaseDeprovisioning(this BaseOrchestrator orchestrator, Func<DatabaseDeprovisioningArgs, Task> func)
            => orchestrator.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider before it begins a database deprovisioning
        /// </summary>
        public static void OnDatabaseDeprovisioning(this BaseOrchestrator orchestrator, Action<DatabaseDeprovisioningArgs> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider after it has deprovisioned a database
        /// </summary>
        public static void OnDatabaseDeprovisioned(this BaseOrchestrator orchestrator, Func<DatabaseDeprovisionedArgs, Task> func)
            => orchestrator.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider after it has deprovisioned a database
        /// </summary>
        public static void OnDatabaseDeprovisioned(this BaseOrchestrator orchestrator, Action<DatabaseDeprovisionedArgs> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider after it has deprovisioned a table
        /// </summary>
        public static void OnTableDeprovisioned(this BaseOrchestrator orchestrator, Func<TableDeprovisionedArgs, Task> func)
            => orchestrator.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider after it has deprovisioned a table
        /// </summary>
        public static void OnTableDeprovisioned(this BaseOrchestrator orchestrator, Action<TableDeprovisionedArgs> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider before it begins a database provisioning
        /// </summary>
        public static void OnDatabaseProvisioning(this BaseOrchestrator orchestrator, Func<DatabaseProvisioningArgs, Task> func)
            => orchestrator.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider before it begins a database provisioning
        /// </summary>
        public static void OnDatabaseProvisioning(this BaseOrchestrator orchestrator, Action<DatabaseProvisioningArgs> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider after it has provisioned a database
        /// </summary>
        public static void OnDatabaseProvisioned(this BaseOrchestrator orchestrator, Func<DatabaseProvisionedArgs, Task> func)
            => orchestrator.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider after it has provisioned a database
        /// </summary>
        public static void OnDatabaseProvisioned(this BaseOrchestrator orchestrator, Action<DatabaseProvisionedArgs> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider after it has provisioned a table
        /// </summary>
        public static void OnTableProvisioned(this BaseOrchestrator orchestrator, Func<TableProvisionedArgs, Task> func)
            => orchestrator.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider after it has provisioned a table
        /// </summary>
        public static void OnTableProvisioned(this BaseOrchestrator orchestrator, Action<TableProvisionedArgs> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider before it will provisioned a table
        /// </summary>
        public static void OnTableProvisioning(this BaseOrchestrator orchestrator, Func<TableProvisioningArgs, Task> func)
            => orchestrator.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider before it will provisioned a table
        /// </summary>
        public static void OnTableProvisioning(this BaseOrchestrator orchestrator, Action<TableProvisioningArgs> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider action when changes are going to be selected on each table defined in the configuration schema
        /// </summary>
        public static void OnTableChangesSelecting(this BaseOrchestrator orchestrator, Func<TableChangesSelectingArgs, Task> func)
            => orchestrator.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider action when changes are going to be selected on each table defined in the configuration schema
        /// </summary>
        public static void OnTableChangesSelecting(this BaseOrchestrator orchestrator, Action<TableChangesSelectingArgs> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider action when changes are selected on each table defined in the configuration schema
        /// </summary>
        public static void OnTableChangesSelected(this BaseOrchestrator orchestrator, Func<TableChangesSelectedArgs, Task> func)
            => orchestrator.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider action when changes are selected on each table defined in the configuration schema
        /// </summary>
        public static void OnTableChangesSelected(this BaseOrchestrator orchestrator, Action<TableChangesSelectedArgs> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider action when changes are going to be applied on each table defined in the configuration schema
        /// </summary>
        public static void OnTableChangesApplying(this BaseOrchestrator orchestrator, Func<TableChangesApplyingArgs, Task> func)
            => orchestrator.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider action when changes are going to be applied on each table defined in the configuration schema
        /// </summary>
        public static void OnTableChangesApplying(this BaseOrchestrator orchestrator, Action<TableChangesApplyingArgs> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider action when changes are applied on each table defined in the configuration schema
        /// </summary>
        public static void OnTableChangesApplied(this BaseOrchestrator orchestrator, Func<TableChangesAppliedArgs, Task> func)
            => orchestrator.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider action when changes are applied on each table defined in the configuration schema
        /// </summary>
        public static void OnTableChangesApplied(this BaseOrchestrator orchestrator, Action<TableChangesAppliedArgs> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider action when changes are going to be applied on each table defined in the configuration schema
        /// </summary>
        public static void OnDatabaseChangesApplying(this BaseOrchestrator orchestrator, Func<DatabaseChangesApplyingArgs, Task> func)
            => orchestrator.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider action when changes are going to be applied on each table defined in the configuration schema
        /// </summary>
        public static void OnDatabaseChangesApplying(this BaseOrchestrator orchestrator, Action<DatabaseChangesApplyingArgs> func)
            => orchestrator.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider action when changes are applied on each table defined in the configuration schema
        /// </summary>
        public static void OnDatabaseChangesApplied(this BaseOrchestrator orchestrator, Func<DatabaseChangesAppliedArgs, Task> func)
            => orchestrator.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider action when changes are applied on each table defined in the configuration schema
        /// </summary>
        public static void OnDatabaseChangesApplied(this BaseOrchestrator orchestrator, Action<DatabaseChangesAppliedArgs> func)
            => orchestrator.SetInterceptor(func);

        /// <summary>
        /// Occurs when changes are going to be queried on the local database
        /// </summary>
        public static void OnDatabaseChangesSelecting(this BaseOrchestrator orchestrator, Func<DatabaseChangesSelectingArgs, Task> func)
            => orchestrator.SetInterceptor(func);

        /// <summary>
        /// Occurs when changes are going to be queried on the local database
        /// </summary>
        public static void OnDatabaseChangesSelecting(this BaseOrchestrator orchestrator, Action<DatabaseChangesSelectingArgs> func)
            => orchestrator.SetInterceptor(func);

        /// <summary>
        /// Occurs when changes have been retrieved from the local database
        /// </summary>
        public static void OnDatabaseChangesSelected(this BaseOrchestrator orchestrator, Func<DatabaseChangesSelectedArgs, Task> func)
            => orchestrator.SetInterceptor(func);

        /// <summary>
        /// Occurs when changes have been retrieved from the local database
        /// </summary>
        public static void OnDatabaseChangesSelected(this BaseOrchestrator orchestrator, Action<DatabaseChangesSelectedArgs> func)
            => orchestrator.SetInterceptor(func);


    }
}
