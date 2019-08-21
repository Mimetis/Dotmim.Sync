using System;
using System.Threading.Tasks;

namespace Dotmim.Sync
{

    public static class InterceptorsExtensions
    {


        public static void SetInterceptor<T>(this CoreProvider coreProvider, Func<T, Task> func) where T : ProgressArgs
        {
            coreProvider.On(new Interceptor<T>(func));
        }

        public static void SetInterceptor<T>(this CoreProvider coreProvider, Action<T> action) where T : ProgressArgs
        {
            coreProvider.On(new Interceptor<T>(action));
        }

        /// <summary>
        /// Intercept the provider action whenever a connection is opened
        /// </summary>
        public static void OnConnectionOpen(this CoreProvider coreProvider, Func<ConnectionOpenArgs, Task> func)
            => coreProvider.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider action whenever a connection is opened
        /// </summary>
        public static void OnConnectionOpen(this CoreProvider coreProvider, Action<ConnectionOpenArgs> func)
            => coreProvider.On(func);

        /// <summary>
        /// Intercept the provider action whenever a transaction is opened
        /// </summary>
        public static void OnTransactionOpen(this CoreProvider coreProvider, Action<TransactionOpenArgs> func)
            => coreProvider.On(func);

        /// <summary>
        /// Intercept the provider action whenever a transaction is opened
        /// </summary>
        public static void OnTransactionOpen(this CoreProvider coreProvider, Func<ConnectionOpenArgs, Task> func)
            => coreProvider.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider action whenever a connection is closed
        /// </summary>
        public static void OnConnectionClose(this CoreProvider coreProvider, Func<ConnectionCloseArgs, Task> func)
            => coreProvider.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider action whenever a connection is closed
        /// </summary>
        public static void OnConnectionClose(this CoreProvider coreProvider, Action<ConnectionCloseArgs> func)
            => coreProvider.On(func);

        /// <summary>
        /// Intercept the provider action whenever a transaction is commit
        /// </summary>
        public static void OnTransactionCommit(this CoreProvider coreProvider, Action<TransactionCommitArgs> func)
            => coreProvider.On(func);

        /// <summary>
        /// Intercept the provider action whenever a transaction is commit
        /// </summary>
        public static void OnTransactionCommit(this CoreProvider coreProvider, Func<TransactionCommitArgs, Task> func)
            => coreProvider.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider action when session begin is called
        /// </summary>
        public static void OnOutdated(this CoreProvider coreProvider, Func<OutdatedArgs, Task> func)
            => coreProvider.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider action when session begin is called
        /// </summary>
        public static void OnOutdated(this CoreProvider coreProvider, Action<OutdatedArgs> func)
            => coreProvider.On(func);


        /// <summary>
        /// Intercept the provider when an apply change is failing
        /// </summary>
        public static void OnApplyChangesFailed(this CoreProvider coreProvider, Func<ApplyChangesFailedArgs, Task> func)
            => coreProvider.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider when an apply change is failing
        /// </summary>
        public static void OnApplyChangesFailed(this CoreProvider coreProvider, Action<ApplyChangesFailedArgs> func)
            => coreProvider.On(func);


        /// <summary>
        /// Intercept the provider action when session begin is called
        /// </summary>
        public static void OnSessionBegin(this CoreProvider coreProvider, Func<SessionBeginArgs, Task> func)
            => coreProvider.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider action when session begin is called
        /// </summary>
        public static void OnSessionBegin(this CoreProvider coreProvider, Action<SessionBeginArgs> func)
            => coreProvider.On(func);

        /// <summary>
        /// Intercept the provider action when session end is called
        /// </summary>
        public static void OnSessionEnd(this CoreProvider coreProvider, Func<SessionEndArgs, Task> func)
            => coreProvider.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider action when session end is called
        /// </summary>
        public static void OnSessionEnd(this CoreProvider coreProvider, Action<SessionEndArgs> func)
            => coreProvider.On(func);

        /// <summary>
        /// Intercept the provider when schema is readed
        /// </summary>
        public static void OnSchema(this CoreProvider coreProvider, Func<SchemaArgs, Task> func)
            => coreProvider.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider when schema is readed
        /// </summary>
        public static void OnSchema(this CoreProvider coreProvider, Action<SchemaArgs> func)
            => coreProvider.On(func);

        /// <summary>
        /// Intercept the provider before it begins a database deprovisioning
        /// </summary>
        public static void OnDatabaseDeprovisioning(this CoreProvider coreProvider, Func<DatabaseDeprovisioningArgs, Task> func)
            => coreProvider.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider before it begins a database deprovisioning
        /// </summary>
        public static void OnDatabaseDeprovisioning(this CoreProvider coreProvider, Action<DatabaseDeprovisioningArgs> func)
            => coreProvider.On(func);

        /// <summary>
        /// Intercept the provider after it has deprovisioned a database
        /// </summary>
        public static void OnDatabaseDeprovisioned(this CoreProvider coreProvider, Func<DatabaseDeprovisionedArgs, Task> func)
            => coreProvider.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider after it has deprovisioned a database
        /// </summary>
        public static void OnDatabaseDeprovisioned(this CoreProvider coreProvider, Action<DatabaseDeprovisionedArgs> func)
            => coreProvider.On(func);

        /// <summary>
        /// Intercept the provider before it begins a table deprovisioning
        /// </summary>
        public static void OnTabeDeprovisioning(this CoreProvider coreProvider, Func<TableDeprovisioningArgs, Task> func)
            => coreProvider.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider before it begins a table deprovisioning
        /// </summary>
        public static void OnTabeDeprovisioning(this CoreProvider coreProvider, Action<TableDeprovisioningArgs> func)
            => coreProvider.On(func);

        /// <summary>
        /// Intercept the provider after it has deprovisioned a table
        /// </summary>
        public static void OnTabledDeprovisioned(this CoreProvider coreProvider, Func<TableDeprovisionedArgs, Task> func)
            => coreProvider.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider after it has deprovisioned a table
        /// </summary>
        public static void OnTabledDeprovisioned(this CoreProvider coreProvider, Action<TableDeprovisionedArgs> func)
            => coreProvider.On(func);

        /// <summary>
        /// Intercept the provider before it begins a database provisioning
        /// </summary>
        public static void OnDatabaseProvisioning(this CoreProvider coreProvider, Func<DatabaseProvisioningArgs, Task> func)
            => coreProvider.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider before it begins a database provisioning
        /// </summary>
        public static void OnDatabaseProvisioning(this CoreProvider coreProvider, Action<DatabaseProvisioningArgs> func)
            => coreProvider.On(func);

        /// <summary>
        /// Intercept the provider after it has provisioned a database
        /// </summary>
        public static void OnDatabaseProvisioned(this CoreProvider coreProvider, Func<DatabaseProvisionedArgs, Task> func)
            => coreProvider.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider after it has provisioned a database
        /// </summary>
        public static void OnDatabaseProvisioned(this CoreProvider coreProvider, Action<DatabaseProvisionedArgs> func)
            => coreProvider.On(func);

        /// <summary>
        /// Intercept the provider before it begins a table provisioning
        /// </summary>
        public static void OnTabeProvisioning(this CoreProvider coreProvider, Func<TableDeprovisioningArgs, Task> func)
            => coreProvider.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider before it begins a table provisioning
        /// </summary>
        public static void OnTabeProvisioning(this CoreProvider coreProvider, Action<TableDeprovisioningArgs> func)
            => coreProvider.On(func);

        /// <summary>
        /// Intercept the provider after it has provisioned a table
        /// </summary>
        public static void OnTabledProvisioned(this CoreProvider coreProvider, Func<TableProvisionedArgs, Task> func)
            => coreProvider.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider after it has provisioned a table
        /// </summary>
        public static void OnTabledProvisioned(this CoreProvider coreProvider, Action<TableProvisionedArgs> func)
            => coreProvider.On(func);

        /// <summary>
        /// Intercept the provider action when changes are going to be selected on each table defined in the configuration schema
        /// </summary>
        public static void OnTableChangesSelecting(this CoreProvider coreProvider, Func<TableChangesSelectingArgs, Task> func)
            => coreProvider.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider action when changes are going to be selected on each table defined in the configuration schema
        /// </summary>
        public static void OnTableChangesSelecting(this CoreProvider coreProvider, Action<TableChangesSelectingArgs> func)
            => coreProvider.On(func);

        /// <summary>
        /// Intercept the provider action when changes are selected on each table defined in the configuration schema
        /// </summary>
        public static void OnTableChangesSelected(this CoreProvider coreProvider, Func<TableChangesSelectedArgs, Task> func)
            => coreProvider.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider action when changes are selected on each table defined in the configuration schema
        /// </summary>
        public static void OnTableChangesSelected(this CoreProvider coreProvider, Action<TableChangesSelectedArgs> func)
            => coreProvider.On(func);

        /// <summary>
        /// Intercept the provider action when changes are going to be applied on each table defined in the configuration schema
        /// </summary>
        public static void OnTableChangesApplying(this CoreProvider coreProvider, Func<TableChangesApplyingArgs, Task> func)
            => coreProvider.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider action when changes are going to be applied on each table defined in the configuration schema
        /// </summary>
        public static void OnTableChangesApplying(this CoreProvider coreProvider, Action<TableChangesApplyingArgs> func)
            => coreProvider.On(func);

        /// <summary>
        /// Intercept the provider action when changes are applied on each table defined in the configuration schema
        /// </summary>
        public static void OnTableChangesApplied(this CoreProvider coreProvider, Func<TableChangesAppliedArgs, Task> func)
            => coreProvider.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider action when changes are applied on each table defined in the configuration schema
        /// </summary>
        public static void OnTableChangesApplied(this CoreProvider coreProvider, Action<TableChangesAppliedArgs> func)
            => coreProvider.On(func);

    }
}
