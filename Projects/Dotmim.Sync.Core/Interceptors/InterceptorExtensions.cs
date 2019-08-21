using System;
using System.Threading.Tasks;

namespace Dotmim.Sync
{

    public static class InterceptorsExtensions
    {


        public static void SetInterceptor<T>(this CoreProvider coreProvider, Func<T, Task> func) where T : ProgressArgs
        {
            coreProvider.SetInterceptor(new Interceptor<T>(func));
        }

        public static void SetInterceptor<T>(this CoreProvider coreProvider, Action<T> action) where T : ProgressArgs
        {
            coreProvider.SetInterceptor(new Interceptor<T>(action));
        }

        /// <summary>
        /// Intercept the provider action whenever a connection is opened
        /// </summary>
        public static void InterceptConnectionOpen(this CoreProvider coreProvider, Func<ConnectionOpenArgs, Task> func)
            => coreProvider.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider action whenever a connection is opened
        /// </summary>
        public static void InterceptConnectionOpen(this CoreProvider coreProvider, Action<ConnectionOpenArgs> func)
            => coreProvider.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider action whenever a transaction is opened
        /// </summary>
        public static void InterceptTransactionOpen(this CoreProvider coreProvider, Action<TransactionOpenArgs> func)
            => coreProvider.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider action whenever a transaction is opened
        /// </summary>
        public static void InterceptTransactionOpen(this CoreProvider coreProvider, Func<ConnectionOpenArgs, Task> func)
            => coreProvider.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider action whenever a connection is closed
        /// </summary>
        public static void InterceptConnectionClose(this CoreProvider coreProvider, Func<ConnectionCloseArgs, Task> func)
            => coreProvider.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider action whenever a connection is closed
        /// </summary>
        public static void InterceptConnectionClose(this CoreProvider coreProvider, Action<ConnectionCloseArgs> func)
            => coreProvider.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider action whenever a transaction is commit
        /// </summary>
        public static void InterceptTransactionCommit(this CoreProvider coreProvider, Action<TransactionCommitArgs> func)
            => coreProvider.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider action whenever a transaction is commit
        /// </summary>
        public static void InterceptTransactionCommit(this CoreProvider coreProvider, Func<TransactionCommitArgs, Task> func)
            => coreProvider.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider action when session begin is called
        /// </summary>
        public static void InterceptOutdated(this CoreProvider coreProvider, Func<OutdatedArgs, Task> func)
            => coreProvider.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider action when session begin is called
        /// </summary>
        public static void InterceptOutdated(this CoreProvider coreProvider, Action<OutdatedArgs> func)
            => coreProvider.SetInterceptor(func);


        /// <summary>
        /// Intercept the provider when an apply change is failing
        /// </summary>
        public static void InterceptApplyChangesFailed(this CoreProvider coreProvider, Func<ApplyChangesFailedArgs, Task> func)
            => coreProvider.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider when an apply change is failing
        /// </summary>
        public static void InterceptApplyChangesFailed(this CoreProvider coreProvider, Action<ApplyChangesFailedArgs> func)
            => coreProvider.SetInterceptor(func);


        /// <summary>
        /// Intercept the provider action when session begin is called
        /// </summary>
        public static void InterceptSessionBegin(this CoreProvider coreProvider, Func<SessionBeginArgs, Task> func)
            => coreProvider.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider action when session begin is called
        /// </summary>
        public static void InterceptSessionBegin(this CoreProvider coreProvider, Action<SessionBeginArgs> func)
            => coreProvider.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider action when session end is called
        /// </summary>
        public static void InterceptSessionEnd(this CoreProvider coreProvider, Func<SessionEndArgs, Task> func)
            => coreProvider.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider action when session end is called
        /// </summary>
        public static void InterceptSessionEnd(this CoreProvider coreProvider, Action<SessionEndArgs> func)
            => coreProvider.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider when schema is readed
        /// </summary>
        public static void InterceptSchema(this CoreProvider coreProvider, Func<SchemaArgs, Task> func)
            => coreProvider.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider when schema is readed
        /// </summary>
        public static void InterceptSchema(this CoreProvider coreProvider, Action<SchemaArgs> func)
            => coreProvider.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider before it begins a database deprovisioning
        /// </summary>
        public static void InterceptDatabaseDeprovisioning(this CoreProvider coreProvider, Func<DatabaseDeprovisioningArgs, Task> func)
            => coreProvider.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider before it begins a database deprovisioning
        /// </summary>
        public static void InterceptDatabaseDeprovisioning(this CoreProvider coreProvider, Action<DatabaseDeprovisioningArgs> func)
            => coreProvider.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider after it has deprovisioned a database
        /// </summary>
        public static void InterceptDatabaseDeprovisioned(this CoreProvider coreProvider, Func<DatabaseDeprovisionedArgs, Task> func)
            => coreProvider.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider after it has deprovisioned a database
        /// </summary>
        public static void InterceptDatabaseDeprovisioned(this CoreProvider coreProvider, Action<DatabaseDeprovisionedArgs> func)
            => coreProvider.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider before it begins a table deprovisioning
        /// </summary>
        public static void InterceptTabeDeprovisioning(this CoreProvider coreProvider, Func<TableDeprovisioningArgs, Task> func)
            => coreProvider.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider before it begins a table deprovisioning
        /// </summary>
        public static void InterceptTabeDeprovisioning(this CoreProvider coreProvider, Action<TableDeprovisioningArgs> func)
            => coreProvider.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider after it has deprovisioned a table
        /// </summary>
        public static void InterceptTabledDeprovisioned(this CoreProvider coreProvider, Func<TableDeprovisionedArgs, Task> func)
            => coreProvider.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider after it has deprovisioned a table
        /// </summary>
        public static void InterceptTabledDeprovisioned(this CoreProvider coreProvider, Action<TableDeprovisionedArgs> func)
            => coreProvider.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider before it begins a database provisioning
        /// </summary>
        public static void InterceptDatabaseProvisioning(this CoreProvider coreProvider, Func<DatabaseProvisioningArgs, Task> func)
            => coreProvider.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider before it begins a database provisioning
        /// </summary>
        public static void InterceptDatabaseProvisioning(this CoreProvider coreProvider, Action<DatabaseProvisioningArgs> func)
            => coreProvider.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider after it has provisioned a database
        /// </summary>
        public static void InterceptDatabaseProvisioned(this CoreProvider coreProvider, Func<DatabaseProvisionedArgs, Task> func)
            => coreProvider.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider after it has provisioned a database
        /// </summary>
        public static void InterceptDatabaseProvisioned(this CoreProvider coreProvider, Action<DatabaseProvisionedArgs> func)
            => coreProvider.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider before it begins a table provisioning
        /// </summary>
        public static void InterceptTabeProvisioning(this CoreProvider coreProvider, Func<TableDeprovisioningArgs, Task> func)
            => coreProvider.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider before it begins a table provisioning
        /// </summary>
        public static void InterceptTabeProvisioning(this CoreProvider coreProvider, Action<TableDeprovisioningArgs> func)
            => coreProvider.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider after it has provisioned a table
        /// </summary>
        public static void InterceptTabledProvisioned(this CoreProvider coreProvider, Func<TableProvisionedArgs, Task> func)
            => coreProvider.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider after it has provisioned a table
        /// </summary>
        public static void InterceptTabledProvisioned(this CoreProvider coreProvider, Action<TableProvisionedArgs> func)
            => coreProvider.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider action when changes are going to be selected on each table defined in the configuration schema
        /// </summary>
        public static void InterceptTableChangesSelecting(this CoreProvider coreProvider, Func<TableChangesSelectingArgs, Task> func)
            => coreProvider.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider action when changes are going to be selected on each table defined in the configuration schema
        /// </summary>
        public static void InterceptTableChangesSelecting(this CoreProvider coreProvider, Action<TableChangesSelectingArgs> func)
            => coreProvider.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider action when changes are selected on each table defined in the configuration schema
        /// </summary>
        public static void InterceptTableChangesSelected(this CoreProvider coreProvider, Func<TableChangesSelectedArgs, Task> func)
            => coreProvider.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider action when changes are selected on each table defined in the configuration schema
        /// </summary>
        public static void InterceptTableChangesSelected(this CoreProvider coreProvider, Action<TableChangesSelectedArgs> func)
            => coreProvider.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider action when changes are going to be applied on each table defined in the configuration schema
        /// </summary>
        public static void InterceptTableChangesApplying(this CoreProvider coreProvider, Func<TableChangesApplyingArgs, Task> func)
            => coreProvider.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider action when changes are going to be applied on each table defined in the configuration schema
        /// </summary>
        public static void InterceptTableChangesApplying(this CoreProvider coreProvider, Action<TableChangesApplyingArgs> func)
            => coreProvider.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider action when changes are applied on each table defined in the configuration schema
        /// </summary>
        public static void InterceptTableChangesApplied(this CoreProvider coreProvider, Func<TableChangesAppliedArgs, Task> func)
            => coreProvider.SetInterceptor(func);

        /// <summary>
        /// Intercept the provider action when changes are applied on each table defined in the configuration schema
        /// </summary>
        public static void InterceptTableChangesApplied(this CoreProvider coreProvider, Action<TableChangesAppliedArgs> func)
            => coreProvider.SetInterceptor(func);

    }
}
