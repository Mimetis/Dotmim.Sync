using System;
using System.Threading.Tasks;

namespace Dotmim.Sync
{

    //public static class InterceptorsExtensions
    //{

    //    /// <summary>
    //    /// Intercept the provider action when session begin is called
    //    /// </summary>
    //    public static void InterceptOutdated(this CoreProvider coreProvider, Func<OutdatedArgs, Task> func)
    //        => coreProvider.GetInterceptor<OutdatedArgs>().Set(func);
        
    //    /// <summary>
    //    /// Intercept the provider action when session begin is called
    //    /// </summary>
    //    public static void InterceptOutdated(this CoreProvider coreProvider, Action<OutdatedArgs> func)
    //        => coreProvider.GetInterceptor<OutdatedArgs>().Set(func);


    //    /// <summary>
    //    /// Intercept the provider action when session begin is called
    //    /// </summary>
    //    public static void InterceptSessionBegin(this CoreProvider coreProvider, Func<SessionBeginArgs, Task> func)
    //        => coreProvider.GetInterceptor<SessionBeginArgs>().Set(func);

    //    /// <summary>
    //    /// Intercept the provider action when session begin is called
    //    /// </summary>
    //    public static void InterceptSessionBegin(this CoreProvider coreProvider, Action<SessionBeginArgs> func)
    //        => coreProvider.GetInterceptor<SessionBeginArgs>().Set(func);

    //    /// <summary>
    //    /// Intercept the provider action when session end is called
    //    /// </summary>
    //    public static void InterceptSessionEnd(this CoreProvider coreProvider, Func<SessionEndArgs, Task> func)
    //        => coreProvider.GetInterceptor<SessionEndArgs>().Set(func);

    //    /// <summary>
    //    /// Intercept the provider action when session end is called
    //    /// </summary>
    //    public static void InterceptSessionEnd(this CoreProvider coreProvider, Action<SessionEndArgs> func)
    //        => coreProvider.GetInterceptor<SessionEndArgs>().Set(func);

    //    /// <summary>
    //    /// Intercept the provider when schema is readed
    //    /// </summary>
    //    public static void InterceptSchema(this CoreProvider coreProvider, Func<SchemaArgs, Task> func)
    //        => coreProvider.GetInterceptor<SchemaArgs>().Set(func);

    //    /// <summary>
    //    /// Intercept the provider when schema is readed
    //    /// </summary>
    //    public static void InterceptSchema(this CoreProvider coreProvider, Action<SchemaArgs> func)
    //        => coreProvider.GetInterceptor<SchemaArgs>().Set(func);

    //    /// <summary>
    //    /// Intercept the provider before it begins a database deprovisioning
    //    /// </summary>
    //    public static void InterceptDatabaseDeprovisioning(this CoreProvider coreProvider, Func<DatabaseDeprovisioningArgs, Task> func)
    //        => coreProvider.GetInterceptor<DatabaseDeprovisioningArgs>().Set(func);

    //    /// <summary>
    //    /// Intercept the provider before it begins a database deprovisioning
    //    /// </summary>
    //    public static void InterceptDatabaseDeprovisioning(this CoreProvider coreProvider, Action<DatabaseDeprovisioningArgs> func)
    //        => coreProvider.GetInterceptor<DatabaseDeprovisioningArgs>().Set(func);

    //    /// <summary>
    //    /// Intercept the provider after it has deprovisioned a database
    //    /// </summary>
    //    public static void InterceptDatabaseDeprovisioned(this CoreProvider coreProvider, Func<DatabaseDeprovisionedArgs, Task> func)
    //        => coreProvider.GetInterceptor<DatabaseDeprovisionedArgs>().Set(func);

    //    /// <summary>
    //    /// Intercept the provider after it has deprovisioned a database
    //    /// </summary>
    //    public static void InterceptDatabaseDeprovisioned(this CoreProvider coreProvider, Action<DatabaseDeprovisionedArgs> func)
    //        => coreProvider.GetInterceptor<DatabaseDeprovisionedArgs>().Set(func);

    //    /// <summary>
    //    /// Intercept the provider before it begins a table deprovisioning
    //    /// </summary>
    //    public static void InterceptTabeDeprovisioning(this CoreProvider coreProvider, Func<TableDeprovisioningArgs, Task> func)
    //        => coreProvider.GetInterceptor<TableDeprovisioningArgs>().Set(func);

    //    /// <summary>
    //    /// Intercept the provider before it begins a table deprovisioning
    //    /// </summary>
    //    public static void InterceptTabeDeprovisioning(this CoreProvider coreProvider, Action<TableDeprovisioningArgs> func)
    //        => coreProvider.GetInterceptor<TableDeprovisioningArgs>().Set(func);

    //    /// <summary>
    //    /// Intercept the provider after it has deprovisioned a table
    //    /// </summary>
    //    public static void InterceptTabledDeprovisioned(this CoreProvider coreProvider, Func<TableDeprovisionedArgs, Task> func)
    //        => coreProvider.GetInterceptor<TableDeprovisionedArgs>().Set(func);

    //    /// <summary>
    //    /// Intercept the provider after it has deprovisioned a table
    //    /// </summary>
    //    public static void InterceptTabledDeprovisioned(this CoreProvider coreProvider, Action<TableDeprovisionedArgs> func)
    //        => coreProvider.GetInterceptor<TableDeprovisionedArgs>().Set(func);

    //    /// <summary>
    //    /// Intercept the provider before it begins a database provisioning
    //    /// </summary>
    //    public static void InterceptDatabaseProvisioning(this CoreProvider coreProvider, Func<DatabaseProvisioningArgs, Task> func)
    //        => coreProvider.GetInterceptor<DatabaseProvisioningArgs>().Set(func);

    //    /// <summary>
    //    /// Intercept the provider before it begins a database provisioning
    //    /// </summary>
    //    public static void InterceptDatabaseProvisioning(this CoreProvider coreProvider, Action<DatabaseProvisioningArgs> func)
    //        => coreProvider.GetInterceptor<DatabaseProvisioningArgs>().Set(func);

    //    /// <summary>
    //    /// Intercept the provider after it has provisioned a database
    //    /// </summary>
    //    public static void InterceptDatabaseProvisioned(this CoreProvider coreProvider, Func<DatabaseProvisionedArgs, Task> func)
    //        => coreProvider.GetInterceptor<DatabaseProvisionedArgs>().Set(func);

    //    /// <summary>
    //    /// Intercept the provider after it has provisioned a database
    //    /// </summary>
    //    public static void InterceptDatabaseProvisioned(this CoreProvider coreProvider, Action<DatabaseProvisionedArgs> func)
    //        => coreProvider.GetInterceptor<DatabaseProvisionedArgs>().Set(func);

    //    /// <summary>
    //    /// Intercept the provider before it begins a table provisioning
    //    /// </summary>
    //    public static void InterceptTabeProvisioning(this CoreProvider coreProvider, Func<TableDeprovisioningArgs, Task> func)
    //        => coreProvider.GetInterceptor<TableDeprovisioningArgs>().Set(func);

    //    /// <summary>
    //    /// Intercept the provider before it begins a table provisioning
    //    /// </summary>
    //    public static void InterceptTabeProvisioning(this CoreProvider coreProvider, Action<TableDeprovisioningArgs> func)
    //        => coreProvider.GetInterceptor<TableDeprovisioningArgs>().Set(func);

    //    /// <summary>
    //    /// Intercept the provider after it has provisioned a table
    //    /// </summary>
    //    public static void InterceptTabledProvisioned(this CoreProvider coreProvider, Func<TableProvisionedArgs, Task> func)
    //        => coreProvider.GetInterceptor<TableProvisionedArgs>().Set(func);

    //    /// <summary>
    //    /// Intercept the provider after it has provisioned a table
    //    /// </summary>
    //    public static void InterceptTabledProvisioned(this CoreProvider coreProvider, Action<TableProvisionedArgs> func)
    //        => coreProvider.GetInterceptor<TableProvisionedArgs>().Set(func);

    //    /// <summary>
    //    /// Intercept the provider action when changes are going to be selected on each table defined in the configuration schema
    //    /// </summary>
    //    public static void InterceptTableChangesSelecting(this CoreProvider coreProvider, Func<TableChangesSelectingArgs, Task> func)
    //        => coreProvider.GetInterceptor<TableChangesSelectingArgs>().Set(func);

    //    /// <summary>
    //    /// Intercept the provider action when changes are going to be selected on each table defined in the configuration schema
    //    /// </summary>
    //    public static void InterceptTableChangesSelecting(this CoreProvider coreProvider, Action<TableChangesSelectingArgs> func)
    //        => coreProvider.GetInterceptor<TableChangesSelectingArgs>().Set(func);

    //    /// <summary>
    //    /// Intercept the provider action when changes are selected on each table defined in the configuration schema
    //    /// </summary>
    //    public static void InterceptTableChangesSelected(this CoreProvider coreProvider, Func<TableChangesSelectedArgs, Task> func)
    //        => coreProvider.GetInterceptor<TableChangesSelectedArgs>().Set(func);

    //    /// <summary>
    //    /// Intercept the provider action when changes are selected on each table defined in the configuration schema
    //    /// </summary>
    //    public static void InterceptTableChangesSelected(this CoreProvider coreProvider, Action<TableChangesSelectedArgs> func)
    //        => coreProvider.GetInterceptor<TableChangesSelectedArgs>().Set(func);

    //    /// <summary>
    //    /// Intercept the provider action when changes are going to be applied on each table defined in the configuration schema
    //    /// </summary>
    //    public static void InterceptTableChangesApplying(this CoreProvider coreProvider, Func<TableChangesApplyingArgs, Task> func)
    //        => coreProvider.GetInterceptor<TableChangesApplyingArgs>().Set(func);

    //    /// <summary>
    //    /// Intercept the provider action when changes are going to be applied on each table defined in the configuration schema
    //    /// </summary>
    //    public static void InterceptTableChangesApplying(this CoreProvider coreProvider, Action<TableChangesApplyingArgs> func)
    //        => coreProvider.GetInterceptor<TableChangesApplyingArgs>().Set(func);

    //    /// <summary>
    //    /// Intercept the provider action when changes are applied on each table defined in the configuration schema
    //    /// </summary>
    //    public static void InterceptTableChangesApplied(this CoreProvider coreProvider, Func<TableChangesAppliedArgs, Task> func)
    //        => coreProvider.GetInterceptor<TableChangesAppliedArgs>().Set(func);

    //    /// <summary>
    //    /// Intercept the provider action when changes are applied on each table defined in the configuration schema
    //    /// </summary>
    //    public static void InterceptTableChangesApplied(this CoreProvider coreProvider, Action<TableChangesAppliedArgs> func)
    //        => coreProvider.GetInterceptor<TableChangesAppliedArgs>().Set(func);

    //}
}
