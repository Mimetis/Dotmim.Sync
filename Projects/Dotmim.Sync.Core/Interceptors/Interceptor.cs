using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync
{

    public class InterceptorBase
    {
        private readonly Dictionary<Type, ISyncInterceptor> dictionary = new Dictionary<Type, ISyncInterceptor>();

        internal InterceptorWrapper<T> GetInterceptor<T>() where T : ProgressArgs
        {
            InterceptorWrapper<T> interceptor = null;
            var typeofT = typeof(T);

            // try get the interceptor from the dictionary and cast it
            if (this.dictionary.TryGetValue(typeofT, out var i))
                interceptor = (InterceptorWrapper<T>)i;

            // if null, create a new one
            if (interceptor == null)
            {
                interceptor = new InterceptorWrapper<T>();
                this.dictionary.Add(typeofT, interceptor);
            }

            return interceptor;
        }
    }

    public class Interceptor<T> : InterceptorBase where T : ProgressArgs
    {
        public Interceptor(Func<T, Task> func) => this.GetInterceptor<T>().Set(func);
        public Interceptor(Action<T> action) => this.GetInterceptor<T>().Set(action);
    }

    public class Interceptor<T, U> : InterceptorBase where T : ProgressArgs
                                                     where U : ProgressArgs
    {
        public Interceptor(Func<T, Task> t, Func<U, Task> u)
        {
            this.GetInterceptor<T>().Set(t);
            this.GetInterceptor<U>().Set(u);
        }
        public Interceptor(Func<T, Task> t, Action<U> u)
        {
            this.GetInterceptor<T>().Set(t);
            this.GetInterceptor<U>().Set(u);
        }
        public Interceptor(Action<T> t, Func<U, Task> u)
        {
            this.GetInterceptor<T>().Set(t);
            this.GetInterceptor<U>().Set(u);
        }
        public Interceptor(Action<T> t, Action<U> u)
        {
            this.GetInterceptor<T>().Set(t);
            this.GetInterceptor<U>().Set(u);
        }
    }
  
    public class Interceptors : InterceptorBase
    {

        /// <summary>
        /// Intercept the provider action when session begin is called
        /// </summary>
        public void OnSessionBegin(Func<SessionBeginArgs, Task> func)
            => this.GetInterceptor<SessionBeginArgs>().Set(func);

        /// <summary>
        /// Intercept the provider action when session begin is called
        /// </summary>
        public void OnSessionBegin(Action<SessionBeginArgs> func)
            => this.GetInterceptor<SessionBeginArgs>().Set(func);


        /// <summary>
        /// Intercept the provider action when session end is called
        /// </summary>
        public void OnSessionEnd(Func<SessionEndArgs, Task> func)
            => this.GetInterceptor<SessionEndArgs>().Set(func);

        /// <summary>
        /// Intercept the provider action when session end is called
        /// </summary>
        public void OnSessionEnd(Action<SessionEndArgs> func)
            => this.GetInterceptor<SessionEndArgs>().Set(func);

        /// <summary>
        /// Intercept the provider when schema is readed
        /// </summary>
        public void OnSchema(Func<SchemaArgs, Task> func)
            => this.GetInterceptor<SchemaArgs>().Set(func);

        /// <summary>
        /// Intercept the provider when schema is readed
        /// </summary>
        public void OnSchema(Action<SchemaArgs> func)
            => this.GetInterceptor<SchemaArgs>().Set(func);

        /// <summary>
        /// Intercept the provider before it begins a database deprovisioning
        /// </summary>
        public void OnDatabaseDeprovisioning(Func<DatabaseDeprovisioningArgs, Task> func)
            => this.GetInterceptor<DatabaseDeprovisioningArgs>().Set(func);

        /// <summary>
        /// Intercept the provider before it begins a database deprovisioning
        /// </summary>
        public void OnDatabaseDeprovisioning(Action<DatabaseDeprovisioningArgs> func)
            => this.GetInterceptor<DatabaseDeprovisioningArgs>().Set(func);

        /// <summary>
        /// Intercept the provider after it has deprovisioned a database
        /// </summary>
        public void OnDatabaseDeprovisioned(Func<DatabaseDeprovisionedArgs, Task> func)
            => this.GetInterceptor<DatabaseDeprovisionedArgs>().Set(func);

        /// <summary>
        /// Intercept the provider after it has deprovisioned a database
        /// </summary>
        public void OnDatabaseDeprovisioned(Action<DatabaseDeprovisionedArgs> func)
            => this.GetInterceptor<DatabaseDeprovisionedArgs>().Set(func);

        /// <summary>
        /// Intercept the provider before it begins a table deprovisioning
        /// </summary>
        public void OnTabeDeprovisioning(Func<TableDeprovisioningArgs, Task> func)
            => this.GetInterceptor<TableDeprovisioningArgs>().Set(func);

        /// <summary>
        /// Intercept the provider before it begins a table deprovisioning
        /// </summary>
        public void OnTabeDeprovisioning(Action<TableDeprovisioningArgs> func)
            => this.GetInterceptor<TableDeprovisioningArgs>().Set(func);

        /// <summary>
        /// Intercept the provider after it has deprovisioned a table
        /// </summary>
        public void OnTabledDeprovisioned(Func<TableDeprovisionedArgs, Task> func)
            => this.GetInterceptor<TableDeprovisionedArgs>().Set(func);

        /// <summary>
        /// Intercept the provider after it has deprovisioned a table
        /// </summary>
        public void OnTabledDeprovisioned(Action<TableDeprovisionedArgs> func)
            => this.GetInterceptor<TableDeprovisionedArgs>().Set(func);

        /// <summary>
        /// Intercept the provider before it begins a database provisioning
        /// </summary>
        public void OnDatabaseProvisioning(Func<DatabaseProvisioningArgs, Task> func)
            => this.GetInterceptor<DatabaseProvisioningArgs>().Set(func);

        /// <summary>
        /// Intercept the provider before it begins a database provisioning
        /// </summary>
        public void OnDatabaseProvisioning(Action<DatabaseProvisioningArgs> func)
            => this.GetInterceptor<DatabaseProvisioningArgs>().Set(func);

        /// <summary>
        /// Intercept the provider after it has provisioned a database
        /// </summary>
        public void OnDatabaseProvisioned(Func<DatabaseProvisionedArgs, Task> func)
            => this.GetInterceptor<DatabaseProvisionedArgs>().Set(func);

        /// <summary>
        /// Intercept the provider after it has provisioned a database
        /// </summary>
        public void OnDatabaseProvisioned(Action<DatabaseProvisionedArgs> func)
            => this.GetInterceptor<DatabaseProvisionedArgs>().Set(func);

        /// <summary>
        /// Intercept the provider before it begins a table provisioning
        /// </summary>
        public void OnTabeProvisioning(Func<TableDeprovisioningArgs, Task> func)
            => this.GetInterceptor<TableDeprovisioningArgs>().Set(func);

        /// <summary>
        /// Intercept the provider before it begins a table provisioning
        /// </summary>
        public void OnTabeProvisioning(Action<TableDeprovisioningArgs> func)
            => this.GetInterceptor<TableDeprovisioningArgs>().Set(func);

        /// <summary>
        /// Intercept the provider after it has provisioned a table
        /// </summary>
        public void OnTabledProvisioned(Func<TableProvisionedArgs, Task> func)
            => this.GetInterceptor<TableProvisionedArgs>().Set(func);

        /// <summary>
        /// Intercept the provider after it has provisioned a table
        /// </summary>
        public void OnTabledProvisioned(Action<TableProvisionedArgs> func)
            => this.GetInterceptor<TableProvisionedArgs>().Set(func);

        /// <summary>
        /// Intercept the provider action when changes are going to be selected on each table defined in the configuration schema
        /// </summary>
        public void OnTableChangesSelecting(Func<TableChangesSelectingArgs, Task> func)
            => this.GetInterceptor<TableChangesSelectingArgs>().Set(func);

        /// <summary>
        /// Intercept the provider action when changes are going to be selected on each table defined in the configuration schema
        /// </summary>
        public void OnTableChangesSelecting(Action<TableChangesSelectingArgs> func)
            => this.GetInterceptor<TableChangesSelectingArgs>().Set(func);

        /// <summary>
        /// Intercept the provider action when changes are selected on each table defined in the configuration schema
        /// </summary>
        public void OnTableChangesSelected(Func<TableChangesSelectedArgs, Task> func)
            => this.GetInterceptor<TableChangesSelectedArgs>().Set(func);

        /// <summary>
        /// Intercept the provider action when changes are selected on each table defined in the configuration schema
        /// </summary>
        public void OnTableChangesSelected(Action<TableChangesSelectedArgs> func)
            => this.GetInterceptor<TableChangesSelectedArgs>().Set(func);

        /// <summary>
        /// Intercept the provider action when changes are going to be applied on each table defined in the configuration schema
        /// </summary>
        public void OnTableChangesApplying(Func<TableChangesApplyingArgs, Task> func)
            => this.GetInterceptor<TableChangesApplyingArgs>().Set(func);

        /// <summary>
        /// Intercept the provider action when changes are going to be applied on each table defined in the configuration schema
        /// </summary>
        public void OnTableChangesApplying(Action<TableChangesApplyingArgs> func)
            => this.GetInterceptor<TableChangesApplyingArgs>().Set(func);

        /// <summary>
        /// Intercept the provider action when changes are applied on each table defined in the configuration schema
        /// </summary>
        public void OnTableChangesApplied(Func<TableChangesAppliedArgs, Task> func)
            => this.GetInterceptor<TableChangesAppliedArgs>().Set(func);

        /// <summary>
        /// Intercept the provider action when changes are applied on each table defined in the configuration schema
        /// </summary>
        public void OnTableChangesApplied(Action<TableChangesAppliedArgs> func)
            => this.GetInterceptor<TableChangesAppliedArgs>().Set(func);


        /// <summary>
        /// Intercept the provider action when changes are going to be applied on each table defined in the configuration schema
        /// </summary>
        public void OnDatabaseChangesApplying(Func<DatabaseChangesApplyingArgs, Task> func)
            => this.GetInterceptor<DatabaseChangesApplyingArgs>().Set(func);

        /// <summary>
        /// Intercept the provider action when changes are going to be applied on each table defined in the configuration schema
        /// </summary>
        public void OnDatabaseChangesApplying(Action<DatabaseChangesApplyingArgs> func)
            => this.GetInterceptor<DatabaseChangesApplyingArgs>().Set(func);

        /// <summary>
        /// Intercept the provider action when changes are applied on each table defined in the configuration schema
        /// </summary>
        public void OnDatabaseChangesApplied(Func<DatabaseChangesAppliedArgs, Task> func)
            => this.GetInterceptor<DatabaseChangesAppliedArgs>().Set(func);

        /// <summary>
        /// Intercept the provider action when changes are applied on each table defined in the configuration schema
        /// </summary>
        public void OnDatabaseChangesApplied(Action<DatabaseChangesAppliedArgs> func)
            => this.GetInterceptor<DatabaseChangesAppliedArgs>().Set(func);




    }
}
