using Dotmim.Sync.Batch;
using Dotmim.Sync.Messages;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{

    /// <summary>
    /// Base orchestrator.
    /// </summary>
    public interface IOrchestrator<T> where T : IProvider
    {
        /// <summary>
        /// Set the provider used for this orchestrator
        /// </summary>
        void SetProvider(T coreProvider);

        /// <summary>
        /// Set the progress for this orchestrator
        /// </summary>
        void SetProgress(IProgress<ProgressArgs> progress);

        /// <summary>
        /// Set the options for this orchestrator
        /// </summary>
        void SetOptions(Action<SyncOptions> options);

        /// <summary>
        /// Set the configuration for this orchestrator
        /// </summary>
        void SetConfiguration(Action<SyncConfiguration> configuration);


        /// <summary>
        /// Set the Interceptor class to intercepts multipes events during the sync process
        /// </summary>
        void On(InterceptorBase interceptor);

        /// <summary>
        /// Set the Interceptor class to intercepts multipes events during the sync process
        /// </summary>
        void On<T>(Action<T> interceptorAction) where T : ProgressArgs;

        /// <summary>
        /// Send all changes and get new changes in return
        /// </summary>
        Task<object> GetChangesAsync(object localChanges);

    }

    /// <summary>
    /// Remote provider
    /// </summary>
    public interface IRemoteOrchestrator<T> : IOrchestrator<T> where T : IProvider
    {

    }

    /// <summary>
    /// Remote provider
    /// </summary>
    public interface ILocalOrchestrator<T> : IOrchestrator<T> where T : IProvider
    {

        /// <summary>
        /// Save changes locally
        /// </summary>
        Task<string> ApplyChangesAsync(object remoteChanges);

    }


}
