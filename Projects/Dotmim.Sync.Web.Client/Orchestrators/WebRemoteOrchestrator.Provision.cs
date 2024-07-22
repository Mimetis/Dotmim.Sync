using Dotmim.Sync.Enumerations;
using System;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync.Web.Client
{
    /// <summary>
    /// Web remote orchestrator, used to make remote calls to the server side, from the client side, when using an http mode.
    /// </summary>
    public partial class WebRemoteOrchestrator : RemoteOrchestrator
    {

        /// <summary>
        /// Raise an error, as you are not authorized to launch a deprovision on the server from a client.
        /// </summary>
        public override Task<bool> DeprovisionAsync(string scopeName, SyncProvision provision = SyncProvision.NotSet,
            DbConnection connection = null, DbTransaction transaction = null, IProgress<ProgressArgs> progress = null, CancellationToken cancellationToken = default)
                => throw new NotImplementedException();

        /// <summary>
        /// Raise an error, as you are not authorized to launch a provision on the server from a client.
        /// </summary>
        public override Task<ScopeInfo> ProvisionAsync(ScopeInfo serverScopeInfo, SyncProvision provision = SyncProvision.NotSet, bool overwrite = false,
            DbConnection connection = null, DbTransaction transaction = null, IProgress<ProgressArgs> progress = null, CancellationToken cancellationToken = default)
            => throw new NotImplementedException();

        /// <summary>
        /// Raise an error, as you are not authorized to launch a provision on the server from a client.
        /// </summary>
        public override Task<ScopeInfo> ProvisionAsync(string scopeName, SyncProvision provision = SyncProvision.NotSet, bool overwrite = false,
            DbConnection connection = null, DbTransaction transaction = null, IProgress<ProgressArgs> progress = null, CancellationToken cancellationToken = default)
            => throw new NotImplementedException();

        /// <summary>
        /// Raise an error, as you are not authorized to launch a provision on the server from a client.
        /// </summary>
        public override Task<ScopeInfo> ProvisionAsync(string scopeName, SyncSetup setup = null, SyncProvision provision = SyncProvision.NotSet, bool overwrite = false,
            DbConnection connection = null, DbTransaction transaction = null, IProgress<ProgressArgs> progress = null, CancellationToken cancellationToken = default)
            => throw new NotImplementedException();

        /// <summary>
        /// Raise an error, as you are not authorized to launch a provision on the server from a client.
        /// </summary>
        public override Task<ScopeInfo> ProvisionAsync(SyncProvision provision = SyncProvision.NotSet, bool overwrite = false,
            DbConnection connection = null, DbTransaction transaction = null, IProgress<ProgressArgs> progress = null, CancellationToken cancellationToken = default)
            => throw new NotImplementedException();

        /// <summary>
        /// Raise an error, as you are not authorized to launch a provision on the server from a client.
        /// </summary>
        public override Task<ScopeInfo> ProvisionAsync(SyncSetup setup, SyncProvision provision = SyncProvision.NotSet, bool overwrite = false,
            DbConnection connection = null, DbTransaction transaction = null, IProgress<ProgressArgs> progress = null, CancellationToken cancellationToken = default)
            => throw new NotImplementedException();

        /// <summary>
        /// Raise an error, as you are not authorized to launch a deprovision on the server from a client.
        /// </summary>
        public override Task<bool> DeprovisionAsync(string scopeName, SyncSetup setup, SyncProvision provision = SyncProvision.NotSet,
            DbConnection connection = null, DbTransaction transaction = null, IProgress<ProgressArgs> progress = null, CancellationToken cancellationToken = default)
            => throw new NotImplementedException();

        /// <summary>
        /// Raise an error, as you are not authorized to launch a deprovision on the server from a client.
        /// </summary>
        public override Task<bool> DeprovisionAsync(
            SyncProvision provision = SyncProvision.NotSet,
            DbConnection connection = null, DbTransaction transaction = null, IProgress<ProgressArgs> progress = null, CancellationToken cancellationToken = default)
            => throw new NotImplementedException();

        /// <summary>
        /// Raise an error, as you are not authorized to launch a deprovision on the server from a client.
        /// </summary>
        public override Task<bool> DeprovisionAsync(SyncSetup setup, SyncProvision provision = SyncProvision.NotSet,
            DbConnection connection = null, DbTransaction transaction = null, IProgress<ProgressArgs> progress = null, CancellationToken cancellationToken = default)
            => throw new NotImplementedException();

        /// <summary>
        /// Raise an error, as you are not authorized to launch a deprovision on the server from a client.
        /// </summary>
        public override Task DropAllAsync(DbConnection connection = null, DbTransaction transaction = null, IProgress<ProgressArgs> progress = null, CancellationToken cancellationToken = default)
            => throw new NotImplementedException();

        /// <summary>
        /// Raise an error, as you are not authorized to launch a provision on the server from a client.
        /// </summary>
        internal override Task<bool> InternalShouldProvisionServerAsync(ScopeInfo sScopeInfo, SyncContext context, DbConnection connection, DbTransaction transaction, IProgress<ProgressArgs> progress, CancellationToken cancellationToken)
            => Task.FromResult(false);
    }
}