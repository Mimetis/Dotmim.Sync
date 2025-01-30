using System;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync.Web.Client
{
    /// <summary>
    /// Contains the forbidden logic to handle constraints on the server side.
    /// </summary>
    public partial class WebRemoteOrchestrator : RemoteOrchestrator
    {
        /// <summary>
        /// Http Client is not authorized to ask for a table reset on the server.
        /// </summary>
        public override Task ResetTableAsync(ScopeInfo scopeInfo, string tableName, string schemaName = null, DbConnection connection = null, DbTransaction transaction = null,
            IProgress<ProgressArgs> progress = null, CancellationToken cancellationToken = default)
            => throw new NotImplementedException();

        /// <summary>
        /// Http Client is not authorized to ask for a table reset on the server.
        /// </summary>
        public override Task ResetTablesAsync(ScopeInfo scopeInfo, DbConnection connection = null, DbTransaction transaction = null, IProgress<ProgressArgs> progress = null, CancellationToken cancellationToken = default)
            => throw new NotImplementedException();

        /// <summary>
        /// Http Client is not authorized to command a disable constraints on the server.
        /// </summary>
        public override Task DisableConstraintsAsync(ScopeInfo scopeInfo, string tableName, string schemaName = null, DbConnection connection = null, DbTransaction transaction = null,
            IProgress<ProgressArgs> progress = null, CancellationToken cancellationToken = default)
            => throw new NotImplementedException();

        /// <summary>
        /// Http Client is not authorized to command an enable constraints on the server.
        /// </summary>
        public override Task EnableConstraintsAsync(ScopeInfo scopeInfo, string tableName, string schemaName = null, DbConnection connection = null, DbTransaction transaction = null,
            IProgress<ProgressArgs> progress = null, CancellationToken cancellationToken = default)
            => throw new NotImplementedException();

        /// <summary>
        /// Http Client is not authorized to ask for a table reset on the server.
        /// </summary>
        internal override Task ResetTableAsync(ScopeInfo scopeInfo, SyncContext context, string tableName, string schemaName = null, DbConnection connection = null, DbTransaction transaction = null,
            IProgress<ProgressArgs> progress = null, CancellationToken cancellationToken = default)
            => throw new NotImplementedException();
    }
}