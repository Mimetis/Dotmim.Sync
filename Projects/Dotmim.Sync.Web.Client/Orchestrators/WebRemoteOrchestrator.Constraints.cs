using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Dotmim.Sync.Batch;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Serialization;

namespace Dotmim.Sync.Web.Client
{
    public partial class WebRemoteOrchestrator : RemoteOrchestrator
    {
        /// <summary>
        /// Http Client is not authorized to ask for a table reset on the server
        /// </summary>
        public override Task ResetTableAsync(ScopeInfo scopeInfo, string tableName, string schemaName = null, DbConnection connection = null, DbTransaction transaction = null)
            => throw new NotImplementedException();

        /// <summary>
        /// Http Client is not authorized to ask metadatas deletion on the server
        /// </summary>
        public override Task ResetTableAsync(ScopeInfo scopeInfo, SyncContext context, string tableName, string schemaName = null, DbConnection connection = null, DbTransaction transaction = null)
            => throw new NotImplementedException();

        /// <summary>
        /// Http Client is not authorized to command a disable constraints on the server
        /// </summary>
        public override Task DisableConstraintsAsync(ScopeInfo scopeInfo, string tableName, string schemaName = null, DbConnection connection = null, DbTransaction transaction = null)
            => throw new NotImplementedException();

        /// <summary>
        /// Http Client is not authorized to command an enable constraints on the server
        /// </summary>
        public override Task EnableConstraintsAsync(ScopeInfo scopeInfo, string tableName, string schemaName = null, DbConnection connection = null, DbTransaction transaction = null)
            => throw new NotImplementedException();

    }
}
