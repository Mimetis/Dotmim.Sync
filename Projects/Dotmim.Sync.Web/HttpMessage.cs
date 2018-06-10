using Dotmim.Sync.Batch;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Scope;
using Dotmim.Sync.Data.Surrogate;
using System;
using System.Collections.Generic;
using System.Text;
using Dotmim.Sync.Data;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Filter;

namespace Dotmim.Sync.Web
{
    /// <summary>
    /// Message send and receieved during http call
    /// </summary>
    [Serializable]
    public class HttpMessage
    {
        /// <summary>
        /// Context of the request / response
        /// </summary>
        public SyncContext SyncContext { get; set; }

        /// <summary>
        /// Gets or Sets the current step during proxy client communication
        /// </summary>
        public HttpStep Step { get; set; }


        /// <summary>
        /// Gets or Sets the message used when the session begins.
        /// Basically, exchange the configuration and check on both side
        /// </summary>
        public HttpBeginSessionMessage BeginSessionMessage { get; set; }


        /// <summary>
        /// Message sent during EnsureConfiguration stage
        /// </summary>
        public HttpEnsureSchemaMessage EnsureSchema { get; set; }

        /// <summary>
        /// Message sent during EnsureScope stage
        /// </summary>
        public HttpEnsureScopesMessage EnsureScopes { get; set; }

        /// <summary>
        /// Message sent during EnsureDatabase stage
        /// </summary>
        public HttpEnsureDatabaseMessage EnsureDatabase { get; set; }

        /// <summary>
        /// Message sent during GetChangeBatch stage
        /// </summary>
        public HttpGetChangeBatchMessage GetChangeBatch { get; set; }

        /// <summary>
        /// Message sent during ApplyChanges stage
        /// </summary>
        public HttpApplyChangesMessage ApplyChanges { get; set; }

        /// <summary>
        /// Message sent during GetLocalTimestamp stage
        /// </summary>
        public HttpGetLocalTimestampMessage GetLocalTimestamp { get; set; }

        /// <summary>
        /// Message sent during WriteScopes stage
        /// </summary>
        public HttpWriteScopesMessage WriteScopes { get; set; }

      
    }


    [Serializable]
    public class HttpBeginSessionMessage
    {
        public SyncConfiguration SyncConfiguration { get; set; }
    }

    [Serializable]
    public class HttpGetChangeBatchMessage
    {
        public DmSet Schema { get; set; }
        public int DownloadBatchSizeInKB { get; set; }
        public string BatchDirectory { get; set; }
        public ConflictResolutionPolicy Policy { get; set; }
        public ICollection<FilterClause> Filters { get; set; }

        public ScopeInfo ScopeInfo { get; set; }
        public int BatchIndexRequested { get; set; }
        public Boolean InMemory { get; set; }
        public BatchPartInfo BatchPartInfo { get; set; }
        public DmSetSurrogate Set { get; set; }
        public ChangesSelected ChangesSelected { get; set; }
    }
    [Serializable]
    public class HttpGetLocalTimestampMessage
    {
        public String ScopeInfoTableName { get; set; }
        public Int64 LocalTimestamp { get; set; }
    }

    [Serializable]
    public class HttpApplyChangesMessage
    {
        public ScopeInfo ScopeInfo { get; set; }
        public DmSetSurrogate Schema { get; set; }
        public ConflictResolutionPolicy Policy { get; set; }
        public Boolean UseBulkOperations { get; set; }
        public String ScopeInfoTableName { get; set; }

        public Boolean InMemory { get; set; }
        public int BatchIndex { get; set; }
        public BatchPartInfo BatchPartInfo { get; set; }
        public DmSetSurrogate Set { get; set; }
        public ChangesApplied ChangesApplied { get; set; }


    }

    [Serializable]
    public class HttpEnsureScopesMessage
    {
        public Guid? ClientReferenceId { get; set; }

        public List<ScopeInfo> Scopes { get; set; }
        public String ScopeInfoTableName { get; set; }
        public String ScopeName { get; set; }
    }

    [Serializable]
    public class HttpWriteScopesMessage
    {
        public String ScopeInfoTableName { get; set; }
        public List<ScopeInfo> Scopes { get; set; }
    }

    [Serializable]
    public class HttpEnsureSchemaMessage
    {
        /// <summary>
        /// Gets or Sets the tables schema
        /// </summary>
        public DmSetSurrogate Schema { get; set; }
    }

    [Serializable]
    public class HttpEnsureDatabaseMessage
    {
        public ScopeInfo ScopeInfo { get; set; }
        public DmSetSurrogate Schema { get; set; }
        public ICollection<FilterClause> Filters { get; set; }
    }


}
