using Dotmim.Sync.Batch;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Scope;
using Dotmim.Sync.Data.Surrogate;
using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Web
{
    /// <summary>
    /// Message send and receieved during http call
    /// </summary>
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
        /// Message sent during EnsureConfiguration stage
        /// </summary>
        public HttpEnsureConfigurationMessage EnsureConfiguration { get; set; }

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


    public class HttpGetChangeBatchMessage
    {
        public ScopeInfo ScopeInfo { get; set; }
        public int BatchIndexRequested { get; set; }
        public Boolean InMemory { get; set; }
        public BatchPartInfo BatchPartInfo { get; set; }
        public DmSetSurrogate Set { get; set; }
        public ChangesStatistics ChangesStatistics { get; set; }
    }
    public class HttpGetLocalTimestampMessage
    {
        public Int64 LocalTimestamp { get; set; }
    }

    public class HttpApplyChangesMessage
    {
        public ScopeInfo ScopeInfo { get; set; }
        public Boolean InMemory { get; set; }
        public int BatchIndex { get; set; }
        public BatchPartInfo BatchPartInfo { get; set; }
        public DmSetSurrogate Set { get; set; }
        public ChangesStatistics ChangesStatistics { get; set; }

    }

    public class HttpEnsureScopesMessage
    {
        public String ScopeName { get; set; }

        public Guid? ClientReferenceId { get; set; }

        public List<ScopeInfo> Scopes { get; set; }
    }

    public class HttpWriteScopesMessage
    {
        public List<ScopeInfo> Scopes { get; set; }
    }

    public class HttpEnsureConfigurationMessage
    {
        /// <summary>
        /// Contains the configuration from the server, to be applied on client, without the Set tables
        /// </summary>
        public SyncConfiguration Configuration { get; set; }

        /// <summary>
        /// Contains the Configuration Set tables
        /// </summary>
        public DmSetSurrogate ConfigurationSet { get; set; }
    }

    public class HttpEnsureDatabaseMessage
    {
        public ScopeInfo ScopeInfo { get; set; }
        public DbBuilderOption DbBuilderOption { get; set; }
    }
}
