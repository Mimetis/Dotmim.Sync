using Dotmim.Sync.Batch;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Data.Surrogate;
using System;
using System.Collections.Generic;
using System.Text;
using Dotmim.Sync.Data;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Filter;
using Dotmim.Sync.Messages;

namespace Dotmim.Sync.Web.Client
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
        /// Content of the message. Can represent each HttpMessage**
        /// </summary>
        public object Content { get; set; }

    }


    //[Serializable]
    //public class HttpMessageGetChangesBatch : MessageGetChangesBatch
    //{
    //    /// <summary>
    //    /// Gets or Sets the tables schema. Overriding the default Schema DmSet property, which is not serializable
    //    /// </summary>
    //    public new DmSet Schema { get; set; }

    //    /// <summary>
    //    /// Gets the output batch index, returned by the server
    //    /// </summary>
    //    public int BatchIndexRequested { get; set; }

    //    /// <summary>
    //    /// Gets the output in memory flag from the server
    //    /// </summary>
    //    public Boolean InMemory { get; set; }

    //    /// <summary>
    //    /// Gets the BatchParInfo returned by the server
    //    /// </summary>
    //    public BatchPartInfo BatchPartInfo { get; set; }

    //    /// <summary>
    //    /// Gets the DmSet containing the data for the corresponding batch, returned by the server
    //    /// </summary>
    //    public DmSetSurrogate Set { get; set; }

    //    /// <summary>
    //    /// Gets the changes statistics
    //    /// </summary>
    //    public DatabaseChangesSelected ChangesSelected { get; set; }
    //}

    [Serializable]

    public class HttpMessageSendChangesResponse
    {
        /// <summary>
        /// Gets the current batch index, send from the server 
        /// </summary>
        public int BatchIndex { get; set; }

        /// <summary>
        /// Gets or Sets if this is the last Batch send from the server 
        /// </summary>
        public bool IsLastBatch { get; set; }

        /// <summary>
        /// Gets the BatchParInfo send from the server 
        /// </summary>
        public DmSet Changes { get; set; }

        /// <summary>
        /// Gets the changes applied stats from the server
        /// </summary>
        public DatabaseChangesSelected ChangesSelected { get; set; }

    }



    [Serializable]
    public class HttpMessageGetMoreChangesRequest
    {

        public int BatchIndexRequested { get; set; }
    }


    [Serializable]
    public class HttpMessageSendChangesRequest
    {

        public HttpMessageSendChangesRequest(Guid fromScopeId, ScopeInfo localScopeReferenceInfo, ScopeInfo serverScopeInfo)
        {
            this.FromScopeId = fromScopeId;
            this.LocalScopeReferenceInfo = localScopeReferenceInfo;
            this.ServerScopeInfo = serverScopeInfo;
        }

        /// <summary>
        /// Gets or Sets the local scope id sent to server
        /// </summary>
        public Guid FromScopeId { get; }

        /// <summary>
        /// Gets or Sets the reference scope for local repository, stored on server
        /// </summary>
        public ScopeInfo LocalScopeReferenceInfo { get; }

        /// <summary>
        /// Gets ors Sets the reference scope for server repository, stored on server
        /// </summary>
        public ScopeInfo ServerScopeInfo { get; }

        /// <summary>
        /// Get the current batch index (if InMemory == false)
        /// </summary>
        public int BatchIndex { get; set; }

        /// <summary>
        /// Gets or Sets if this is the last Batch to sent to server 
        /// </summary>
        public bool IsLastBatch { get; set; }

        /// <summary>
        /// Changes to send
        /// </summary>
        public DmSet Changes { get; set; }
    }


    [Serializable]
    public class HttpMessageEnsureScopesResponse
    {
        public HttpMessageEnsureScopesResponse(ScopeInfo serverScopeInfo, ScopeInfo localScopeReferenceInfo, SyncSchema schema)
        {
            this.ServerScopeInfo = serverScopeInfo ?? throw new ArgumentNullException(nameof(serverScopeInfo));
            this.LocalScopeReferenceInfo = localScopeReferenceInfo ?? throw new ArgumentNullException(nameof(localScopeReferenceInfo));
            this.Schema = schema ?? throw new ArgumentNullException(nameof(schema));
        }

        /// <summary>
        /// Gets or Sets the server scope stored within the server
        /// </summary>
        public ScopeInfo ServerScopeInfo { get; set; }

        /// <summary>
        /// Gets or Sets the client reference scope within the server
        /// </summary>

        public ScopeInfo LocalScopeReferenceInfo { get; set; }

        /// <summary>
        /// Gets or Sets the schema option (without schema itself, that is not serializable)
        /// </summary>
        public SyncSchema Schema { get; set; }
    }


    [Serializable]
    public class HttpMessageEnsureScopesRequest
    {

        /// <summary>
        /// Create a new message to web remote server.
        /// Scope info table name is not provided since we do not care about it on the server side
        /// </summary>
        public HttpMessageEnsureScopesRequest(string scopeName, Guid clientReferenceId)
        {
            this.ScopeName = scopeName;
            this.ClientReferenceId = clientReferenceId;
        }

        /// <summary>
        /// Gets or Sets the client id. If null, the ensure scope step is occuring on the client. If not null, we are on the server
        /// </summary>
        public Guid ClientReferenceId { get; private set; }

        /// <summary>
        /// Gets or Sets the scope name
        /// </summary>
        public string ScopeName { get; private set; }
    }
}
