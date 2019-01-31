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
        public Object Content { get; set; }

        ///// <summary>
        ///// Gets or Sets the message used when the session begins.
        ///// Basically, exchange the configuration and check on both side
        ///// </summary>
        //public HttpMessageBeginSession BeginSession { get; set; }

        ///// <summary>
        ///// Message sent during EnsureConfiguration stage
        ///// </summary>
        //public HttpEnsureSchemaMessage EnsureSchema { get; set; }

        ///// <summary>
        ///// Message sent during EnsureScope stage
        ///// </summary>
        //public HttpEnsureScopesMessage EnsureScopes { get; set; }

        ///// <summary>
        ///// Message sent during EnsureDatabase stage
        ///// </summary>
        //public HttpMessageEnsureDatabase EnsureDatabase { get; set; }

        ///// <summary>
        ///// Message sent during GetChangeBatch stage
        ///// </summary>
        //public HttpMessageGetChangesBatch GetChangeBatch { get; set; }

        ///// <summary>
        ///// Message sent during ApplyChanges stage
        ///// </summary>
        //public HttpMessageApplyChanges ApplyChanges { get; set; }

        ///// <summary>
        ///// Message sent during GetLocalTimestamp stage
        ///// </summary>
        //public HttpMessageTimestamp GetLocalTimestamp { get; set; }

        ///// <summary>
        ///// Message sent during WriteScopes stage
        ///// </summary>
        //public HttpMessageWriteScopes WriteScopes { get; set; }

    }

    [Serializable]
    public class HttpMessageBeginSession : MessageBeginSession
    {
    }


    [Serializable]
    public class HttpMessageGetChangesBatch: MessageGetChangesBatch
    {
        /// <summary>
        /// Gets or Sets the tables schema. Overriding the default Schema DmSet property, which is not serializable
        /// </summary>
        public new DmSetSurrogate Schema { get; set; }

        /// <summary>
        /// Gets the output batch index, returned by the server
        /// </summary>
        public int BatchIndexRequested { get; set; }

        /// <summary>
        /// Gets the output in memory flag from the server
        /// </summary>
        public Boolean InMemory { get; set; }

        /// <summary>
        /// Gets the BatchParInfo returned by the server
        /// </summary>
        public BatchPartInfo BatchPartInfo { get; set; }

        /// <summary>
        /// Gets the DmSet containing the data for the corresponding batch, returned by the server
        /// </summary>
        public DmSetSurrogate Set { get; set; }

        /// <summary>
        /// Gets the changes statistics
        /// </summary>
        public DatabaseChangesSelected ChangesSelected { get; set; }
    }


    [Serializable]
    public class HttpMessageTimestamp : MessageTimestamp
    {
        /// <summary>
        /// Gets the output result from server : The server timestamp
        /// </summary>
        public Int64 LocalTimestamp { get; set; }
    }

    [Serializable]
    public class HttpMessageApplyChanges : MessageApplyChanges
    {
        /// <summary>
        /// Gets or Sets the tables schema. Overriding the default Schema DmSet property, which is not serializable
        /// </summary>
        public new DmSetSurrogate Schema { get; set; }

        /// <summary>
        /// Gets the current batch index, exchanged between server and client
        /// </summary>
        public int BatchIndex { get; set; }

        /// <summary>
        /// Gets the output in memory flag exchanged between server and client
        /// </summary>
        public Boolean InMemory { get; set; }

        /// <summary>
        /// Gets the BatchParInfo exchanged between server and client
        /// </summary>
        public BatchPartInfo BatchPartInfo { get; set; }

        /// <summary>
        /// Gets the DmSet containing the data for the corresponding batch, returned by the server
        /// </summary>
        public DmSetSurrogate Set { get; set; }

        /// <summary>
        /// Gets the changes applied stats from the server
        /// </summary>
        public DatabaseChangesApplied ChangesApplied { get; set; }


    }

    [Serializable]
    public class HttpMessageEnsureScopes : MessageEnsureScopes
    {
        /// <summary>
        /// Gets the result from server : Scopes returned.
        /// </summary>
        public List<ScopeInfo> Scopes { get; set; }
    }

    [Serializable]
    public class HttpMessageEnsureSchema : MessageEnsureSchema
    {
        /// <summary>
        /// Gets or Sets the tables schema. Overriding the default Schema DmSet property, which is not serializable
        /// </summary>
        public new DmSetSurrogate Schema { get; set; }
    }

    [Serializable]
    public class HttpMessageEnsureDatabase : MessageEnsureDatabase
    {
        /// <summary>
        /// Gets or Sets the tables schema. Overriding the default Schema DmSet property, which is not serializable
        /// </summary>
        public new DmSetSurrogate Schema { get; set; }
 
    }

    [Serializable]
    public class HttpMessageWriteScopes : MessageWriteScopes
    {

    }


}
