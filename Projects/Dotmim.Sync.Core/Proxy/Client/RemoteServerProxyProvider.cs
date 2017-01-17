using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;
using Dotmim.Sync.Core.Batch;
using Dotmim.Sync.Core.Builders;
using Dotmim.Sync.Core.Manager;
using Dotmim.Sync.Core.Scope;
using Dotmim.Sync.Data;
using Microsoft.AspNetCore.Http;
using DmBinaryFormatter;

namespace Dotmim.Sync.Core.Proxy.Client
{
    public class RemoteServerProxyProvider : CoreProvider
    {
        private HttpRequest httpRequest;


        /// <summary>
        /// Construct a new RemoteServerProxy with a local used provider
        /// </summary>
        public RemoteServerProxyProvider(CoreProvider remoteProvider)
        {
            this.RemoteProvider = remoteProvider;
        }

        public ChangeSetResponse HandleRequest(HttpRequest httpRequest)
        {
            this.httpRequest = httpRequest;

            DmSerializer serializer = new DmSerializer();

            ChangeSetRequest syncRequest = serializer.Deserialize<ChangeSetRequest>(httpRequest.Body);

            switch(syncRequest.Step)
            {
                case HttpStep.BeginSession:
                    this.BeginSession();
                    break;
                case HttpStep.ApplyConfiguration:
                    this.ApplyConfiguration();
                    break;
                case HttpStep.EnsureDatabase:
                    this.EnsureDatabase(DbBuilderOption.UseExistingSchema | DbBuilderOption.UseExistingTrackingTables);
                    break;
                case HttpStep.EnsureScopes:
                    this.EnsureScopes(syncRequest.ServerScopeName, syncRequest.ClientScopeName);
                    break;
                case HttpStep.GetChangeBatch:
                    this.GetChangeBatch();
                    break;
                case HttpStep.ApplyChanges:
                    //this.ApplyChanges(syncRequest.ChangeSet.ScopeInfo, syncRequest.ChangeSet.Progress);
                    break;
                case HttpStep.GetLocalTimestamp:
                    break;
                case HttpStep.WriteScopes:
                    this.WriteScopes();
                    break;
                case HttpStep.EndSession:
                    this.EndSession();
                    break;
            }

          
            return null;
        }



        public override void ApplyChanges(ScopeInfo fromScope, BatchInfo changes)
        {
            base.ApplyChanges(fromScope, changes);
        }

        /// <summary>
        /// internal used provider (like a SqlSyncProvider)
        /// </summary>
        public CoreProvider RemoteProvider { get; private set; }

        public override DbConnection CreateConnection()
        {
            return this.RemoteProvider.CreateConnection();
        }

        public override DbBuilder GetDatabaseBuilder(DmTable tableDescription, DbBuilderOption options = DbBuilderOption.UseExistingSchema)
        {
            return this.RemoteProvider.GetDatabaseBuilder(tableDescription, options);
        }

        public override DbManager GetDbManager(string tableName)
        {
            return this.RemoteProvider.GetDbManager(tableName);
        }

        public override DbScopeBuilder GetScopeBuilder()
        {
            return this.RemoteProvider.GetScopeBuilder();
        }

        public override SyncBatchSerializer GetSerializer()
        {
            return this.RemoteProvider.GetSerializer();
        }
    }
}
