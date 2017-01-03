using Dotmim.Sync.Core.Batch;
using Dotmim.Sync.Core.Builders;
using Dotmim.Sync.Core.Common;
using Dotmim.Sync.Core.Context;
using Dotmim.Sync.Core.Enumerations;
using Dotmim.Sync.Core.Log;
using Dotmim.Sync.Core.Manager;
using Dotmim.Sync.Core.Scope;
using Dotmim.Sync.Data;
using Dotmim.Sync.Enumerations;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;

namespace Dotmim.Sync.Core
{
    /// <summary>
    /// Core provider : should be implemented by any server / client provider
    /// </summary>
    public abstract class CoreProvider
    {
        bool _syncInProgress;

        string providerType;
        // internal object to manage progression
        SyncSetProgress setProgress;

        //event args used for get back info to the user
        ScopeProgressEventArgs progressEventArgs;

        internal object lockObject = new object();
        //SyncBatchProducer _syncBatchProducer;

        public ScopeInfo ClientScopeInfo { get; set; }
        public ScopeInfo ServerScopeInfo { get; set; }


        /// <summary>
        /// Gets or sets the maximum amount of memory (in KB) that Sync Framework uses to cache changes before 
        /// </summary>
        public int MemoryDataCacheSize { get; set; }

        /// <summary>
        /// Gets or sets the directory in which batch files are spooled to disk.
        /// </summary>
        public string BatchingDirectory { get; set; }

        internal void ThrowIfSyncInProgress(string propertyName)
        {

            if (this._syncInProgress)
                lock (this)
                    if (this._syncInProgress)
                        throw new Exception($"ConcurrentSyncInProgress {propertyName}");
        }

        /// <summary>
        /// Raise an event if the sync is outdated. 
        /// Let the user choose if he wants to force or not
        /// </summary>
        public event EventHandler<OutdatedEventArgs> SyncOutdated = null;

        /// <summary>
        /// Occurs during progress
        /// </summary>
        public event EventHandler<ScopeProgressEventArgs> SyncProgress = null;

        ///// <summary>
        ///// Occurs during progress
        ///// </summary>
        //public event EventHandler<ApplyChangeFailedEventArgs> ApplyChangedFailed = null;

        /// <summary>
        /// Create a new instance of the implemented Connection provider
        /// </summary>
        /// <returns></returns>
        public abstract DbConnection CreateConnection();

        /// <summary>
        /// Get the serializer
        /// </summary>
        public abstract SyncBatchSerializer GetSerializer();

        /// <summary>
        /// Get a table builder helper. Need a complete table description (DmTable). Will then generate table, table tracking, stored proc and triggers
        /// </summary>
        /// <returns></returns>
        public abstract DbBuilder GetDatabaseBuilder(DmTable tableDescription, DbBuilderOption options = DbBuilderOption.UseExistingSchema);

        /// <summary>
        /// Get a table manager, which can get informations directly from data source
        /// </summary>
        public abstract DbManager GetDbManager(string tableName);

        /// <summary>
        /// Create a Scope Builder, which can create scope table, and scope config
        /// </summary>
        /// <returns></returns>
        public abstract DbScopeBuilder GetScopeBuilder();

        /// <summary>
        /// Sync Agent configuration
        /// </summary>
        public ServiceConfiguration Configuration { get; set; }

        /// <summary>
        /// Get the provider type name
        /// </summary>
        public string ProviderTypeName
        {
            get
            {
                if (!string.IsNullOrEmpty(providerType))
                    return providerType;

                Type type = base.GetType();
                providerType = $"{type.Name}, {type.AssemblyQualifiedName}";

                return providerType;
            }
        }



        /// <summary>
        /// Called by the  to indicate that a 
        /// synchronization session has started.
        /// </summary>
        public void BeginSession()
        {
            try
            {

                lock (this)
                {
                    if (this._syncInProgress)
                        throw new Exception("Session already in progress");

                    this._syncInProgress = true;
                }

                // init progress handler
                this.setProgress = new SyncSetProgress();

                // init progress args
                this.progressEventArgs = new ScopeProgressEventArgs();
                this.progressEventArgs.ProviderTypeName = this.ProviderTypeName;
                this.progressEventArgs.Stage = SyncStage.BeginSession;
                this.progressEventArgs.Action = ChangeApplicationAction.Continue;
                this.SyncProgress?.Invoke(this, this.progressEventArgs);

                Logger.Current.Info($"BeginSession() called on Provider {this.ProviderTypeName}");
            }
            catch (Exception)
            {
                this._syncInProgress = false;
                throw;
            }


        }

        /// <summary>
        /// if We pass a DmSet, so it's already configured
        /// </summary>
        internal ServiceConfiguration BuildConfiguration(ServiceConfiguration configuration)
        {
            if (configuration.ScopeSet == null)
                throw new ArgumentNullException("configuration.ScopeSet");

            if (configuration.ScopeSet.Tables.Count == 0)
                throw new ArgumentNullException("You should pass a valid DmSet with valid tables");

            this.Configuration = configuration;

            // any check or other things to do on Configuration here ?

            this.progressEventArgs.Stage = SyncStage.BuildConfiguration;
            this.progressEventArgs.Action = ChangeApplicationAction.Continue;
            this.progressEventArgs.Configuration = this.Configuration;
            this.SyncProgress?.Invoke(this, this.progressEventArgs);


            return this.Configuration;
        }

        /// <summary>
        /// Construct a default configuration for a given table name
        /// </summary>
        /// <param name="tableName"></param>
        internal ServiceConfiguration BuildConfiguration(string[] tables)
        {
            // Set the Server configuration
            this.Configuration = ServiceConfiguration.CreateDefaultConfiguration();

            // Open the connection
            using (var connection = this.CreateConnection())
            {
                try
                {
                    connection.Open();

                    using (var transaction = connection.BeginTransaction(IsolationLevel.ReadUncommitted))
                    {
                        foreach (var table in tables)
                        {

                            var builderTable = this.GetDbManager(table);
                            var tblManager = builderTable.GetManagerTable(connection, transaction);
                            var dmTable = tblManager.GetTableDefinition();

                            this.Configuration.ScopeSet.Tables.Add(dmTable);
                        }

                        transaction.Commit();
                    }

                    connection.Close();
                }
                catch (Exception ex)
                {
                    Logger.Current.Error($"Error during building BuildConfiguration : {ex.Message}");

                    throw;
                }
                finally
                {
                    if (connection.State != ConnectionState.Closed)
                        connection.Close();
                }
            }


            // any check or other things to do on Configuration here ?

            this.progressEventArgs.Stage = SyncStage.BuildConfiguration;
            this.progressEventArgs.Action = ChangeApplicationAction.Continue;
            this.progressEventArgs.Configuration = this.Configuration;
            this.SyncProgress?.Invoke(this, this.progressEventArgs);

            return this.Configuration;
        }

        /// <summary>
        /// Build Progression
        /// </summary>
        private void BuildTableProgress()
        {

            try
            {
                //// Create all adapters
                foreach (var tableDescription in this.Configuration.ScopeSet.Tables)
                {
                    if (tableDescription.PrimaryKey == null)
                        throw new Exception($"Every table should have a primary key, unique.");

                    // clone the table to store progress
                    var dmTable = tableDescription.Clone();

                    // Adding the tracking columns
                    AddTrackingColumns<string>(dmTable, "create_scope_name");
                    AddTrackingColumns<long>(dmTable, "create_timestamp");
                    AddTrackingColumns<string>(dmTable, "update_scope_name");
                    AddTrackingColumns<long>(dmTable, "update_timestamp");

                    this.setProgress.AddTableProgress(dmTable);
                }


            }
            catch (Exception ex)
            {
                Logger.Current.Error($"Error during building adapters : {ex.Message}");
                throw;
            }


        }



        /// <summary>
        /// Called when the sync is over
        /// </summary>
        public void EndSession()
        {
            try
            {
                Logger.Current.Info($"EndSession() called on Provider {this.ProviderTypeName}");
                this.setProgress.TablesProgress.Clear();
                this.setProgress.Dispose();
                this.setProgress = null;

                this.progressEventArgs.Stage = SyncStage.EndSession;
                this.SyncProgress?.Invoke(this, this.progressEventArgs);
                this.progressEventArgs.Cleanup();
                this.progressEventArgs = null;
            }
            finally
            {
                lock (this)
                {
                    this._syncInProgress = false;
                }
            }
        }


        public void EnsureScopes(string serverScopeName, string clientScopeName = null)
        {
            if (string.IsNullOrEmpty(serverScopeName))
                throw new ArgumentNullException("ScopeName is mandatory");


            // Open the connection
            using (var connection = this.CreateConnection())
            {
                try
                {
                    connection.Open();

                    using (var transaction = connection.BeginTransaction(IsolationLevel.ReadUncommitted))
                    {
                        var scopeBuilder = this.GetScopeBuilder();
                        var scopeInfoBuilder = scopeBuilder.CreateScopeInfoBuilder(connection, transaction);

                        // create the scope info table if needed
                        if (scopeInfoBuilder.NeedToCreateScopeInfoTable())
                            scopeInfoBuilder.CreateScopeInfoTable();

                        // get all scopes
                        var lstScopes = scopeInfoBuilder.GetAllScopes();

                        // --------------------------------------
                        // SERVER SCOPE NAME
                        // --------------------------------------

                        // try to get the remote scope
                        var scopeInfo = lstScopes.FirstOrDefault(s => s.Name.Equals(serverScopeName, StringComparison.CurrentCultureIgnoreCase));

                        // create it if not exist
                        this.ServerScopeInfo = scopeInfo == null ?
                            scopeInfoBuilder.InsertOrUpdateScopeInfo(serverScopeName) :
                            scopeInfo;

                        // --------------------------------------
                        // CLIENT SCOPE
                        // --------------------------------------

                        // client scope name doesn't exist, create one.
                        // can't happen on server side, since it's mandatory
                        if (string.IsNullOrEmpty(clientScopeName))
                        {
                            // We are client side

                            // try to get the first one which is not a the server scope trace
                            var scopeInfoLocal = lstScopes.FirstOrDefault(s => !s.Name.Equals(serverScopeName, StringComparison.CurrentCultureIgnoreCase));

                            if (scopeInfoLocal != null)
                            {
                                this.ClientScopeInfo = scopeInfoLocal;
                            }
                            else
                            {
                                Guid clientId = Guid.NewGuid();
                                clientScopeName = string.Format(CultureInfo.InvariantCulture, "{0}_{1}", serverScopeName, clientId);
                                this.ClientScopeInfo = scopeInfoBuilder.InsertOrUpdateScopeInfo(clientScopeName);
                                this.ClientScopeInfo.IsNewScope = true;
                            }

                        }
                        else
                        {
                            // try to get the local scope
                            var scopeInfoLocal = lstScopes.FirstOrDefault(s => s.Name.Equals(clientScopeName, StringComparison.CurrentCultureIgnoreCase));

                            // create it if not exist
                            this.ClientScopeInfo = scopeInfoLocal == null ?
                                scopeInfoBuilder.InsertOrUpdateScopeInfo(clientScopeName) :
                                scopeInfoLocal;
                        }


                        transaction.Commit();
                    }

                }
                catch (Exception ex)
                {
                    Logger.Current.Error(ex.Message);
                    throw;
                }
                finally
                {
                    if (connection.State != ConnectionState.Closed)
                        connection.Close();
                }

            }

        }

        /// <summary>
        /// Ensure scopes are created
        /// On Client side, only one scope is created
        /// On Server side, we have multiples scope
        /// </summary>
        public List<ScopeInfo> EnsureScopes2(string scopeName, string clientScopeName = null)
        {
            if (string.IsNullOrEmpty(scopeName))
                throw new ArgumentNullException("ScopeName can't be empty when we talk about Remote datasource");

            var isServerSide = true;

            if (string.IsNullOrEmpty(clientScopeName))
                isServerSide = false;

            var scopes = new List<ScopeInfo>();

            // Open the connection
            using (var connection = this.CreateConnection())
            {
                try
                {
                    connection.Open();

                    using (var transaction = connection.BeginTransaction(IsolationLevel.ReadUncommitted))
                    {
                        ScopeInfo scopeInfoArgs = null;
                        ScopeInfo returnValue = null;
                        var scopeBuilder = this.GetScopeBuilder();

                        var scopeInfoBuilder = scopeBuilder.CreateScopeInfoBuilder(connection, transaction);

                        // create the scope info table if needed
                        if (scopeInfoBuilder.NeedToCreateScopeInfoTable())
                            scopeInfoBuilder.CreateScopeInfoTable();

                        // get all scopes
                        var lstScopes = scopeInfoBuilder.GetAllScopes();

                        if (isServerSide)
                        {
                            ScopeInfo scopeInfo = null, localScopeInfo = null;

                            // Try to get the scopes information
                            scopeInfo = lstScopes.FirstOrDefault(s => s.Name.Equals(scopeName, StringComparison.CurrentCultureIgnoreCase));
                            localScopeInfo = lstScopes.FirstOrDefault(s => s.Name.Equals(clientScopeName, StringComparison.CurrentCultureIgnoreCase));

                            if (scopeInfo == null)
                            {
                                scopeInfo = scopeInfoBuilder.InsertOrUpdateScopeInfo(scopeName);
                                scopeInfo.IsNewScope = true;
                            }

                            if (localScopeInfo == null)
                            {
                                localScopeInfo = scopeInfoBuilder.InsertOrUpdateScopeInfo(clientScopeName);
                                localScopeInfo.IsNewScope = true;
                            }

                            scopes.Add(scopeInfo);
                            scopes.Add(localScopeInfo);
                            scopeInfoArgs = scopeInfo;
                        }
                        else
                        {
                            ScopeInfo localScopeInfo = null;

                            // Get the first scope founded, since there is only one scope on client side
                            localScopeInfo = lstScopes.FirstOrDefault();

                            // if no scope, create the new one
                            if (localScopeInfo == null)
                            {
                                Guid clientId = Guid.NewGuid();
                                var newScopeName = string.Format(CultureInfo.InvariantCulture, "{0}_{1}", scopeName, clientId);

                                localScopeInfo = scopeInfoBuilder.InsertOrUpdateScopeInfo(newScopeName);
                                localScopeInfo.IsNewScope = true;
                            }

                            scopes.Add(localScopeInfo);
                            scopeInfoArgs = returnValue = localScopeInfo;
                        }

                        transaction.Commit();

                        this.progressEventArgs.ScopeInfo = scopeInfoArgs;
                        this.progressEventArgs.Stage = SyncStage.ReadingScope;
                        this.SyncProgress?.Invoke(this, this.progressEventArgs);
                        return scopes;
                    }
                }
                catch (Exception ex)
                {
                    Logger.Current.Error(ex.Message);
                    throw;
                }
                finally
                {
                    if (connection.State != ConnectionState.Closed)
                        connection.Close();
                }

            }

        }


        /// <summary>
        /// Write scope in the provider datasource
        /// </summary>
        public void WriteScopes()
        {
            // Open the connection
            using (var connection = this.CreateConnection())
            {
                try
                {
                    connection.Open();
                    var scopeBuilder = this.GetScopeBuilder();
                    var scopeInfoBuilder = scopeBuilder.CreateScopeInfoBuilder(connection);
                    ClientScopeInfo = scopeInfoBuilder.InsertOrUpdateScopeInfo(ClientScopeInfo.Name);
                    ServerScopeInfo = scopeInfoBuilder.InsertOrUpdateScopeInfo(ServerScopeInfo.Name);

                    this.progressEventArgs.Stage = SyncStage.WritingScope;
                    this.progressEventArgs.ScopeInfo = ClientScopeInfo;
                    this.SyncProgress?.Invoke(this, this.progressEventArgs);

                }
                catch (Exception ex)
                {
                    Logger.Current.Error(ex.Message);
                    throw;
                }
                finally
                {
                    if (connection.State != ConnectionState.Closed)
                        connection.Close();
                }
            }
        }


        /// <summary>
        /// Read a scope info
        /// </summary>
        /// <returns></returns>
        public long GetLocalTimestamp()
        {
            // Open the connection
            using (var connection = this.CreateConnection())
            {
                try
                {
                    connection.Open();
                    var scopeBuilder = this.GetScopeBuilder();
                    var scopeInfoBuilder = scopeBuilder.CreateScopeInfoBuilder(connection);

                    return scopeInfoBuilder.GetLocalTimestamp();
                }
                catch (Exception ex)
                {
                    Logger.Current.Error(ex.Message);
                    throw;
                }
                finally
                {
                    if (connection.State != ConnectionState.Closed)
                        connection.Close();
                }
            }
        }

        /// <summary>
        /// Be sure all tables are ready and configured for sync
        /// the ScopeSet Configuration MUST be filled by the schema form Database
        /// </summary>
        public void EnsureDatabase(DbBuilderOption options)
        {
            if (Configuration == null || Configuration.ScopeSet == null || Configuration.ScopeSet.Tables.Count == 0)
                throw new ArgumentNullException("Configuration or Configuration.ScopeSet");

            string script = null;

            // Open the connection
            using (var connection = this.CreateConnection())
            {
                try
                {
                    connection.Open();

                    using (var transaction = connection.BeginTransaction(IsolationLevel.ReadUncommitted))
                    {
                        foreach (var dmTable in Configuration.ScopeSet.Tables)
                        {
                            var builder = GetDatabaseBuilder(dmTable, options);
                            script = builder.Script(connection, transaction);
                            builder.Apply(connection, transaction);
                        }

                        transaction.Commit();
                    }

                    this.progressEventArgs.Stage = SyncStage.EnsureDatabase;
                    this.progressEventArgs.DatabaseScript = script;
                    this.SyncProgress?.Invoke(this, this.progressEventArgs);

                }
                catch
                {
                    throw;
                }
                finally
                {
                    if (connection.State != ConnectionState.Closed)
                        connection.Close();
                }
            }
        }


        /// <summary>
        /// TODO : Manager le fait qu'un scope peut être out dater, car il n'a pas synchronisé depuis assez longtemps
        /// </summary>
        internal bool IsRemoteOutdated()
        {
            //var lastCleanupTimeStamp = 0; // A établir comment récupérer la dernière date de clean up des metadatas
            //return (ScopeInfo.LastTimestamp < lastCleanupTimeStamp);

            return false;
        }

        /// <summary>
        /// Gets a batch of changes to synchronize when given batch size, 
        /// destination knowledge, and change data retriever parameters.
        /// </summary>
        /// <returns>A DbSyncContext object that will be used to retrieve the modified data.</returns>
        public SyncSetProgress GetChangeBatch()
        {
            if (this.ClientScopeInfo == null)
                throw new ArgumentException("ClientScope is null");

            // check batchSize if not > then MemoryDataCacheSize
            if (this.MemoryDataCacheSize > 0)
                Logger.Current.Info($"Enumeration data cache size selected: {MemoryDataCacheSize} Kb");

            this.progressEventArgs.Stage = SyncStage.SelectedChanges;

            this.BuildTableProgress();

            this.GetChanges();

            if (this.setProgress.IsOutdated)
                throw new Exception("OutDatedPeer");


            return this.setProgress;
        }

        internal void GetChanges()
        {
            var serializer = this.GetSerializer();

            this.setProgress.serializer = serializer;

            try
            {
                Logger.Current.Info("GetChanges called: _syncBatchProducer is null");

                // Check if the remote is not outdated
                this.setProgress.IsOutdated = this.IsRemoteOutdated();

                // Get a chance to make the sync even if it's outdated
                if (this.setProgress.IsOutdated && this.SyncOutdated != null)
                {
                    Logger.Current.Info("Raising Sync Remote Outdated Event");
                    var outdatedEventArgs = new OutdatedEventArgs();
                    this.SyncOutdated(this, outdatedEventArgs);
                    Logger.Current.Info($"Action taken : {outdatedEventArgs.Action.ToString()}");

                    if (outdatedEventArgs.Action == OutdatedSyncAction.PartialSync)
                    {
                        Logger.Current.Info("Attempting Partial Sync");
                        this.setProgress.IsOutdated = false;
                    }
                }

                // the sync is still outdated, abort it
                if (this.setProgress.IsOutdated)
                {
                    Logger.Current.Info("Aborting Sync");
                    return;
                }

                if (this.MemoryDataCacheSize == 0)
                    this.EnumerateChangesInternal();
                else
                    this.EnumerateChangesInBatchesInternal();

                Logger.Current.Info("Committing transaction");

            }
            catch (Exception exception)
            {
                Logger.Current.Error($"Caught exception while getting changes: {exception}");
                throw;
            }

            finally
            {
            }
        }

        /// <summary>
        /// Enumerate all internal changes, no batch mode
        /// </summary>
        internal void EnumerateChangesInternal()
        {
            Logger.Current.Info($"----- Enumerating Changes for Scope \"{this.ClientScopeInfo.Name}\" -----");
            Logger.Current.Info("");
            Logger.Current.Info("");

            using (var connection = this.CreateConnection())
            {
                // Open the connection
                connection.Open();

                using (var transaction = connection.BeginTransaction(IsolationLevel.ReadUncommitted))
                {

                    try
                    {
                        foreach (var tableDescription in this.Configuration.ScopeSet.Tables)
                        {

                            var builder = this.GetDatabaseBuilder(tableDescription);
                            var syncAdapter = builder.CreateSyncAdapter(connection, transaction);
                            syncAdapter.TableDescription = tableDescription;
                            syncAdapter.ObjectNames = builder.ObjectNames;

                            Logger.Current.Info($"----- Table \"{syncAdapter.TableDescription.TableName}\" -----");

                            // get the select incremental changes command
                            DbCommand selectIncrementalChangesCommand = connection.CreateCommand();
                            selectIncrementalChangesCommand.Connection = connection;
                            selectIncrementalChangesCommand.CommandType = CommandType.StoredProcedure;
                            selectIncrementalChangesCommand.CommandText = syncAdapter.ObjectNames.GetObjectName(DbObjectType.SelectChangesProcName);
                            if (transaction != null)
                                selectIncrementalChangesCommand.Transaction = transaction;
                            syncAdapter.SetCommandSessionParameters(selectIncrementalChangesCommand);

                            if (selectIncrementalChangesCommand == null)
                            {
                                var exc = "Missing command 'SelectIncrementalChangesCommand' ";
                                Logger.Current.Error(exc);
                                throw new Exception(exc);
                            }

                            var tableProgress = this.setProgress.FindTableProgress(syncAdapter.TableDescription.TableName);


                            // Set the parameters
                            DbManager.SetParameterValue(selectIncrementalChangesCommand, "sync_min_timestamp", this.ClientScopeInfo.LastTimestamp);
                            DbManager.SetParameterValue(selectIncrementalChangesCommand, "sync_scope_name", this.ClientScopeInfo.Name);

                            this.AddTrackingColumns<int>(tableProgress.Changes, "sync_row_is_tombstone");

                            // Get the reader
                            using (var dataReader = selectIncrementalChangesCommand.ExecuteReader())
                            {
                                var dmTable = tableProgress.Changes;

                                while (dataReader.Read())
                                {
                                    DmRow dataRow = CreateRowFromReader(dataReader, dmTable);

                                    // assuming the row is not inserted / modified
                                    DmRowState state = DmRowState.Unchanged;

                                    // get if the current row is inserted, modified, deleted
                                    state = GetStateFromDmRow(dataRow);

                                    if (state != DmRowState.Deleted && state != DmRowState.Modified && state != DmRowState.Added)
                                        continue;

                                    // add row
                                    dmTable.Rows.Add(dataRow);

                                    // acceptchanges before modifying 
                                    dataRow.AcceptChanges();

                                    // Set the correct state to be applied
                                    if (state == DmRowState.Deleted)
                                        dataRow.Delete();
                                    else if (state == DmRowState.Added)
                                        dataRow.SetAdded();
                                    else if (state == DmRowState.Modified)
                                        dataRow.SetModified();
                                }

                                // Since we dont need this column anymore, remove it
                                this.RemoveTrackingColumns(tableProgress.Changes, "sync_row_is_tombstone");

                                // Create a progress event
                                var changes = new ScopeSelectedChanges();
                                changes.View = new DmView(tableProgress.Changes);
                                this.progressEventArgs.SelectedChanges.Add(changes);
                            }

                            Logger.Current.Info($"--- End Table \"{syncAdapter.TableDescription.TableName}\" ---");
                            Logger.Current.Info("");
                        }

                        this.progressEventArgs.Stage = SyncStage.SelectedChanges;
                        this.SyncProgress?.Invoke(this, this.progressEventArgs);

                        transaction.Commit();
                    }
                    catch (Exception dbException)
                    {
                        Logger.Current.Error($"Caught exception while enumerating changes\n{dbException}\n");
                        throw;
                    }
                    finally
                    {
                        if (connection != null && connection.State == ConnectionState.Open)
                            connection.Close();
                    }

                }

            }
            Logger.Current.Info($"--- End Enumerating Changes for Scope \"{this.ClientScopeInfo.Name}\" ---");
            Logger.Current.Info("");
        }

        /// <summary>
        /// Enumerate all internal changes, no batch mode
        /// </summary>
        internal void EnumerateChangesInBatchesInternal()
        {
            Logger.Current.Info($"----- Enumerating Changes for Scope \"{this.ClientScopeInfo.Name}\" -----");
            Logger.Current.Info("");
            Logger.Current.Info("");

            // memory size total
            long memorySizeFromDmRows = 0L;

            using (var connection = this.CreateConnection())
            {
                // Open the connection
                connection.Open();

                using (var transaction = connection.BeginTransaction(IsolationLevel.ReadUncommitted))
                {

                    foreach (var tableDescription in this.Configuration.ScopeSet.Tables)
                    {

                        var builder = this.GetDatabaseBuilder(tableDescription);
                        var syncAdapter = builder.CreateSyncAdapter(connection, transaction);
                        syncAdapter.TableDescription = tableDescription;
                        syncAdapter.ObjectNames = builder.ObjectNames;

                        Logger.Current.Info($"----- Table \"{syncAdapter.TableDescription.TableName}\" -----");

                        // get the select incremental changes command
                        DbCommand selectIncrementalChangesCommand = connection.CreateCommand();
                        selectIncrementalChangesCommand.Connection = connection;
                        selectIncrementalChangesCommand.CommandType = CommandType.StoredProcedure;
                        selectIncrementalChangesCommand.CommandText = syncAdapter.ObjectNames.GetObjectName(DbObjectType.SelectChangesProcName);
                        if (transaction != null)
                            selectIncrementalChangesCommand.Transaction = transaction;
                        syncAdapter.SetCommandSessionParameters(selectIncrementalChangesCommand);


                        if (selectIncrementalChangesCommand == null)
                        {
                            var exc = "Missing command 'SelectIncrementalChangesCommand' ";
                            Logger.Current.Error(exc);
                            throw new Exception(exc);
                        }

                        var tableProgress = this.setProgress.FindTableProgress(syncAdapter.TableDescription.TableName);

                        try
                        {

                            // Set the parameters
                            DbManager.SetParameterValue(selectIncrementalChangesCommand, "sync_min_timestamp", this.ClientScopeInfo.LastTimestamp);
                            DbManager.SetParameterValue(selectIncrementalChangesCommand, "sync_scope_name", this.ClientScopeInfo.Name);

                            this.AddTrackingColumns<int>(tableProgress.Changes, "sync_row_is_tombstone");

                            // Get the reader
                            using (var dataReader = selectIncrementalChangesCommand.ExecuteReader())
                            {
                                var dmTable = tableProgress.Changes;

                                while (dataReader.Read())
                                {
                                    DmRow dmRow = CreateRowFromReader(dataReader, dmTable);
                                    DmRowState state = DmRowState.Unchanged;

                                    state = GetStateFromDmRow(dmRow);

                                    // If the row is not deleted inserted or modified, go next
                                    if (state != DmRowState.Deleted && state != DmRowState.Modified && state != DmRowState.Added)
                                        continue;


                                    var dmRowSize = DbManager.GetRowSizeFromDataRow(dmRow);

                                    if (dmRowSize > this.MemoryDataCacheSize)
                                    {
                                        var exc = $"Row is too big ({dmRowSize} kb.) for the current MemoryDataCacheSize ({MemoryDataCacheSize} kb.) Aborting Sync...";
                                        Logger.Current.Error(exc);
                                        throw new Exception(exc);
                                    }

                                    // Calculate the new memory size
                                    memorySizeFromDmRows = memorySizeFromDmRows + dmRowSize;

                                    // add row
                                    dmTable.Rows.Add(dmRow);

                                    // acceptchanges before modifying 
                                    dmRow.AcceptChanges();

                                    // Set the correct state to be applied
                                    if (state == DmRowState.Deleted)
                                        dmRow.Delete();
                                    else if (state == DmRowState.Added)
                                        dmRow.SetAdded();
                                    else if (state == DmRowState.Modified)
                                        dmRow.SetModified();

                                    // We exceed the memorySize, so we can add it to a batch
                                    if (memorySizeFromDmRows > MemoryDataCacheSize)
                                    {
                                        // Create a batch item containing:
                                        // Les noms des tables contenues
                                        // Les données des tables
                                        // Un Id
                                        // Si c'est le dernier batch
                                        // Un numéro d'ordre

                                        // Since we dont need this column anymore, remove it
                                        this.RemoveTrackingColumns(tableProgress.Changes, "sync_row_is_tombstone");

                                        //// Create a progress event
                                        //var changes = new ScopeSelectedChanges();
                                        //changes.View = new DmView(tableProgress.Changes);
                                        //this.progressEventArgs.SelectedChanges.Add(changes);
                                        // Erase la table
                                        dmTable.Clone();

                                        // Init the row memory size
                                        memorySizeFromDmRows = 0L;
                                    }
                                }


                                // Since we dont need this column anymore, remove it
                                this.RemoveTrackingColumns(tableProgress.Changes, "sync_row_is_tombstone");

                                // Create a progress event
                                var selectedChanges = new ScopeSelectedChanges();
                                selectedChanges.View = new DmView(tableProgress.Changes);
                                this.progressEventArgs.SelectedChanges.Add(selectedChanges);
                                this.progressEventArgs.Stage = SyncStage.SelectedChanges;
                                this.SyncProgress?.Invoke(this, this.progressEventArgs);


                                //this.SyncSelectedChanges?.Invoke(this, selectedChanges);

                                // Erase la table
                                dmTable.Clone();

                                // Init the row memory size
                                memorySizeFromDmRows = 0L;

                            }
                        }
                        catch (Exception dbException)
                        {
                            Logger.Current.Error($"Caught exception while enumerating changes\n{dbException}\n");
                            throw;
                        }
                        finally
                        {

                            Logger.Current.Info($"--- End Table \"{syncAdapter.TableDescription.TableName}\" ---");
                            Logger.Current.Info("");
                        }


                    }
                    transaction.Commit();
                }


                if (connection != null && connection.State == ConnectionState.Open)
                    connection.Close();

            }
            Logger.Current.Info($"--- End Enumerating Changes for Scope \"{this.ClientScopeInfo.Name}\" ---");
            Logger.Current.Info("");
        }

        /// <summary>
        /// Create a DmRow from a IDataReader
        /// </summary>
        private static DmRow CreateRowFromReader(IDataReader dataReader, DmTable dmTable)
        {
            // we have an insert / update or delete
            DmRow dataRow = dmTable.NewRow();

            for (int i = 0; i < dataReader.FieldCount; i++)
            {
                var columnName = dataReader.GetName(i);
                var value = dataReader.GetValue(i);

                if (!dmTable.Columns.Contains(columnName))
                    Logger.Current.Critical($"{columnName} does not exist ...");

                if (value != DBNull.Value)
                    dataRow[columnName] = value;
            }

            return dataRow;
        }

        /// <summary>
        /// Get a DmRow state
        /// </summary>
        private DmRowState GetStateFromDmRow(DmRow dataRow)
        {
            DmRowState dmRowState = DmRowState.Unchanged;

            if ((bool)dataRow["sync_row_is_tombstone"])
                dmRowState = DmRowState.Deleted;
            else
            {
                var createdTimeStamp = DbManager.ParseTimestamp(dataRow["create_timestamp"]);
                var updatedTimeStamp = DbManager.ParseTimestamp(dataRow["update_timestamp"]);
                var isLocallyCreated = dataRow["create_scope_name"] == DBNull.Value || dataRow["create_scope_name"] == null;
                var islocallyUpdated = dataRow["update_scope_name"] == DBNull.Value || dataRow["update_scope_name"] == null;

                if (islocallyUpdated && updatedTimeStamp > this.ClientScopeInfo.LastTimestamp)
                    dmRowState = DmRowState.Modified;
                else if (isLocallyCreated && createdTimeStamp > this.ClientScopeInfo.LastTimestamp)
                    dmRowState = DmRowState.Added;
                else
                    dmRowState = DmRowState.Unchanged;
            }

            return dmRowState;
        }


        // ------------------------------------------------------------------------------------------
        // Process changes on the server
        // ------------------------------------------------------------------------------------------

        /// <summary>
        /// REPLACE ProcessChangeBatch
        /// Apply changes : Insert / Updates Delete
        /// the fromScope is local client scope when this method is called from server
        /// the fromScope is server scope when this method is called from client
        /// </summary>
        public void ApplyChanges(ScopeInfo fromScope, SyncSetProgress changes)
        {
            ChangeApplicationAction changeApplicationAction;
            DbTransaction applyTransaction = null;

            using (var connection = this.CreateConnection())
            {
                try
                {
                    connection.Open();

                    // Shortcut to rollback
                    Action rollbackAction = () =>
                    {
                        if (applyTransaction != null)
                        {
                            applyTransaction.Rollback();
                            applyTransaction.Dispose();
                            applyTransaction = null;
                        }

                        // Update the syncContext metadatas
                        //this.UpdateRollbackSessionStats(setProgress);

                        if (connection != null && connection.State == ConnectionState.Open)
                            connection.Close();

                    };

                    // Create a transaction
                    applyTransaction = connection.BeginTransaction(IsolationLevel.ReadCommitted);

                    Logger.Current.Info($"----- Applying Changes for Scope \"{fromScope.Name}\" -----");
                    Logger.Current.Info("");

                    // -----------------------------------------------------
                    // 1) Applying deletes
                    // -----------------------------------------------------
                    changeApplicationAction = this.ApplyChangesInternal(connection, applyTransaction, fromScope, changes, DmRowState.Deleted);

                    // Rollback
                    if (changeApplicationAction == ChangeApplicationAction.RollbackTransaction)
                    {
                        rollbackAction();
                        return;
                    }

                    // -----------------------------------------------------
                    // 1) Applying Inserts
                    // -----------------------------------------------------

                    changeApplicationAction = this.ApplyChangesInternal(connection, applyTransaction, fromScope, changes, DmRowState.Added);

                    // Rollback
                    if (changeApplicationAction == ChangeApplicationAction.RollbackTransaction)
                    {
                        rollbackAction();
                        return;
                    }

                    // -----------------------------------------------------
                    // 1) Applying updates
                    // -----------------------------------------------------

                    changeApplicationAction = this.ApplyChangesInternal(connection, applyTransaction, fromScope, changes, DmRowState.Modified);

                    //if (changeApplicationAction == ChangeApplicationAction.Continue && this.ChangesApplied != null)
                    //    this.ChangesApplied?.Invoke(this, this.CreateChangesAppliedEvent(scopeMetadata, applyTransaction));


                    // Rollback
                    if (changeApplicationAction == ChangeApplicationAction.RollbackTransaction)
                    {
                        rollbackAction();
                        return;
                    }


                    Logger.Current.Info($"--- End Applying Changes for Scope \"{fromScope.Name}\" ---");
                    Logger.Current.Info("");

                    applyTransaction.Commit();

                }
                catch (Exception exception)
                {
                    Logger.Current.Info($"Caught exception while applying changes: {exception}");
                    throw;
                }
                finally
                {
                    if (applyTransaction != null)
                    {
                        applyTransaction.Dispose();
                        applyTransaction = null;
                    }

                    if (connection != null && connection.State == ConnectionState.Open)
                    {
                        connection.Close();
                    }

                }
            }
        }

        /// <summary>
        /// Apply changes internal method for one Insert or Update or Delete for every dbSyncAdapter
        /// </summary>
        internal ChangeApplicationAction ApplyChangesInternal(DbConnection connection, DbTransaction transaction, ScopeInfo fromScope, SyncSetProgress changes, DmRowState applyType)
        {
            ChangeApplicationAction changeApplicationAction = ChangeApplicationAction.Continue;

            // for each adapters (Zero to End for Insert / Updates -- End to Zero for Deletes
            for (int i = 0; i < this.Configuration.ScopeSet.Tables.Count; i++)
            {
                try
                {

                    // If we have a delete we must go from Up to Down, orthewise Dow to Up index
                    var tableDescription = (applyType != DmRowState.Deleted ?
                            this.Configuration.ScopeSet.Tables[i] :
                            this.Configuration.ScopeSet.Tables[this.Configuration.ScopeSet.Tables.Count - i - 1]);

                    var builder = this.GetDatabaseBuilder(tableDescription);
                    var syncAdapter = builder.CreateSyncAdapter(connection, transaction);
                    syncAdapter.TableDescription = tableDescription;
                    syncAdapter.ObjectNames = builder.ObjectNames;

                    // Set syncAdapter properties
                    syncAdapter.applyType = applyType;

                    if (syncAdapter.ConflictActionInvoker == null)
                        syncAdapter.ConflictActionInvoker = GetConflictAction;

                    Logger.Current.Info($"----- Operation {applyType.ToString()} for Table \"{syncAdapter.TableDescription.TableName}\" -----");

                    // getting the tableProgress to be applied
                    var tableProgress = changes.FindTableProgress(syncAdapter.TableDescription.TableName);

                    if (tableProgress == null)
                        continue;

                    var dmTable = tableProgress.Changes;

                    if (dmTable == null)
                        continue;

                    // check and filter
                    var dmChangesView = new DmView(dmTable, (r) => r.RowState == applyType);

                    if (dmChangesView.Count == 0)
                    {
                        Logger.Current.Info($"0 {applyType.ToString()} Applied");
                        Logger.Current.Info($"--- End {applyType.ToString()} for Table \"{syncAdapter.TableDescription.TableName}\" ---");
                        Logger.Current.Info($"");
                        continue;
                    }


                    // Conflicts occured when trying to apply rows
                    List<SyncConflict> conflicts = new List<SyncConflict>();

                    DmView rowsApplied;
                    // applying the bulkchanges command
                    if (this.Configuration.UseBulkOperations)
                        rowsApplied = syncAdapter.ApplyBulkChanges(dmChangesView, fromScope, conflicts);
                    else
                        rowsApplied = syncAdapter.ApplyChanges(dmChangesView, fromScope, conflicts);

                    // If conflicts occured
                    // Eventuall, conflicts are resolved on server side.
                    if (conflicts != null)
                    {
                        foreach (var conflict in conflicts)
                        {
                            DmRow resolvedRow;
                            var scopeBuilder = this.GetScopeBuilder();
                            var scopeInfoBuilder = scopeBuilder.CreateScopeInfoBuilder(connection, transaction);
                            var localTimeStamp = scopeInfoBuilder.GetLocalTimestamp();

                            changeApplicationAction = syncAdapter.HandleConflict(conflict, fromScope, localTimeStamp, out resolvedRow);

                            // row resolved
                            if (resolvedRow != null)
                                rowsApplied.Add(resolvedRow);

                            if (changeApplicationAction != ChangeApplicationAction.RollbackTransaction)
                                continue;
                        }
                    }

                    // Handle sync progress for this syncadapter (so this table)
                    var appliedChanges = new ScopeAppliedChanges();
                    appliedChanges.View = rowsApplied;
                    appliedChanges.State = applyType;

                    this.progressEventArgs.AppliedChanges.Add(appliedChanges);


                    Logger.Current.Info("");
                    //Logger.Current.Info($"{this._changeHandler.ApplyCount} {operation} Applied");
                    Logger.Current.Info($"--- End {applyType.ToString()} for Table \"{syncAdapter.TableDescription.TableName}\" ---");
                    Logger.Current.Info("");

                }
                catch (Exception ex)
                {
                    Logger.Current.Error($"Error during ApplyInternalChanges : {ex.Message}");
                    throw;
                }
            }

            switch (applyType)
            {
                case DmRowState.Added:
                    this.progressEventArgs.Stage = SyncStage.ApplyingInserts;
                    break;
                case DmRowState.Modified:
                    this.progressEventArgs.Stage = SyncStage.ApplyingUpdates;
                    break;
                case DmRowState.Deleted:
                    this.progressEventArgs.Stage = SyncStage.ApplyingDeletes;
                    break;
            }

            this.SyncProgress?.Invoke(this, this.progressEventArgs);

            // Check action
            changeApplicationAction = this.progressEventArgs.Action;

            if (changeApplicationAction == ChangeApplicationAction.RollbackTransaction)
                return ChangeApplicationAction.RollbackTransaction;


            return ChangeApplicationAction.Continue;
        }

        //internal void UpdateRollbackSessionStats(SyncSetProgress setProgress)
        //{
        //    foreach (SyncTableProgress tablesProgress in setProgress.TablesProgress)
        //    {
        //        tablesProgress.ChangesFailed = tablesProgress.ChangesApplied;
        //        tablesProgress.ChangesApplied = 0;
        //    }
        //}

        /// <summary>
        /// Add metadata columns
        /// </summary>
        /// <param name="table"></param>
        private void AddTrackingColumns<T>(DmTable table, string name)
        {
            if (!table.Columns.Contains(name))
            {
                var dc = new DmColumn<T>(name) { DefaultValue = default(T) };
                table.Columns.Add(dc);
            }
        }

        private void RemoveTrackingColumns(DmTable changes, string name)
        {
            if (changes.Columns.Contains(name))
                changes.Columns.Remove(name);
        }

        /// <summary>
        /// Adding sync columns to the changes datatable
        /// </summary>
        private void AddTimestampValue(DmTable table, long tickCount)
        {
            // For each datarow, set the create peerkey or update peer key based on the rowstate.
            // The SyncCreatePeerKey and SyncUpdatePeerKey values are 0 which means the client replica sent these changes.
            foreach (DmRow row in table.Rows)
            {
                switch (row.RowState)
                {
                    case DmRowState.Added:
                        // for rows that have been added we need to
                        // update both the create and update versions to be the same.
                        // for ex, if a row was deleted and added again, the server update version will otherwise have a higher value
                        // since the sent update version will be set to 0. This results in the DbChangeHandler.ApplyInsert returning LocalSupersedes
                        // internally after it compares the versions.
                        row["create_timestamp"] = tickCount;
                        row["update_timestamp"] = tickCount;
                        break;
                    case DmRowState.Modified:
                        // Only update the update version for modified rows.
                        row["update_timestamp"] = tickCount;
                        break;
                    case DmRowState.Deleted:
                        row.RejectChanges();
                        row["update_timestamp"] = tickCount;
                        row.AcceptChanges();
                        row.Delete();
                        break;
                }
            }
        }


        /// <summary>
        /// A conflict has occured, we try to ask for the solution to the user
        /// </summary>
        internal ApplyAction GetConflictAction(SyncConflict conflict, DbConnection connection, DbTransaction transaction = null)
        {
            Logger.Current.Debug("Raising Apply Change Failed Event");
            var dbApplyChangeFailedEventArg = new ApplyChangeFailedEventArgs(conflict, connection, transaction);

            // this.ApplyChangedFailed?.Invoke(this, dbApplyChangeFailedEventArg);

            ApplyAction action = dbApplyChangeFailedEventArg.Action;
            Logger.Current.Debug($"Action: {action.ToString()}");
            return action;
        }

    }
}
