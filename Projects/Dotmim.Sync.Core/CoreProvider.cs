using DmBinaryFormatter;
using Dotmim.Sync.Core.Batch;
using Dotmim.Sync.Core.Builders;
using Dotmim.Sync.Core.Common;
using Dotmim.Sync.Core.Context;
using Dotmim.Sync.Core.Enumerations;
using Dotmim.Sync.Core.Log;
using Dotmim.Sync.Core.Manager;
using Dotmim.Sync.Core.Scope;
using Dotmim.Sync.Data;
using Dotmim.Sync.Data.Surrogate;
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
    public abstract class CoreProvider : IResponseHandler
    {
        bool _syncInProgress;

        string providerType;
         //event args used for get back info to the user
        ScopeProgressEventArgs progressEventArgs;
   
        public ScopeInfo ClientScopeInfo { get; set; }
        public ScopeInfo ServerScopeInfo { get; set; }


        /// <summary>
        /// Raise an event if the sync is outdated. 
        /// Let the user choose if he wants to force or not
        /// </summary>
        public event EventHandler<OutdatedEventArgs> SyncOutdated = null;

        /// <summary>
        /// Occurs during progress
        /// </summary>
        public event EventHandler<ScopeProgressEventArgs> SyncProgress = null;

        /// <summary>
        /// Occurs when a conflict is raised.
        /// </summary>
        public event EventHandler<ApplyChangeFailedEventArgs> ApplyChangedFailed = null;

        /// <summary>
        /// Create a new instance of the implemented Connection provider
        /// </summary>
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
                providerType = $"{type.Name}, {type.ToString()}";

                return providerType;
            }
        }


        /// <summary>
        /// Gets or Sets the Apply action to use when a conflict occurs
        /// </summary>
        public ApplyAction ConflictApplyAction { get; internal set; }

        /// <summary>
        /// Called by the  to indicate that a 
        /// synchronization session has started.
        /// </summary>
        public virtual void BeginSession()
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
                //this.setProgress = new SyncSetProgress();

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
        public virtual void ApplyConfiguration(ServiceConfiguration configuration = null)
        {
            if (configuration == null && this.Configuration == null)
                throw new ArgumentException("Configuration must be set, if the local provider didn't configure itself");

            // if it's null, probably on the local provider
            if (this.Configuration == null)
                this.Configuration = configuration;

            // Directory folder
            if (!String.IsNullOrEmpty(this.Configuration.BatchDirectory) && !Directory.Exists(this.Configuration.BatchDirectory))
                Directory.CreateDirectory(this.Configuration.BatchDirectory);

            // ApplyAction for policy
            this.ConflictApplyAction = this.Configuration.ConflictResolutionPolicy == ConflictResolutionPolicy.ServerWins ? 
                ApplyAction.Continue : 
                ApplyAction.RetryWithForceWrite;

            // Already configured
            if (this.Configuration.ScopeSet != null && this.Configuration.ScopeSet.Tables.Count > 0)
                return;

            // Open the connection
            using (var connection = this.CreateConnection())
            {
                try
                {
                    connection.Open();

                    using (var transaction = connection.BeginTransaction(IsolationLevel.ReadUncommitted))
                    {
                        foreach (var table in this.Configuration.Tables)
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


            this.progressEventArgs.Stage = SyncStage.BuildConfiguration;
            this.progressEventArgs.Action = ChangeApplicationAction.Continue;
            this.progressEventArgs.Configuration = this.Configuration;
            this.SyncProgress?.Invoke(this, this.progressEventArgs);
        }

        public virtual ServiceConfiguration GetConfiguration()
        {
            return this.Configuration;
        }


        private DmTable BuildChangesTable(string tableName)
        {
            var dmTable = this.Configuration.ScopeSet.Tables[tableName].Clone();

            // Adding the tracking columns
            AddTrackingColumns<string>(dmTable, "create_scope_name");
            AddTrackingColumns<long>(dmTable, "create_timestamp");
            AddTrackingColumns<string>(dmTable, "update_scope_name");
            AddTrackingColumns<long>(dmTable, "update_timestamp");

            // Since we can have some deleted rows, the Changes table should have only null columns (except PrimaryKeys)

            foreach (var c in dmTable.Columns)
            {
                var isPrimaryKey = dmTable.PrimaryKey.Columns.Any(cc => dmTable.IsEqual(cc.ColumnName, c.ColumnName));

                if (!isPrimaryKey)
                    c.AllowDBNull = true;
            }

            return dmTable;

        }
     

        /// <summary>
        /// Called when the sync is over
        /// </summary>
        public virtual void EndSession()
        {
            try
            {
                Logger.Current.Info($"EndSession() called on Provider {this.ProviderTypeName}");
                //this.setProgress.TablesProgress.Clear();
                //this.setProgress.Dispose();
                //this.setProgress = null;

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

        public virtual (ScopeInfo serverScope, ScopeInfo clientScope) EnsureScopes(string serverScopeName, string clientScopeName = null)
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
                        if (scopeInfo == null)
                        {
                            this.ServerScopeInfo = scopeInfoBuilder.InsertOrUpdateScopeInfo(serverScopeName);
                            this.ServerScopeInfo.IsNewScope = true;
                        }
                        else
                        {
                            this.ServerScopeInfo = scopeInfo;
                        }

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
                            if (scopeInfoLocal == null)
                            {
                                this.ClientScopeInfo = scopeInfoBuilder.InsertOrUpdateScopeInfo(clientScopeName);
                                this.ClientScopeInfo.IsNewScope = true;
                            }
                            else
                            {
                                this.ClientScopeInfo = scopeInfoLocal;
                            }
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
            return (this.ServerScopeInfo, this.ClientScopeInfo);
        }

        /// <summary>
        /// Write scope in the provider datasource
        /// </summary>
        public virtual void WriteScopes()
        {
            // Open the connection
            using (var connection = this.CreateConnection())
            {
                try
                {
                    connection.Open();
                    var scopeBuilder = this.GetScopeBuilder();
                    var scopeInfoBuilder = scopeBuilder.CreateScopeInfoBuilder(connection);
                    ClientScopeInfo = scopeInfoBuilder.InsertOrUpdateScopeInfo(ClientScopeInfo.Name, true);
                    ServerScopeInfo = scopeInfoBuilder.InsertOrUpdateScopeInfo(ServerScopeInfo.Name, true);

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
        public virtual long GetLocalTimestamp()
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
        public virtual void EnsureDatabase(DbBuilderOption options)
        {
            if (Configuration == null || Configuration.ScopeSet == null || Configuration.ScopeSet.Tables.Count == 0)
                throw new ArgumentNullException("Configuration or Configuration.ScopeSet");

            // Check if database is already created
            if (this.ClientScopeInfo.IsDatabaseCreated)
                return;

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
        internal virtual bool IsRemoteOutdated()
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
        public virtual BatchInfo GetChangeBatch()
        {
            if (this.ClientScopeInfo == null)
                throw new ArgumentException("ClientScope is null");

            // check batchSize if not > then Configuration.DownloadBatchSizeInKB
            if (this.Configuration.DownloadBatchSizeInKB > 0)
                Logger.Current.Info($"Enumeration data cache size selected: {this.Configuration.DownloadBatchSizeInKB} Kb");

            this.progressEventArgs.Stage = SyncStage.SelectedChanges;

            //this.BuildTableProgress();

            var batchInfo = this.GetChanges();

            // Check if the remote is not outdated
            var isOutdated = this.IsRemoteOutdated();

            if (isOutdated)
                throw new Exception("OutDatedPeer");


            return batchInfo;
        }

        internal BatchInfo GetChanges()
        {
            BatchInfo batchInfo = null;
            try
            {
                Logger.Current.Info("GetChanges called: _syncBatchProducer is null");

                // Check if the remote is not outdated
                var isOutdated = this.IsRemoteOutdated();

                // Get a chance to make the sync even if it's outdated
                if (isOutdated && this.SyncOutdated != null)
                {
                    Logger.Current.Info("Raising Sync Remote Outdated Event");
                    var outdatedEventArgs = new OutdatedEventArgs();
                    this.SyncOutdated(this, outdatedEventArgs);
                    Logger.Current.Info($"Action taken : {outdatedEventArgs.Action.ToString()}");

                    if (outdatedEventArgs.Action == OutdatedSyncAction.PartialSync)
                    {
                        Logger.Current.Info("Attempting Partial Sync");
                    }
                }

                // the sync is still outdated, abort it
                if (isOutdated)
                {
                    Logger.Current.Info("Aborting Sync");
                    return null;
                }

                if (this.Configuration.DownloadBatchSizeInKB == 0)
                    batchInfo = this.EnumerateChangesInternal();
                else
                    batchInfo = this.EnumerateChangesInBatchesInternal();

                Logger.Current.Info("Committing transaction");

                return batchInfo;

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
        internal BatchInfo EnumerateChangesInternal()
        {
            Logger.Current.Info($"----- Enumerating Changes for Scope \"{this.ClientScopeInfo.Name}\" -----");
            Logger.Current.Info("");
            Logger.Current.Info("");

            // create the in memory changes set
            DmSet changesSet = new DmSet(this.Configuration.ScopeSet.DmSetName);

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
                            syncAdapter.ConflictApplyAction = this.ConflictApplyAction;

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

                            // Get a clone of the table with tracking columns
                            var dmTableChanges = BuildChangesTable(syncAdapter.TableDescription.TableName);

                            // Set the parameters
                            DbManager.SetParameterValue(selectIncrementalChangesCommand, "sync_min_timestamp", this.ClientScopeInfo.LastTimestamp);
                            DbManager.SetParameterValue(selectIncrementalChangesCommand, "sync_scope_name", this.ClientScopeInfo.Name);
                            DbManager.SetParameterValue(selectIncrementalChangesCommand, "sync_scope_is_new", this.ClientScopeInfo.IsNewScope ? 1 : 0);


                            this.AddTrackingColumns<int>(dmTableChanges, "sync_row_is_tombstone");

                            // Get the reader
                            using (var dataReader = selectIncrementalChangesCommand.ExecuteReader())
                            {

                                while (dataReader.Read())
                                {
                                    DmRow dataRow = CreateRowFromReader(dataReader, dmTableChanges);

                                    // assuming the row is not inserted / modified
                                    DmRowState state = DmRowState.Unchanged;

                                    // get if the current row is inserted, modified, deleted
                                    state = GetStateFromDmRow(dataRow);

                                    if (state != DmRowState.Deleted && state != DmRowState.Modified && state != DmRowState.Added)
                                        continue;

                                    // add row
                                    dmTableChanges.Rows.Add(dataRow);

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
                                this.RemoveTrackingColumns(dmTableChanges, "sync_row_is_tombstone");

                                // add it to the DmSet
                                changesSet.Tables.Add(dmTableChanges);

                                // Create a progress event
                                var changes = new ScopeSelectedChanges();
                                changes.View = new DmView(dmTableChanges);
                                this.progressEventArgs.SelectedChanges.Add(changes);
                            }

                            Logger.Current.Info($"--- End Table \"{syncAdapter.TableDescription.TableName}\" ---");
                            Logger.Current.Info("");
                        }

                        this.progressEventArgs.Stage = SyncStage.SelectedChanges;
                        this.SyncProgress?.Invoke(this, this.progressEventArgs);

                        transaction.Commit();

                        var batchInfoPart = GenerateBatchInfo(0, changesSet, true);
                        var batchInfo = new BatchInfo();
                        if (batchInfoPart != null)
                            batchInfo.BatchPartsInfo.Add(batchInfoPart);

                        // Create a new in-memory batch info with an the changes DmSet
                        return batchInfo;

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
        internal BatchInfo EnumerateChangesInBatchesInternal()
        {
            Logger.Current.Info($"----- Enumerating Changes for Scope \"{this.ClientScopeInfo.Name}\" -----");
            Logger.Current.Info("");
            Logger.Current.Info("");

            // memory size total
            double memorySizeFromDmRows = 0L;

            int batchIndex = 0;

            DmSerializer serializer = new DmSerializer();
            BatchInfo batchInfo = new BatchInfo();

            using (var connection = this.CreateConnection())
            {
                // Open the connection
                connection.Open();

                using (var transaction = connection.BeginTransaction(IsolationLevel.ReadUncommitted))
                {
                    // create the in memory changes set
                    DmSet changesSet = new DmSet(this.Configuration.ScopeSet.DmSetName);

                    foreach (var tableDescription in this.Configuration.ScopeSet.Tables)
                    {
                        var builder = this.GetDatabaseBuilder(tableDescription);
                        var syncAdapter = builder.CreateSyncAdapter(connection, transaction);
                        syncAdapter.TableDescription = tableDescription;
                        syncAdapter.ObjectNames = builder.ObjectNames;
                        syncAdapter.ConflictApplyAction = this.ConflictApplyAction;

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

                        var dmTable = BuildChangesTable(syncAdapter.TableDescription.TableName);

                        try
                        {

                            // Set the parameters
                            DbManager.SetParameterValue(selectIncrementalChangesCommand, "sync_min_timestamp", this.ClientScopeInfo.LastTimestamp);
                            DbManager.SetParameterValue(selectIncrementalChangesCommand, "sync_scope_name", this.ClientScopeInfo.Name);
                            DbManager.SetParameterValue(selectIncrementalChangesCommand, "sync_scope_is_new", (this.ClientScopeInfo.IsNewScope ? 1 : 0));

                            this.AddTrackingColumns<int>(dmTable, "sync_row_is_tombstone");

                            // Get the reader
                            using (var dataReader = selectIncrementalChangesCommand.ExecuteReader())
                            {

                                while (dataReader.Read())
                                {
                                    DmRow dmRow = CreateRowFromReader(dataReader, dmTable);

                                    DmRowState state = DmRowState.Unchanged;

                                    state = GetStateFromDmRow(dmRow);

                                    // If the row is not deleted inserted or modified, go next
                                    if (state != DmRowState.Deleted && state != DmRowState.Modified && state != DmRowState.Added)
                                        continue;

                                    var fieldsSize = DmTableSurrogate.GetRowSizeFromDataRow(dmRow);
                                    var dmRowSize = fieldsSize / 1024d;

                                    if (dmRowSize > this.Configuration.DownloadBatchSizeInKB)
                                    {
                                        var exc = $"Row is too big ({dmRowSize} kb.) for the current Configuration.DownloadBatchSizeInKB ({Configuration.DownloadBatchSizeInKB} kb.) Aborting Sync...";
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
                                    if (memorySizeFromDmRows > Configuration.DownloadBatchSizeInKB)
                                    {
                                        // Since we dont need this column anymore, remove it
                                        this.RemoveTrackingColumns(dmTable, "sync_row_is_tombstone");

                                        changesSet.Tables.Add(dmTable);

                                        var bpi = GenerateBatchInfo(batchIndex, changesSet, false);
                                        if (bpi != null)
                                        {
                                            batchInfo.BatchPartsInfo.Add(bpi);
                                            batchIndex++;
                                        }

                                        // Recreate an empty DmSet, then a dmTable clone
                                        changesSet = new DmSet(this.Configuration.ScopeSet.DmSetName);
                                        dmTable = dmTable.Clone();
                                        this.AddTrackingColumns<int>(dmTable, "sync_row_is_tombstone");

                                        // Init the row memory size
                                        memorySizeFromDmRows = 0L;
                                    }
                                }


                                // Since we dont need this column anymore, remove it
                                this.RemoveTrackingColumns(dmTable, "sync_row_is_tombstone");

                                // Create a progress event
                                var selectedChanges = new ScopeSelectedChanges();
                                selectedChanges.View = new DmView(dmTable);
                                this.progressEventArgs.SelectedChanges.Add(selectedChanges);
                                this.progressEventArgs.Stage = SyncStage.SelectedChanges;
                                this.SyncProgress?.Invoke(this, this.progressEventArgs);


                                //this.SyncSelectedChanges?.Invoke(this, selectedChanges);
                                changesSet.Tables.Add(dmTable);

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

                    // We are in batch mode, and we are at the last batchpart info
                    var batchPartInfo = GenerateBatchInfo(batchIndex, changesSet, false);
                    if (batchPartInfo != null)
                    {
                        batchPartInfo.IsLastBatch = true;
                        batchInfo.BatchPartsInfo.Add(batchPartInfo);
                    }

                    transaction.Commit();
                }


                if (connection != null && connection.State == ConnectionState.Open)
                    connection.Close();

            }
            Logger.Current.Info($"--- End Enumerating Changes for Scope \"{this.ClientScopeInfo.Name}\" ---");
            Logger.Current.Info("");

            return batchInfo;
        }

        /// <summary>
        /// Generate a batch file, add it as batch part info in a batch info
        /// </summary>
        private BatchPartInfo GenerateBatchInfo(int batchIndex, DmSet changesSet, bool inMemory)
        {
            var hasData = true;

            if (changesSet == null || changesSet.Tables.Count == 0)
                hasData = false;
            else
                hasData = changesSet.Tables.Any(t => t.Rows.Count > 0);

            if (!hasData)
                return null;

            BatchPartInfo bpi = null;
            // Create a batch part
            // The batch part creation process will serialize the changesSet to the disk
            if (!inMemory)
            {
                var bpId = $"{batchIndex}_{Path.GetRandomFileName().Replace(".", "_")}.batch";
                var fileName = Path.Combine(this.Configuration.BatchDirectory, bpId);
                BatchPart.Serialize(changesSet, fileName);
                bpi = new BatchPartInfo(fileName);
                bpi.InMemory = false;
                bpi.IsLastBatch = false;
            }
            else
            {
                bpi = new BatchPartInfo(changesSet);
                bpi.InMemory = true;
                bpi.IsLastBatch = true;
            }

            // Generate the Batch part info
            bpi.Index = batchIndex;
            bpi.Tables = changesSet.Tables.Select(t => t.TableName).ToArray();

            return bpi;
        }

        /// <summary>
        /// Create a DmRow from a IDataReader
        /// </summary>
        private DmRow CreateRowFromReader(IDataReader dataReader, DmTable dmTable)
        {
            object object_create_timestamp = dataReader["create_timestamp"];
            object object_update_timestamp = dataReader["update_timestamp"];
            object object_sync_row_is_tombstone = dataReader["sync_row_is_tombstone"];
            object object_create_scope_name = dataReader["create_scope_name"];
            object object_update_scope_name = dataReader["update_scope_name"];

            //var state = GetStateFromDmRow(object_create_timestamp, object_update_timestamp,
            //    object_sync_row_is_tombstone, object_create_scope_name, object_update_scope_name);

            // we have an insert / update or delete
            DmRow dataRow = dmTable.NewRow();

            for (int i = 0; i < dataReader.FieldCount; i++)
            {
                var columnName = dataReader.GetName(i);
                var value = dataReader.GetValue(i);

                //if (!dmTable.Columns.Contains(columnName))
                //    Logger.Current.Critical($"{columnName} does not exist ...");

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

                if (!this.ClientScopeInfo.IsNewScope && islocallyUpdated && updatedTimeStamp > this.ClientScopeInfo.LastTimestamp)
                    dmRowState = DmRowState.Modified;
                else if (this.ClientScopeInfo.IsNewScope || (isLocallyCreated && createdTimeStamp > this.ClientScopeInfo.LastTimestamp))
                    dmRowState = DmRowState.Added;
                else
                    dmRowState = DmRowState.Unchanged;
            }

            return dmRowState;
        }

        /// <summary>
        /// Get a DmRow state
        /// </summary>
        private DmRowState GetStateFromDmRow(
            object object_create_timestamp,
            object object_update_timestamp,
            object object_sync_row_is_tombstone,
            object object_create_scope_name,
            object object_update_scope_name)
        {
            DmRowState dmRowState = DmRowState.Unchanged;

            if ((bool)object_sync_row_is_tombstone)
                dmRowState = DmRowState.Deleted;
            else
            {
                var createdTimeStamp = DbManager.ParseTimestamp(object_create_timestamp);
                var updatedTimeStamp = DbManager.ParseTimestamp(object_update_timestamp);
                var isLocallyCreated = object_create_scope_name == DBNull.Value || object_create_scope_name == null;
                var islocallyUpdated = object_update_scope_name == DBNull.Value || object_update_scope_name == null;

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
        public virtual void ApplyChanges(ScopeInfo fromScope, BatchInfo changes)
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
        internal ChangeApplicationAction ApplyChangesInternal(DbConnection connection, DbTransaction transaction, ScopeInfo fromScope, BatchInfo changes, DmRowState applyType)
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
                    syncAdapter.ConflictApplyAction = this.ConflictApplyAction;

                    // Set syncAdapter properties
                    syncAdapter.applyType = applyType;

                    if (syncAdapter.ConflictActionInvoker == null && this.ApplyChangedFailed != null)
                        syncAdapter.ConflictActionInvoker = GetConflictAction;

                    Logger.Current.Info($"----- Operation {applyType.ToString()} for Table \"{syncAdapter.TableDescription.TableName}\" -----");

                    if (changes.BatchPartsInfo != null && changes.BatchPartsInfo.Count > 0)
                    {
                        // getting the table to be applied
                        // we may have multiple batch files, so we iterate
                        foreach (var dmTable in changes.GetTable(syncAdapter.TableDescription.TableName))
                        {
                            if (dmTable == null || dmTable.Rows.Count == 0)
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

                            int rowsApplied;
                            // applying the bulkchanges command
                            if (this.Configuration.UseBulkOperations)
                                rowsApplied = syncAdapter.ApplyBulkChanges(dmChangesView, fromScope, conflicts);
                            else
                                rowsApplied = syncAdapter.ApplyChanges(dmChangesView, fromScope, conflicts);

                            // If conflicts occured
                            // Eventuall, conflicts are resolved on server side.
                            if (conflicts != null && conflicts.Count > 0)
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
                                        rowsApplied++;

                                    if (changeApplicationAction != ChangeApplicationAction.RollbackTransaction)
                                        continue;
                                }
                            }

                            // Handle sync progress for this syncadapter (so this table)
                            var appliedChanges = new ScopeAppliedChanges();
                            appliedChanges.TableName = syncAdapter.TableDescription.TableName;
                            appliedChanges.ChangesApplied = rowsApplied;
                            appliedChanges.ChangesFailed = dmChangesView.Count - rowsApplied;
                            appliedChanges.State = applyType;

                            this.progressEventArgs.AppliedChanges.Add(appliedChanges);
                        }
                    }

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
            var dbApplyChangeFailedEventArg = new ApplyChangeFailedEventArgs(conflict, this.ConflictApplyAction, connection, transaction);

            this.ApplyChangedFailed?.Invoke(this, dbApplyChangeFailedEventArg);

            ApplyAction action = dbApplyChangeFailedEventArg.Action;
            Logger.Current.Debug($"Action: {action.ToString()}");
            return action;
        }

    }
}
