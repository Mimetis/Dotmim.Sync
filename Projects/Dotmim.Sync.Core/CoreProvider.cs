using Dotmim.Sync.Core.Adapter;
using Dotmim.Sync.Core.Batch;
using Dotmim.Sync.Core.Common;
using Dotmim.Sync.Core.Context;
using Dotmim.Sync.Core.Enumerations;
using Dotmim.Sync.Core.Log;
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
        bool _isNewRemote;
        string providerType;
        SyncSetProgress _setProgress;
        ScopeProgressEventArgs _progressEventArgs;
        internal object lockObject = new object();
        //SyncBatchProducer _syncBatchProducer;

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

        /// <summary>
        /// Occurs every time we have a table selected for sync
        /// </summary>
        public event EventHandler<ScopeSelectedChanges> SyncSelectedChanges = null;

        /// <summary>
        /// Occurs every time we have a table selected for sync
        /// </summary>
        public event EventHandler<ScopeAppliedChanges> SyncAppliedChanges = null;

        /// <summary>
        /// Occurs during progress
        /// </summary>
        public event EventHandler<ApplyChangeFailedEventArgs> ApplyChangedFailed = null;

        /// <summary>
        /// Get the database connection
        /// </summary>
        /// <returns></returns>
        public abstract DbConnection Connection { get; }

        /// <summary>
        /// Create a typed sync adapter
        /// </summary>
        /// <returns></returns>
        public abstract SyncAdapter CreateSyncAdapter();

        /// <summary>
        /// Get the serializer
        /// </summary>
        public abstract SyncBatchSerializer CreateSerializer();

        /// <summary>
        /// List of Adapter, for each synchronized table
        /// </summary>
        public List<SyncAdapter> Adapters { get; set; }

        /// <summary>
        /// Scope Config
        /// </summary>
        public ScopeConfigData ScopeConfigData { get; set; }

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
        /// Read a scope info
        /// </summary>
        /// <returns></returns>
        public ScopeInfo ReadScopeInfo(string scopeName = null)
        {
            ScopeInfo scopeInfo;

            using (var scopeFactory = new ScopeFactory(this.Connection))
                scopeInfo = string.IsNullOrEmpty(scopeName) ? scopeFactory.ReadFirstScopeInfo() : scopeFactory.ReadScopeInfo(scopeName);

            if (scopeInfo == null)
                return null;

            this._progressEventArgs.Stage = SyncStage.ReadingMetadata;
            this._progressEventArgs.ScopeInfo = scopeInfo;
            this.SyncProgress?.Invoke(this, this._progressEventArgs);

            return scopeInfo;
        }



        /// <summary>
        /// Read a scope configuration (serialized as xml in provider db)
        /// </summary>
        public void ReadScopeConfig(ScopeInfo scope)
        {
            if (scope == null || string.IsNullOrEmpty(scope.Name))
                throw new Exception("Can't read a scope config if scopename is not defined");

            ScopeConfigData scd = null;
            // Read the scope config, identified by the Server scope name
            using (var scopeFactory = new ScopeFactory(this.Connection))
            {
                // Read the configuration
                var scopeConfig = scopeFactory.ReadScopeConfig(scope.ConfigId);

                using (StringReader stringReader = new StringReader(scopeConfig.ConfigData))
                {
                    var xmlSerializer = new XmlSerializer(typeof(ScopeConfigData));
                    scd = (ScopeConfigData)xmlSerializer.Deserialize(stringReader);
                    scd.ScopeConfigId = scopeConfig.ConfigId;
                }
            }

            this._progressEventArgs.Stage = SyncStage.ReadingSchema;
            this._progressEventArgs.Schema = scd;
            this.SyncProgress?.Invoke(this, this._progressEventArgs);

            this.ScopeConfigData = scd;

            this.BuildAdapters(scope.Name);
        }

        public void WriteScopeConfig(ScopeInfo scope)
        {
            if (scope == null || string.IsNullOrEmpty(scope.Name))
                throw new Exception("Can't write a scope config if scopename is not defined");

            // Create a new scope for this new remote client
            using (var scopeFactory = new ScopeFactory(this.Connection))
            {
                StringBuilder scopeConfigBuilder = new StringBuilder();
                using (StringWriter sw = new StringWriter(scopeConfigBuilder))
                {
                    var xmlSerializer = new XmlSerializer(typeof(ScopeConfigData));
                    xmlSerializer.Serialize(sw, this.ScopeConfigData);
                }

                var scopeConfig = new ScopeConfig();
                scopeConfig.ConfigId = scope.ConfigId;
                scopeConfig.ConfigData = scopeConfigBuilder.ToString();
                scopeConfig.ConfigStatus = "C";

                if (!scopeFactory.IsScopeConfigProvisionned(scope.Name))
                    scopeFactory.InsertScopeConfig(scopeConfig);
                else
                    scopeFactory.UpdateScopeConfig(scopeConfig);
            }
        }

        public ScopeInfo WriteScopeInfo(string scopeName)
        {
            ScopeInfo scopeInfo;

            // Create a new scope for this new remote client
            using (var scopeFactory = new ScopeFactory(this.Connection))
            {
                if (scopeFactory.IsScopeExist(scopeName))
                    scopeInfo = scopeFactory.UpdateScopeInfo(scopeName);
                else
                    scopeInfo = scopeFactory.InsertScopeInfo(scopeName, this.ScopeConfigData.ScopeConfigId);
            }

            this._progressEventArgs.Stage = SyncStage.WritingMetadata;
            this._progressEventArgs.ScopeInfo = scopeInfo;
            this.SyncProgress?.Invoke(this, this._progressEventArgs);

            return scopeInfo;
        }

        public long ReadLocalTimestamp()
        {
            long tickCount = 0;

            // Get the active min rowversion for current db
            using (var scopeFactory = new ScopeFactory(this.Connection))
            {
                tickCount = scopeFactory.GetLocalTimestamp();
            }

            return tickCount;
        }

        /// <summary>
        /// Build adapters, based on the config data object
        /// </summary>
        void BuildAdapters(string scopeName)
        {
            // create all factories
            if (this.Adapters == null)
                this.Adapters = new List<SyncAdapter>();
            else
                this.Adapters.Clear();

            try
            {
                // Open connection and build adapters
                this.Connection.Open();


                // Create all adapters
                foreach (var adapterConfiguration in this.ScopeConfigData.AdapterConfigurations)
                {
                    var adapter = this.CreateSyncAdapter();
                    adapter.BuildAdapter(this.Connection, adapterConfiguration);
                    this.Adapters.Add(adapter);

                    var dmTable = adapter.BuildDataTable();

                    // Adding the tracking columns
                    AddTrackingColumns<long>(dmTable, "create_timestamp");
                    AddTrackingColumns<long>(dmTable, "update_timestamp");

                    this._setProgress.AddTableProgress(dmTable);
                }

                if (this.Adapters.Count <= 0)
                    throw new Exception("No adapters found !");

                // Check if every DbSyncAdapter have a unique row id column
                foreach (SyncAdapter adapter in this.Adapters)
                {
                    if (adapter.RowIdColumns.Count != 0)
                        continue;

                    throw new Exception($"At least, an adapter doesn't have any unique column as identifier : {adapter.TableName}");
                }
            }
            catch (Exception ex)
            {
                Logger.Current.Error($"Error during building adapters : {ex.Message}");
                throw;
            }
            finally
            {
                if (this.Connection.State != ConnectionState.Closed)
                    this.Connection.Close();

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

                // init progress
                this._setProgress = new SyncSetProgress();
                this._progressEventArgs = new ScopeProgressEventArgs();
                this._progressEventArgs.ProviderTypeName = this.ProviderTypeName;

                Logger.Current.Info($"BeginSession() called on Provider {this.ProviderTypeName}");
            }
            catch (Exception)
            {
                this._syncInProgress = false;
                throw;
            }


        }

        public void EndSession()
        {
            try
            {
                Logger.Current.Info($"EndSession() called on Provider {this.ProviderTypeName}");

                this._setProgress.Dispose();
                this._setProgress = null;

                this._progressEventArgs.Cleanup();
                this._progressEventArgs = null;
            }
            finally
            {
                lock (this)
                {
                    this._syncInProgress = false;
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
        public SyncSetProgress GetChangeBatch(ScopeInfo scope)
        {
            // check batchSize if not > then MemoryDataCacheSize
            if (this.MemoryDataCacheSize > 0)
            {
                Logger.Current.Info($"Enumeration data cache size selected: {MemoryDataCacheSize} Kb");
            }
            this._progressEventArgs.Stage = SyncStage.SelectedChanges;

            this.GetChanges(scope);

            if (this._setProgress.IsOutdated)
                throw new Exception("OutDatedPeer");


            return this._setProgress;
        }

        internal void GetChanges(ScopeInfo scope)
        {
            var serializer = this.CreateSerializer();

            this._setProgress.serializer = serializer;

            try
            {
                Logger.Current.Info("GetChanges called: _syncBatchProducer is null");

                // Check if the remote is not outdated
                this._setProgress.IsOutdated = this.IsRemoteOutdated();

                // Get a chance to make the sync even if it's outdated
                if (this._setProgress.IsOutdated && this.SyncOutdated != null)
                {
                    Logger.Current.Info("Raising Sync Remote Outdated Event");
                    var outdatedEventArgs = new OutdatedEventArgs();
                    this.SyncOutdated(this, outdatedEventArgs);
                    Logger.Current.Info($"Action taken : {outdatedEventArgs.Action.ToString()}");

                    if (outdatedEventArgs.Action == OutdatedSyncAction.PartialSync)
                    {
                        Logger.Current.Info("Attempting Partial Sync");
                        this._setProgress.IsOutdated = false;
                    }
                }

                // the sync is still outdated, abort it
                if (this._setProgress.IsOutdated)
                {
                    Logger.Current.Info("Aborting Sync");
                    return;
                }

                if (this.MemoryDataCacheSize == 0)
                    this.EnumerateChangesInternal(scope);
                else
                    this.EnumerateChangesInBatchesInternal(scope);

                Logger.Current.Info("Committing transaction");

            }
            catch (Exception exception)
            {
                Logger.Current.Error($"Caught exception while getting changes: {exception}");

                if (this.Connection.State != ConnectionState.Closed)
                    this.Connection.Close();

                throw;
            }

            finally
            {
                if (this.Connection.State != ConnectionState.Closed)
                    this.Connection.Close();
            }
        }

        /// <summary>
        /// Enumerate all internal changes, no batch mode
        /// </summary>
        internal void EnumerateChangesInternal(ScopeInfo scope)
        {
            Logger.Current.Info($"----- Enumerating Changes for Scope \"{scope.Name}\" -----");
            Logger.Current.Info("");
            Logger.Current.Info("");

            foreach (SyncAdapter syncAdapter in this.Adapters)
            {
                Logger.Current.Info($"----- Table \"{syncAdapter.RemoteTableName}\" -----");

                // get the select incremental changes command
                DbCommand selectIncrementalChangesCommand = syncAdapter.SelectIncrementalChangesCommand;

                if (selectIncrementalChangesCommand == null)
                {
                    var exc = "Missing command 'SelectIncrementalChangesCommand' ";
                    Logger.Current.Error(exc);
                    throw new Exception(exc);
                }

                var tableProgress = this._setProgress.FindTableProgress(syncAdapter.RemoteTableName);

                try
                {
                    // Open the connection
                    this.Connection.Open();

                    // Set the parameters
                    DbHelper.SetParameterValue(selectIncrementalChangesCommand, "sync_min_timestamp", scope.LastTimestamp);
                    DbHelper.SetParameterValue(selectIncrementalChangesCommand, "sync_scope_name", scope.Name);

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
                            state = GetStateFromDmRow(scope, dataRow);

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
                        this._progressEventArgs.SelectedChanges.Add(changes);

                        this.SyncSelectedChanges?.Invoke(this, changes);

                    }
                }
                catch (Exception dbException)
                {
                    Logger.Current.Error($"Caught exception while enumerating changes\n{dbException}\n");
                    throw;
                }
                finally
                {

                    Logger.Current.Info($"--- End Table \"{syncAdapter.RemoteTableName}\" ---");
                    Logger.Current.Info("");

                    if (this.Connection != null && this.Connection.State == ConnectionState.Open)
                        this.Connection.Close();
                }
            }

            Logger.Current.Info($"--- End Enumerating Changes for Scope \"{scope.Name}\" ---");
            Logger.Current.Info("");
        }

       
        /// <summary>
        /// Enumerate all internal changes, no batch mode
        /// </summary>
        internal void EnumerateChangesInBatchesInternal(ScopeInfo scope)
        {
            Logger.Current.Info($"----- Enumerating Changes for Scope \"{scope.Name}\" -----");
            Logger.Current.Info("");
            Logger.Current.Info("");

            // memory size total
            long memorySizeFromDmRows = 0L;

            foreach (SyncAdapter syncAdapter in this.Adapters)
            {
                Logger.Current.Info($"----- Table \"{syncAdapter.RemoteTableName}\" -----");

                // get the select incremental changes command
                DbCommand selectIncrementalChangesCommand = syncAdapter.SelectIncrementalChangesCommand;

                if (selectIncrementalChangesCommand == null)
                {
                    var exc = "Missing command 'SelectIncrementalChangesCommand' ";
                    Logger.Current.Error(exc);
                    throw new Exception(exc);
                }

                var tableProgress = this._setProgress.FindTableProgress(syncAdapter.RemoteTableName);

                try
                {
                    // Open the connection
                    this.Connection.Open();

                    // Set the parameters
                    DbHelper.SetParameterValue(selectIncrementalChangesCommand, "sync_min_timestamp", scope.LastTimestamp);
                    DbHelper.SetParameterValue(selectIncrementalChangesCommand, "sync_scope_name", scope.Name);

                    this.AddTrackingColumns<int>(tableProgress.Changes, "sync_row_is_tombstone");

                    // Get the reader
                    using (var dataReader = selectIncrementalChangesCommand.ExecuteReader())
                    {
                        var dmTable = tableProgress.Changes;

                        while (dataReader.Read())
                        {
                            DmRow dmRow = CreateRowFromReader(dataReader, dmTable);
                            DmRowState state = DmRowState.Unchanged;

                            state = GetStateFromDmRow(scope, dmRow);

                            // If the row is not deleted inserted or modified, go next
                            if (state != DmRowState.Deleted && state != DmRowState.Modified && state != DmRowState.Added)
                                continue;


                            var dmRowSize = DbHelper.GetRowSizeFromDataRow(dmRow);

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

                                // Create a progress event
                                var changes = new ScopeSelectedChanges();
                                changes.View = new DmView(tableProgress.Changes);
                                this._progressEventArgs.SelectedChanges.Add(changes);

                                this.SyncSelectedChanges?.Invoke(this, changes);

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
                        this._progressEventArgs.SelectedChanges.Add(selectedChanges);

                        this.SyncSelectedChanges?.Invoke(this, selectedChanges);

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

                    Logger.Current.Info($"--- End Table \"{syncAdapter.RemoteTableName}\" ---");
                    Logger.Current.Info("");

                    if (this.Connection != null && this.Connection.State == ConnectionState.Open)
                        this.Connection.Close();
                }
            }

            Logger.Current.Info($"--- End Enumerating Changes for Scope \"{scope.Name}\" ---");
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
        private static DmRowState GetStateFromDmRow(ScopeInfo scope, DmRow dataRow)
        {
            DmRowState dmRowState = DmRowState.Unchanged;

            if ((int)dataRow["sync_row_is_tombstone"] == 1)
                dmRowState = DmRowState.Deleted;
            else
            {
                var createdTimeStamp = DbHelper.ParseTimestamp(dataRow["create_timestamp"]);
                var updatedTimeStamp = DbHelper.ParseTimestamp(dataRow["update_timestamp"]);

                if (createdTimeStamp > scope.LastTimestamp)
                    dmRowState = DmRowState.Added;
                else if (updatedTimeStamp > scope.LastTimestamp)
                    dmRowState = DmRowState.Modified;
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
        /// </summary>
        public void ApplyChanges(ScopeInfo fromScope, SyncSetProgress changes)
        {
            ChangeApplicationAction changeApplicationAction;
            DbTransaction applyTransaction = null;

            this.Connection.Open();

            try
            {
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

                    if (this.Connection != null && this.Connection.State == ConnectionState.Open)
                    {
                        this.Connection.Close();
                    }

                };

                // Create a transaction
                applyTransaction = this.Connection.BeginTransaction(IsolationLevel.ReadCommitted);

                // Checks adapters
                this.CheckForMissingAdapters(changes.TablesProgress);


                Logger.Current.Info($"----- Applying Changes for Scope \"{fromScope.Name}\" -----");
                Logger.Current.Info("");

                // -----------------------------------------------------
                // 1) Applying deletes
                // -----------------------------------------------------
                changeApplicationAction = this.ApplyChangesInternal(applyTransaction, fromScope, changes, DmRowState.Deleted);

                // Rollback
                if (changeApplicationAction == ChangeApplicationAction.RollbackTransaction)
                {
                    rollbackAction();
                    return;
                }

                // -----------------------------------------------------
                // 1) Applying Inserts
                // -----------------------------------------------------

                changeApplicationAction = this.ApplyChangesInternal(applyTransaction, fromScope, changes, DmRowState.Added);

                // Rollback
                if (changeApplicationAction == ChangeApplicationAction.RollbackTransaction)
                {
                    rollbackAction();
                    return;
                }

                // -----------------------------------------------------
                // 1) Applying updates
                // -----------------------------------------------------


                changeApplicationAction = this.ApplyChangesInternal(applyTransaction, fromScope, changes, DmRowState.Modified);

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

            if (applyTransaction != null)
            {
                applyTransaction.Dispose();
                applyTransaction = null;
            }

            if (this.Connection != null && this.Connection.State == ConnectionState.Open)
            {
                this.Connection.Close();
            }
        }


        /// <summary>
        /// Apply changes internal method for one Insert or Update or Delete for every dbSyncAdapter
        /// </summary>
        internal ChangeApplicationAction ApplyChangesInternal(DbTransaction transaction, ScopeInfo fromScope, SyncSetProgress changes, DmRowState applyType)
        {
            ChangeApplicationAction changeApplicationAction = ChangeApplicationAction.Continue;

            // for each adapters (Zero to End for Insert / Updates -- End to Zero for Deletes
            for (int i = 0; i < this.Adapters.Count; i++)
            {
                try
                {
                    // If we have a delete we must go from Up to Down, orthewise Dow to Up index
                    var syncAdapter = (applyType != DmRowState.Deleted ?
                            this.Adapters[i] :
                            this.Adapters[this.Adapters.Count - i - 1]);

                    // Set syncAdapter properties
                    syncAdapter.applyType = applyType;
                    syncAdapter.Transaction = transaction;

                    if (syncAdapter.ConflictActionInvoker == null)
                        syncAdapter.ConflictActionInvoker = GetConflictAction;

                    Logger.Current.Info($"----- Operation {applyType.ToString()} for Table \"{syncAdapter.RemoteTableName}\" -----");

                    // getting the tableProgress to be applied
                    var tableProgress = changes.FindTableProgress(syncAdapter.RemoteTableName);

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
                        Logger.Current.Info($"--- End {applyType.ToString()} for Table \"{syncAdapter.RemoteTableName}\" ---");
                        Logger.Current.Info($"");
                        continue;
                    }


                    // Conflicts occured when trying to apply rows
                    List<SyncConflict> conflicts = new List<SyncConflict>();

                    DmView rowsApplied;
                    // applying the bulkchanges command
                    if (syncAdapter.ScopeConfigDataAdapter.HasBulkOperationsEnabled)
                        rowsApplied = syncAdapter.ApplyBulkChanges(dmChangesView, fromScope, conflicts);
                    else
                        rowsApplied = syncAdapter.ApplyChanges(dmChangesView, fromScope, conflicts);

                    // If conflicts occured
                    if (conflicts != null)
                    {
                        foreach (var conflict in conflicts)
                        {
                            DmRow resolvedRow;
                            changeApplicationAction = syncAdapter.HandleConflict(conflict, fromScope, out resolvedRow);

                            // row resolved
                            if (resolvedRow != null)
                                rowsApplied.Add(resolvedRow);

                            if (changeApplicationAction != ChangeApplicationAction.RollbackTransaction)
                                continue;
                        }
                    }

                    // Handle sync progress for this syncadapter (so this table)
                    this._progressEventArgs.Stage = SyncStage.ApplyingChanges;
                    var appliedChanges = new ScopeAppliedChanges();
                    appliedChanges.View = rowsApplied;
                    appliedChanges.State = applyType;
                    this.SyncAppliedChanges?.Invoke(this, appliedChanges);

                    // Check action
                    changeApplicationAction = appliedChanges.Action;

                    if (changeApplicationAction == ChangeApplicationAction.RollbackTransaction)
                        return ChangeApplicationAction.RollbackTransaction;


                    Logger.Current.Info("");
                    //Logger.Current.Info($"{this._changeHandler.ApplyCount} {operation} Applied");
                    Logger.Current.Info($"--- End {applyType.ToString()} for Table \"{syncAdapter.RemoteTableName}\" ---");
                    Logger.Current.Info("");

                }
                catch (Exception ex)
                {
                    Logger.Current.Error($"Error during ApplyInternalChanges : {ex.Message}");
                    throw;
                }
            }
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
        void AddTrackingColumns<T>(DmTable table, string name)
        {
            if (!table.Columns.Contains(name))
            {
                var dc = new DmColumn<T>(name) { DefaultValue = default(T) };
                table.Columns.Add(dc);
            }
        }

        internal void RemoveTrackingColumns(DmTable changes, string name)
        {
            if (changes.Columns.Contains(name))
                changes.Columns.Remove(name);
        }

        /// <summary>
        /// Adding sync columns to the changes datatable
        /// </summary>
        void AddTimestampValue(DmTable table, long tickCount)
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

        internal void CheckForMissingAdapters(IList<SyncTableProgress> lstStp)
        {
            List<string> strs = new List<string>();

            foreach (var stp in lstStp)
            {
                var adapterFound = this.Adapters.Any(a => string.Equals(a.TableName, stp.TableName, StringComparison.CurrentCultureIgnoreCase));

                if (adapterFound)
                    continue;

                strs.Add(stp.TableName);
            }
            if (strs.Count > 0)
            {
                StringBuilder stringBuilder = new StringBuilder();
                string empty = string.Empty;
                foreach (string str in strs)
                {
                    stringBuilder.Append(string.Concat(empty, str));
                    empty = ", ";
                }

                var errorString = $"Cannot Apply Changes since Adapters are missing for the following tables: {stringBuilder.ToString()}.  Please ensure that the local and global names on the Adapters are set properly.";
                throw new Exception(errorString);
            }
        }

        /// <summary>
        /// A conflict has occured, we try to ask for the solution to the user
        /// </summary>
        internal ApplyAction GetConflictAction(SyncConflict conflict, DbConnection connection, DbTransaction transaction)
        {
            Logger.Current.Debug("Raising Apply Change Failed Event");
            var dbApplyChangeFailedEventArg = new ApplyChangeFailedEventArgs(conflict, connection, transaction);

            this.ApplyChangedFailed?.Invoke(this, dbApplyChangeFailedEventArg);

            ApplyAction action = dbApplyChangeFailedEventArg.Action;
            Logger.Current.Debug($"Action: {action.ToString()}");
            return action;
        }

    }
}
