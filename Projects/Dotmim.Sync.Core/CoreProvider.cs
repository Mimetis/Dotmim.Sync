using Dotmim.Sync.Batch;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Cache;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Log;
using Dotmim.Sync.Manager;
using Dotmim.Sync.Data;
using Dotmim.Sync.Data.Surrogate;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Threading;
using Dotmim.Sync.Serialization;

namespace Dotmim.Sync
{
    /// <summary>
    /// Core provider : should be implemented by any server / client provider
    /// </summary>
    public abstract class CoreProvider : IProvider
    {
        private const string SYNC_CONF = "syncconf";

        private bool syncInProgress;
        private string providerType;
        private SyncConfiguration syncConfiguration;
        private CancellationToken cancellationToken;

        /// <summary>
        /// Raise an event if the sync is outdated. 
        /// Let the user choose if he wants to force or not
        /// </summary>
        public event EventHandler<OutdatedEventArgs> SyncOutdated = null;

        /// <summary>
        /// Occurs during progress
        /// </summary>
        public event EventHandler<SyncProgressEventArgs> SyncProgress = null;

        /// <summary>
        /// Occurs when a conflict is raised.
        /// </summary>
        public event EventHandler<ApplyChangeFailedEventArgs> ApplyChangedFailed = null;

        /// <summary>
        /// Create a new instance of the implemented Connection provider
        /// </summary>
        public abstract DbConnection CreateConnection();

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
        public abstract DbScopeBuilder GetScopeBuilder();

        /// <summary>
        /// Gets or sets the metadata resolver (validating the columns definition from the data store)
        /// </summary>
        public abstract DbMetadata Metadata { get; set; }

        /// <summary>
        /// Get the cache manager. will store the configuration because we dont want to store it in database
        /// </summary>
        public abstract ICache CacheManager { get; set; }

        /// <summary>
        /// Get the provider type name
        /// </summary>
        public abstract string ProviderTypeName { get; }

        /// <summary>
        /// Gets or sets the connection string used by the implemented provider
        /// </summary>
        public string ConnectionString { get; set; }


        /// <summary>
        /// Gets a boolean indicating if the provider can use bulk operations
        /// </summary>
        public abstract bool SupportBulkOperations { get; }

        /// <summary>
        /// Gets a boolean indicating if the provider can be a server side provider
        /// </summary>
        public abstract bool CanBeServerProvider { get; }

        /// <summary>
        /// Called by the  to indicate that a 
        /// synchronization session has started.
        /// </summary>
        public virtual Task<SyncContext> BeginSessionAsync(SyncContext context)
        {
            try
            {
                Logger.Current.Info($"BeginSession() called on Provider {this.ProviderTypeName}");

                lock (this)
                {
                    if (this.syncInProgress)
                        throw SyncException.CreateInProgressException(context.SyncStage);

                    this.syncInProgress = true;
                }

                // Set stage
                context.SyncStage = SyncStage.BeginSession;

                // Event progress
                var progressEventArgs = new SyncProgressEventArgs
                {
                    ProviderTypeName = this.ProviderTypeName,
                    Context = context,
                    Action = ChangeApplicationAction.Continue
                };
                this.SyncProgress?.Invoke(this, progressEventArgs);

                if (progressEventArgs.Action == ChangeApplicationAction.Rollback)
                    throw SyncException.CreateRollbackException(context.SyncStage);


            }
            catch (Exception ex)
            {
                if (ex is SyncException)
                    throw;
                else
                    throw SyncException.CreateUnknowException(context.SyncStage, ex);
            }
            return Task.FromResult(context);
        }

        /// <summary>
        /// Gets or Sets the Server configuration. Use this property only in Proxy mode, and on server side !
        /// </summary>
        public void SetConfiguration(SyncConfiguration syncConfiguration)
        {
            if (syncConfiguration == null || syncConfiguration.Tables == null || syncConfiguration.Tables.Length <= 0)
                throw new ArgumentException("Service Configuration must exists and contains at least one table to sync.");

            this.syncConfiguration = syncConfiguration;
        }


        /// <summary>
        /// Generate the DmTable configuration from a given columns list
        /// Validate that all columns are currently supported by the provider
        /// </summary>
        private DmTable GenerateTableFromColumnsList(string tableName, List<DbColumnDefinition> columns, IDbManagerTable dbManagerTable)
        {
            DmTable table = new DmTable(tableName);

            // set the original provider name. IF Server and Client are same providers, we could probably be safer with types
            table.OriginalProvider = this.ProviderTypeName;

            var ordinal = 0;
            foreach (var columnDefinition in columns.OrderBy(c => c.Ordinal))
            {
                // First of all validate if the column is currently supported
                if (!Metadata.IsValid(columnDefinition))
                    throw SyncException.CreateNotSupportedException(
                        SyncStage.EnsureConfiguration, $"The Column {columnDefinition.Name} of type {columnDefinition.TypeName} from provider {this.ProviderTypeName} is not currently supported.");

                // Gets the datastore owner dbType (could be SqlDbtype, MySqlDbType, SQLiteDbType, NpgsqlDbType & so on ...)
                object datastoreDbType = Metadata.ValidateOwnerDbType(columnDefinition.TypeName, columnDefinition.IsUnsigned, columnDefinition.IsUnicode);

                // once we have the datastore type, we can have the managed type
                Type columnType = Metadata.ValidateType(datastoreDbType);

                // and the DbType
                DbType columnDbType = Metadata.ValidateDbType(columnDefinition.TypeName, columnDefinition.IsUnsigned, columnDefinition.IsUnicode);

                // Create the DmColumn with the right type
                var newColumn = DmColumn.CreateColumn(columnDefinition.Name, columnType);

                newColumn.DbType = columnDbType;

                // Gets the owner dbtype (SqlDbType, OracleDbType, MySqlDbType, NpsqlDbType & so on ...)
                // SQLite does not have it's own type, so it's DbType too
                newColumn.OrginalDbType = datastoreDbType.ToString();

                table.Columns.Add(newColumn);

                newColumn.SetOrdinal(ordinal);
                ordinal++;

                newColumn.AllowDBNull = columnDefinition.IsNullable;
                newColumn.AutoIncrement = columnDefinition.IsIdentity;

                // Validate max length
                newColumn.MaxLength = Metadata.ValidateMaxLength(columnDefinition.TypeName, columnDefinition.IsUnsigned, columnDefinition.IsUnicode, columnDefinition.MaxLength);

                // Validate if column should be readonly
                newColumn.ReadOnly = Metadata.ValidateIsReadonly(columnDefinition);

                // Validate the precision and scale properties
                if (Metadata.IsNumericType(columnDefinition.TypeName))
                {
                    if (Metadata.SupportScale(columnDefinition.TypeName))
                    {
                        var (p, s) = Metadata.ValidatePrecisionAndScale(columnDefinition);
                        newColumn.Precision = p;
                        newColumn.Scale = s;
                    }
                    else
                    {
                        newColumn.Precision = Metadata.ValidatePrecision(columnDefinition);
                    }

                }

            }

            // Get PrimaryKey
            var dmTableKeys = dbManagerTable.GetTablePrimaryKeys();

            if (dmTableKeys == null || dmTableKeys.Count == 0)
                throw new Exception("No Primary Keys in this table, it' can't happen :) ");

            DmColumn[] columnsForKey = new DmColumn[dmTableKeys.Count];

            for (int i = 0; i < dmTableKeys.Count; i++)
            {
                var rowColumn = dmTableKeys[i];

                var columnKey = table.Columns.FirstOrDefault(c => String.Equals(c.ColumnName, rowColumn, StringComparison.InvariantCultureIgnoreCase));

                columnsForKey[i] = columnKey ?? throw new Exception("Primary key found is not present in the columns list");
            }

            // Set the primary Key
            table.PrimaryKey = new DmKey(columnsForKey);


            return table;
        }


        /// <summary>
        /// update configuration object with tables desc from server database
        /// </summary>
        private async Task<SyncConfiguration> UpdateConfigurationInternalAsync(SyncContext context, SyncConfiguration syncConfiguration)
        {
            // clear service configuration
            if (syncConfiguration.ScopeSet != null)
                syncConfiguration.ScopeSet.Clear();

            if (syncConfiguration.ScopeSet == null)
                syncConfiguration.ScopeSet = new DmSet("DotmimSync");

            if (syncConfiguration.Tables == null || syncConfiguration.Tables.Length <= 0)
                return syncConfiguration;
            DbConnection connection;
            DbTransaction transaction;

            using (connection = this.CreateConnection())
            {
                try
                {
                    await connection.OpenAsync();

                    using (transaction = connection.BeginTransaction())
                    {
                        foreach (var table in syncConfiguration.Tables)
                        {
                            var builderTable = this.GetDbManager(table);
                            var tblManager = builderTable.GetManagerTable(connection, transaction);

                            // get columns list
                            var lstColumns = tblManager.GetTableDefinition();

                            // Validate the column list and get the dmTable configuration object.
                            var dmTable = GenerateTableFromColumnsList(table, lstColumns, tblManager);

                            syncConfiguration.ScopeSet.Tables.Add(dmTable);

                            var relations = tblManager.GetTableRelations();

                            if (relations != null)
                            {
                                foreach (var r in relations)
                                {
                                    DmColumn tblColumn = dmTable.Columns[r.ColumnName];
                                    DmColumn foreignColumn = null;
                                    var foreignTable = syncConfiguration.ScopeSet.Tables[r.ReferenceTableName];

                                    if (foreignTable == null)
                                        throw new SyncException($"Foreign table {r.ReferenceTableName} does not exist", context.SyncStage, SyncExceptionType.DataStore);

                                    foreignColumn = foreignTable.Columns[r.ReferenceColumnName];

                                    if (foreignColumn == null)
                                        throw new SyncException($"Foreign column {r.ReferenceColumnName} does not exist in table {r.TableName}", context.SyncStage, SyncExceptionType.DataStore);

                                    DmRelation dmRelation = new DmRelation(r.ForeignKey, tblColumn, foreignColumn);

                                    syncConfiguration.ScopeSet.Relations.Add(dmRelation);
                                }
                            }

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
                return syncConfiguration;
            }
        }

        /// <summary>
        /// Ensure configuration is correct on both server and client side
        /// </summary>
        public virtual async Task<(SyncContext, SyncConfiguration)> EnsureConfigurationAsync(SyncContext context, SyncConfiguration configuration = null)
        {
            try
            {
                DmSerializer serializer = new DmSerializer();

                context.SyncStage = SyncStage.EnsureConfiguration;

                // Get cache manager and try to get configuration from cache
                var cacheManager = this.CacheManager;
                var cacheConfiguration = GetCacheConfiguration();

                // if we don't pass config object, we may be in proxy mode, so the config object is handled by a local configuration object.
                if (configuration == null && this.syncConfiguration == null)
                    throw SyncException.CreateArgumentException(SyncStage.EnsureConfiguration, "Configuration", "You try to set a provider with no configuration object");

                // the configuration has been set from the proxy server itself, use it.
                if (configuration == null && this.syncConfiguration != null)
                    configuration = this.syncConfiguration;

                // if we have already a cache configuration, we can return, except if we should overwrite it
                if (cacheConfiguration != null && !configuration.OverwriteConfiguration)
                {
                    // Event progress
                    var progressEventArgs = new SyncProgressEventArgs
                    {
                        ProviderTypeName = this.ProviderTypeName,
                        Context = context,
                        Action = ChangeApplicationAction.Continue,
                        Configuration = configuration
                    };
                    this.SyncProgress?.Invoke(this, progressEventArgs);

                    if (progressEventArgs.Action == ChangeApplicationAction.Rollback)
                        throw SyncException.CreateRollbackException(context.SyncStage);

                    return (context, cacheConfiguration);
                }

                // create local directory
                if (!String.IsNullOrEmpty(configuration.BatchDirectory) && !Directory.Exists(configuration.BatchDirectory))
                    Directory.CreateDirectory(configuration.BatchDirectory);

                // if we dont have already read the tables || we want to overwrite the current config
                if (configuration.ScopeSet == null || configuration.ScopeSet.Tables.Count == 0 || configuration.OverwriteConfiguration)
                    configuration = await this.UpdateConfigurationInternalAsync(context, configuration);

                // save to cache
                var dmSetConf = new DmSet();
                SyncConfiguration.SerializeInDmSet(dmSetConf, configuration);
                var dmSSetConf = new DmSetSurrogate(dmSetConf);
                cacheManager.Set(SYNC_CONF, dmSSetConf);

                // Event progress
                var progressEventArgs2 = new SyncProgressEventArgs
                {
                    ProviderTypeName = this.ProviderTypeName,
                    Context = context,
                    Action = ChangeApplicationAction.Continue,
                    Configuration = configuration
                };
                this.SyncProgress?.Invoke(this, progressEventArgs2);

                if (progressEventArgs2.Action == ChangeApplicationAction.Rollback)
                    throw SyncException.CreateRollbackException(context.SyncStage);

                return (context, configuration);
            }
            catch (Exception ex)
            {
                if (ex is SyncException)
                    throw;
                else
                    throw SyncException.CreateUnknowException(context.SyncStage, ex);
            }
        }

        /// <summary>
        /// Get cached configuration (inmemory or session cache)
        /// </summary>
        public SyncConfiguration GetCacheConfiguration()
        {
            var configurationSurrogate = this.CacheManager.GetValue<DmSetSurrogate>(SYNC_CONF);
            if (configurationSurrogate == null)
                return null;

            var dmSet = configurationSurrogate.ConvertToDmSet();
            if (dmSet == null)
                return null;

            var conf = SyncConfiguration.DeserializeFromDmSet(dmSet);
            return conf;

        }

        private DmTable BuildChangesTable(string tableName)
        {
            var configuration = GetCacheConfiguration();

            var dmTable = configuration.ScopeSet.Tables[tableName].Clone();

            // Adding the tracking columns
            AddTrackingColumns<Guid>(dmTable, "create_scope_id");
            AddTrackingColumns<long>(dmTable, "create_timestamp");
            AddTrackingColumns<Guid>(dmTable, "update_scope_id");
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
        public virtual Task<SyncContext> EndSessionAsync(SyncContext context)
        {
            try
            {
                // already ended
                lock (this)
                {
                    if (!syncInProgress)
                        return Task.FromResult(context);
                }

                Logger.Current.Info($"EndSession() called on Provider {this.ProviderTypeName}");

                context.SyncStage = SyncStage.EndSession;

                // Event progress
                var progressEventArgs = new SyncProgressEventArgs
                {
                    ProviderTypeName = this.ProviderTypeName,
                    Context = context,
                    Action = ChangeApplicationAction.Continue
                };
                this.SyncProgress?.Invoke(this, progressEventArgs);
            }
            catch (Exception ex)
            {
                if (ex is SyncException)
                    throw;
                else
                    throw SyncException.CreateUnknowException(context.SyncStage, ex);
            }

            finally
            {
                lock (this) { this.syncInProgress = false; }
            }

            return Task.FromResult(context);
        }

        /// <summary>
        /// Called when the sync ensure scopes are created
        /// </summary>
        public virtual async Task<(SyncContext, List<ScopeInfo>)> EnsureScopesAsync(SyncContext context, string scopeName, Guid? clientReferenceId = null)
        {
            try
            {
                if (string.IsNullOrEmpty(scopeName))
                    throw SyncException.CreateArgumentException(SyncStage.EnsureScopes, "ScopeName");

                context.SyncStage = SyncStage.EnsureScopes;

                // Event progress
                var progressEventArgs = new SyncProgressEventArgs
                {
                    ProviderTypeName = this.ProviderTypeName,
                    Context = context,
                    Action = ChangeApplicationAction.Continue
                };
                this.SyncProgress?.Invoke(this, progressEventArgs);

                if (progressEventArgs.Action == ChangeApplicationAction.Rollback)
                    throw SyncException.CreateRollbackException(context.SyncStage);

                List<ScopeInfo> scopes = new List<ScopeInfo>();

                // Open the connection
                using (var connection = this.CreateConnection())
                {
                    try
                    {
                        await connection.OpenAsync();

                        using (var transaction = connection.BeginTransaction())
                        {
                            var scopeBuilder = this.GetScopeBuilder();
                            var scopeInfoBuilder = scopeBuilder.CreateScopeInfoBuilder(connection, transaction);

                            // create the scope info table if needed
                            if (scopeInfoBuilder.NeedToCreateScopeInfoTable())
                                scopeInfoBuilder.CreateScopeInfoTable();

                            // --------------------------------------------------------
                            // get all scopes shared by all (identified by scopeName)
                            var lstScopes = scopeInfoBuilder.GetAllScopes(scopeName);

                            // try to get the scopes from database
                            // could be two scopes if from server or a single scope if from client
                            scopes = lstScopes.Where(s => (s.IsLocal == true || (clientReferenceId.HasValue && s.Id == clientReferenceId.Value))).ToList();
                            // --------------------------------------------------------

                            // If no scope found, create it on the local provider
                            if (scopes == null || scopes.Count <= 0)
                            {
                                scopes = new List<ScopeInfo>();

                                // create a new scope id for the current owner (could be server or client as well)
                                var scope = new ScopeInfo();
                                scope.Id = Guid.NewGuid();
                                scope.Name = scopeName;
                                scope.IsLocal = true;
                                scope.IsNewScope = true;
                                scope.LastSync = null;

                                scope = scopeInfoBuilder.InsertOrUpdateScopeInfo(scope);

                                scopes.Add(scope);
                            }
                            else
                            {
                                //check if we have alread a good last sync. if no, treat it as new
                                scopes.ForEach(sc =>
                                {
                                    sc.IsNewScope = sc.LastSync == null;
                                });
                            }

                            // if we have a reference in args, we need to get this specific line from database
                            // this happen only on the server side
                            if (clientReferenceId.HasValue)
                            {
                                var refScope = scopes.FirstOrDefault(s => s.Id == clientReferenceId);

                                if (refScope == null)
                                {
                                    refScope = new ScopeInfo();
                                    refScope.Id = clientReferenceId.Value;
                                    refScope.Name = scopeName;
                                    refScope.IsLocal = false;
                                    refScope.IsNewScope = true;
                                    refScope.LastSync = null;

                                    refScope = scopeInfoBuilder.InsertOrUpdateScopeInfo(refScope);

                                    scopes.Add(refScope);
                                }
                                else
                                {
                                    refScope.IsNewScope = refScope.LastSync == null;
                                }
                            }
                            transaction.Commit();
                        }

                    }
                    catch (DbException dbex)
                    {
                        throw SyncException.CreateDbException(context.SyncStage, dbex);
                    }
                    finally
                    {
                        if (connection.State != ConnectionState.Closed)
                            connection.Close();
                    }

                }
                return (context, scopes);
            }
            catch (Exception ex)
            {
                if (ex is SyncException)
                    throw;
                else
                    throw SyncException.CreateUnknowException(context.SyncStage, ex);
            }
        }

        /// <summary>
        /// Write scope in the provider datasource
        /// </summary>
        public virtual async Task<SyncContext> WriteScopesAsync(SyncContext context, List<ScopeInfo> scopes)
        {
            // Open the connection
            using (var connection = this.CreateConnection())
            {
                try
                {
                    await connection.OpenAsync();
                    using (var transaction = connection.BeginTransaction())
                    {
                        context.SyncStage = SyncStage.WriteMetadata;


                        var scopeBuilder = this.GetScopeBuilder();
                        var scopeInfoBuilder = scopeBuilder.CreateScopeInfoBuilder(connection, transaction);

                        var progressEventArgs = new SyncProgressEventArgs
                        {
                            ProviderTypeName = this.ProviderTypeName,
                            Context = context,
                            Action = ChangeApplicationAction.Continue,
                            Scopes = new List<ScopeInfo>()
                        };
                        foreach (var scope in scopes)
                        {
                            var newScope = scopeInfoBuilder.InsertOrUpdateScopeInfo(scope);
                            progressEventArgs.Scopes.Add(newScope);
                        }

                        this.SyncProgress?.Invoke(this, progressEventArgs);

                        if (progressEventArgs.Action == ChangeApplicationAction.Rollback)
                            throw SyncException.CreateRollbackException(context.SyncStage);

                        transaction.Commit();
                    }
                }
                catch (DbException dbex)
                {
                    throw SyncException.CreateDbException(context.SyncStage, dbex);
                }
                catch (Exception ex)
                {
                    Logger.Current.Error(ex.Message);

                    if (ex is SyncException)
                        throw;
                    else
                        throw SyncException.CreateUnknowException(context.SyncStage, ex);

                    throw;
                }
                finally
                {
                    if (connection.State != ConnectionState.Closed)
                        connection.Close();
                }
                return context;

            }
        }

        /// <summary>
        /// Read a scope info
        /// </summary>
        /// <returns></returns>
        public virtual async Task<(SyncContext, long)> GetLocalTimestampAsync(SyncContext context)
        {
            // Open the connection
            using (var connection = this.CreateConnection())
            {
                try
                {
                    await connection.OpenAsync();
                    var scopeBuilder = this.GetScopeBuilder();
                    var scopeInfoBuilder = scopeBuilder.CreateScopeInfoBuilder(connection);
                    var localTime = scopeInfoBuilder.GetLocalTimestamp();
                    return (context, localTime);
                }
                catch (DbException dbex)
                {
                    throw SyncException.CreateDbException(context.SyncStage, dbex);
                }
                catch (Exception ex)
                {
                    if (ex is SyncException)
                        throw;
                    else
                        throw SyncException.CreateUnknowException(context.SyncStage, ex);
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
        public virtual async Task<SyncContext> EnsureDatabaseAsync(SyncContext context, ScopeInfo scopeInfo, DbBuilderOption options)
        {

            context.SyncStage = SyncStage.EnsureDatabase;
            var configuration = GetCacheConfiguration();

            // If scope exists and lastdatetime sync is present, so database exists
            // Check if we don't have an OverwriteConfiguration (if true, we force the check)
            if (scopeInfo.LastSync.HasValue && !configuration.OverwriteConfiguration)
                return context;

            string script = null;

            // Open the connection
            using (var connection = this.CreateConnection())
            {
                try
                {
                    await connection.OpenAsync();

                    using (var transaction = connection.BeginTransaction())
                    {
                        foreach (var dmTable in configuration.ScopeSet.Tables)
                        {
                            var builder = GetDatabaseBuilder(dmTable, options);

                            // adding filter
                            if (configuration.Filters != null && configuration.Filters.Count > 0)
                            {
                                var filters = configuration.Filters.Where(f => dmTable.TableName.Equals(f.TableName, StringComparison.InvariantCultureIgnoreCase));

                                foreach (var filter in filters)
                                {
                                    var columnFilter = dmTable.Columns[filter.ColumnName];

                                    if (columnFilter == null)
                                        throw new InvalidExpressionException($"Column {filter.ColumnName} does not exist in Table {dmTable.TableName}");

                                    builder.FilterColumns.Add(filter.TableName, filter.ColumnName);
                                }
                            }

                            script = builder.Script(connection, transaction);
                            builder.Apply(connection, transaction);
                        }

                        // Event progress
                        var progressEventArgs = new SyncProgressEventArgs
                        {
                            ProviderTypeName = this.ProviderTypeName,
                            Context = context,
                            Action = ChangeApplicationAction.Continue,
                            DatabaseScript = script
                        };
                        this.SyncProgress?.Invoke(this, progressEventArgs);

                        if (progressEventArgs.Action == ChangeApplicationAction.Rollback)
                            throw SyncException.CreateRollbackException(context.SyncStage);

                        transaction.Commit();
                    }

                }
                catch (DbException dbex)
                {
                    throw SyncException.CreateDbException(context.SyncStage, dbex);
                }
                catch (Exception ex)
                {
                    if (ex is SyncException)
                        throw;
                    else
                        throw SyncException.CreateUnknowException(context.SyncStage, ex);
                }
                finally
                {
                    if (connection.State != ConnectionState.Closed)
                        connection.Close();
                }
                return context;
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
        public virtual async Task<(SyncContext, BatchInfo, ChangesStatistics)> GetChangeBatchAsync(SyncContext context, ScopeInfo scopeInfo)
        {
            try
            {
                if (scopeInfo == null)
                    throw new ArgumentException("ClientScope is null");

                var configuration = GetCacheConfiguration();

                // check batchSize if not > then Configuration.DownloadBatchSizeInKB
                if (configuration.DownloadBatchSizeInKB > 0)
                    Logger.Current.Info($"Enumeration data cache size selected: {configuration.DownloadBatchSizeInKB} Kb");

                context.SyncStage = SyncStage.SelectingChanges;

                //this.BuildTableProgress();
                BatchInfo batchInfo;
                ChangesStatistics changesStatistics;
                (context, batchInfo, changesStatistics) = await this.GetChanges(context, scopeInfo);

                // Check if the remote is not outdated
                var isOutdated = this.IsRemoteOutdated();

                if (isOutdated)
                    throw new Exception("OutDatedPeer");

                return (context, batchInfo, changesStatistics);
            }
            catch (DbException dbex)
            {
                throw SyncException.CreateDbException(context.SyncStage, dbex);
            }
            catch (Exception ex)
            {
                if (ex is SyncException)
                    throw;
                else
                    throw SyncException.CreateUnknowException(context.SyncStage, ex);
            }
        }

        internal async Task<(SyncContext, BatchInfo, ChangesStatistics)> GetChanges(SyncContext context, ScopeInfo scopeInfo)
        {
            BatchInfo batchInfo = null;
            try
            {
                Logger.Current.Info("GetChanges called: _syncBatchProducer is null");

                var configuration = GetCacheConfiguration();

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
                        Logger.Current.Info("Attempting Partial Sync");
                }

                ChangesStatistics changesStatistics = null;

                // the sync is still outdated, abort it
                if (isOutdated)
                {
                    Logger.Current.Info("Aborting Sync");
                    return (context, null, null);
                }

                context.SyncStage = SyncStage.SelectingChanges;

                // Event progress
                var progressEventArgs = new SyncProgressEventArgs
                {
                    ProviderTypeName = this.ProviderTypeName,
                    Context = context,
                    Action = ChangeApplicationAction.Continue,
                    ChangesStatistics = changesStatistics
                };

                this.SyncProgress?.Invoke(this, progressEventArgs);

                if (progressEventArgs.Action == ChangeApplicationAction.Rollback)
                    throw SyncException.CreateRollbackException(context.SyncStage);

                if (configuration.DownloadBatchSizeInKB == 0)
                    (batchInfo, changesStatistics) = await this.EnumerateChangesInternal(context, scopeInfo);
                else
                    (batchInfo, changesStatistics) = await this.EnumerateChangesInBatchesInternal(context, scopeInfo);

                return (context, batchInfo, changesStatistics);

            }
            catch (Exception)
            {
                throw;
            }


        }

        /// <summary>
        /// Enumerate all internal changes, no batch mode
        /// </summary>
        internal async Task<(BatchInfo, ChangesStatistics)> EnumerateChangesInternal(SyncContext context, ScopeInfo scopeInfo)
        {
            Logger.Current.Info($"----- Enumerating Changes for Scope \"{scopeInfo.Name}\" -----");
            Logger.Current.Info("");
            Logger.Current.Info("");

            // Get config
            var configuration = GetCacheConfiguration();

            // create the in memory changes set
            DmSet changesSet = new DmSet(configuration.ScopeSet.DmSetName);

            // Create the batch info, in memory
            var batchInfo = new BatchInfo();
            batchInfo.InMemory = true;


            using (var connection = this.CreateConnection())
            {
                // Open the connection
                await connection.OpenAsync();

                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        context.SyncStage = SyncStage.SelectingChanges;

                        ChangesStatistics changesStatistics = new ChangesStatistics();

                        // Event progress
                        var progressEventArgs = new SyncProgressEventArgs
                        {
                            ProviderTypeName = this.ProviderTypeName,
                            Context = context,
                            Action = ChangeApplicationAction.Continue,
                            ChangesStatistics = changesStatistics
                        };

                        foreach (var tableDescription in configuration.ScopeSet.Tables)
                        {
                            var builder = this.GetDatabaseBuilder(tableDescription);
                            var syncAdapter = builder.CreateSyncAdapter(connection, transaction);
                            syncAdapter.ConflictApplyAction = configuration.GetApplyAction();

                            Logger.Current.Info($"----- Table \"{tableDescription.TableName}\" -----");

                            // for stats
                            SelectedChanges selectedChanges = new SelectedChanges();
                            selectedChanges.TableName = tableDescription.TableName;

                            // Get Command
                            DbCommand selectIncrementalChangesCommand;
                            DbCommandType dbCommandType;

                            if (this.CanBeServerProvider && context.Parameters != null && context.Parameters.Count > 0 && configuration.Filters != null && configuration.Filters.Count > 0)
                            {
                                var filtersName = configuration.Filters
                                                .Where(f => f.TableName.Equals(tableDescription.TableName, StringComparison.InvariantCultureIgnoreCase))
                                                .Select(f => f.ColumnName);

                                if (filtersName != null && filtersName.Count() > 0)
                                {
                                    dbCommandType = DbCommandType.SelectChangesWitFilters;
                                    selectIncrementalChangesCommand = syncAdapter.GetCommand(dbCommandType, filtersName);
                                }
                                else
                                {
                                    dbCommandType = DbCommandType.SelectChanges;
                                    selectIncrementalChangesCommand = syncAdapter.GetCommand(dbCommandType);
                                }
                            }
                            else
                            {
                                dbCommandType = DbCommandType.SelectChanges;
                                selectIncrementalChangesCommand = syncAdapter.GetCommand(dbCommandType);
                            }

                            if (selectIncrementalChangesCommand == null)
                            {
                                var exc = "Missing command 'SelectIncrementalChangesCommand' ";
                                Logger.Current.Error(exc);
                                throw new Exception(exc);
                            }

                            // Deriving Parameters
                            syncAdapter.SetCommandParameters(dbCommandType, selectIncrementalChangesCommand);

                            // Get a clone of the table with tracking columns
                            var dmTableChanges = BuildChangesTable(tableDescription.TableName);

                            // Set the parameters
                            DbManager.SetParameterValue(selectIncrementalChangesCommand, "sync_min_timestamp", scopeInfo.LastTimestamp);
                            DbManager.SetParameterValue(selectIncrementalChangesCommand, "sync_scope_id", scopeInfo.Id);
                            DbManager.SetParameterValue(selectIncrementalChangesCommand, "sync_scope_is_new", scopeInfo.IsNewScope ? 1 : 0);

                            // Set filter parameters if any
                            if (this.CanBeServerProvider && context.Parameters != null && context.Parameters.Count > 0 && configuration.Filters != null && configuration.Filters.Count > 0)
                            {
                                var filters = configuration.Filters.Where(f => f.TableName.Equals(tableDescription.TableName, StringComparison.InvariantCultureIgnoreCase)).ToList();

                                if (filters != null && filters.Count > 0)
                                {
                                    foreach (var filter in filters)
                                    {
                                        var parameter = context.Parameters.FirstOrDefault(p => p.ColumnName.Equals(filter.ColumnName, StringComparison.InvariantCultureIgnoreCase) && p.TableName.Equals(filter.TableName, StringComparison.InvariantCultureIgnoreCase));

                                        if (parameter != null)
                                            DbManager.SetParameterValue(selectIncrementalChangesCommand, parameter.ColumnName, parameter.Value);
                                    }
                                }
                            }

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
                                    state = GetStateFromDmRow(dataRow, scopeInfo);

                                    if (state != DmRowState.Deleted && state != DmRowState.Modified && state != DmRowState.Added)
                                        continue;

                                    // add row
                                    dmTableChanges.Rows.Add(dataRow);

                                    // acceptchanges before modifying 
                                    dataRow.AcceptChanges();
                                    selectedChanges.TotalChanges++;

                                    // Set the correct state to be applied
                                    if (state == DmRowState.Deleted)
                                    {
                                        dataRow.Delete();
                                        selectedChanges.Deletes++;
                                    }
                                    else if (state == DmRowState.Added)
                                    {
                                        dataRow.SetAdded();
                                        selectedChanges.Inserts++;
                                    }
                                    else if (state == DmRowState.Modified)
                                    {
                                        dataRow.SetModified();
                                        selectedChanges.Updates++;
                                    }
                                }
                                // Since we dont need this column anymore, remove it
                                this.RemoveTrackingColumns(dmTableChanges, "sync_row_is_tombstone");

                                // add it to the DmSet
                                changesSet.Tables.Add(dmTableChanges);

                                // add to stats
                                changesStatistics.SelectedChanges.Add(selectedChanges);

                            }

                            Logger.Current.Info($"--- End Table \"{tableDescription.TableName}\" ---");
                            Logger.Current.Info("");
                        }

                        // add stats for a SyncProgress event
                        context.SyncStage = SyncStage.SelectedChanges;
                        progressEventArgs.ChangesStatistics = changesStatistics;
                        this.SyncProgress?.Invoke(this, progressEventArgs);

                        if (progressEventArgs.Action == ChangeApplicationAction.Rollback)
                            throw SyncException.CreateRollbackException(context.SyncStage);

                        transaction.Commit();

                        // generate the batchpartinfo
                        batchInfo.GenerateBatchInfo(0, changesSet, configuration.BatchDirectory);

                        // Create a new in-memory batch info with an the changes DmSet
                        return (batchInfo, changesStatistics);

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
        }

        /// <summary>
        /// Enumerate all internal changes, no batch mode
        /// </summary>
        internal async Task<(BatchInfo, ChangesStatistics)> EnumerateChangesInBatchesInternal(SyncContext context, ScopeInfo scopeInfo)
        {
            Logger.Current.Info($"----- Enumerating Changes for Scope \"{scopeInfo.Name}\" -----");
            Logger.Current.Info("");
            Logger.Current.Info("");
            var configuration = GetCacheConfiguration();

            // memory size total
            double memorySizeFromDmRows = 0L;

            int batchIndex = 0;

            DmSerializer serializer = new DmSerializer();

            // this batch info won't be in memory, it will be be batched
            BatchInfo batchInfo = new BatchInfo();
            // directory where all files will be stored
            batchInfo.Directory = BatchInfo.GenerateNewDirectoryName();
            // not in memory since we serialized all files in the tmp directory
            batchInfo.InMemory = false;

            // Create stats object to store changes count
            ChangesStatistics changesStatistics = new ChangesStatistics();

            using (var connection = this.CreateConnection())
            {
                try
                {
                    // Open the connection
                    await connection.OpenAsync();

                    using (var transaction = connection.BeginTransaction())
                    {
                        // create the in memory changes set
                        DmSet changesSet = new DmSet(configuration.ScopeSet.DmSetName);

                        foreach (var tableDescription in configuration.ScopeSet.Tables)
                        {
                            var builder = this.GetDatabaseBuilder(tableDescription);
                            var syncAdapter = builder.CreateSyncAdapter(connection, transaction);
                            syncAdapter.ConflictApplyAction = configuration.GetApplyAction();

                            Logger.Current.Info($"----- Table \"{tableDescription.TableName}\" -----");

                            // Get Command
                            DbCommand selectIncrementalChangesCommand;
                            DbCommandType dbCommandType;

                            if (this.CanBeServerProvider && context.Parameters != null && context.Parameters.Count > 0 && configuration.Filters != null && configuration.Filters.Count > 0)
                            {
                                var filtersName = configuration.Filters
                                                .Where(f => f.TableName.Equals(tableDescription.TableName, StringComparison.InvariantCultureIgnoreCase))
                                                .Select(f => f.ColumnName);

                                if (filtersName != null && filtersName.Count() > 0)
                                {
                                    dbCommandType = DbCommandType.SelectChangesWitFilters;
                                    selectIncrementalChangesCommand = syncAdapter.GetCommand(dbCommandType, filtersName);
                                }
                                else
                                {
                                    dbCommandType = DbCommandType.SelectChanges;
                                    selectIncrementalChangesCommand = syncAdapter.GetCommand(dbCommandType);
                                }
                            }
                            else
                            {
                                dbCommandType = DbCommandType.SelectChanges;
                                selectIncrementalChangesCommand = syncAdapter.GetCommand(dbCommandType);
                            }

                            // Deriving Parameters
                            syncAdapter.SetCommandParameters(DbCommandType.SelectChanges, selectIncrementalChangesCommand);

                            if (selectIncrementalChangesCommand == null)
                            {
                                var exc = "Missing command 'SelectIncrementalChangesCommand' ";
                                Logger.Current.Error(exc);
                                throw new Exception(exc);
                            }

                            var dmTable = BuildChangesTable(tableDescription.TableName);

                            try
                            {
                                // Set the parameters
                                DbManager.SetParameterValue(selectIncrementalChangesCommand, "sync_min_timestamp", scopeInfo.LastTimestamp);
                                DbManager.SetParameterValue(selectIncrementalChangesCommand, "sync_scope_id", scopeInfo.Id);
                                DbManager.SetParameterValue(selectIncrementalChangesCommand, "sync_scope_is_new", (scopeInfo.IsNewScope ? 1 : 0));

                                // Set filter parameters if any
                                // Only on server side
                                if (this.CanBeServerProvider && context.Parameters != null && context.Parameters.Count > 0 && configuration.Filters != null && configuration.Filters.Count > 0)
                                {
                                    var filters = configuration.Filters.Where(f => f.TableName.Equals(tableDescription.TableName, StringComparison.InvariantCultureIgnoreCase)).ToList();

                                    if (filters != null && filters.Count > 0)
                                    {
                                        foreach (var filter in filters)
                                        {
                                            var parameter = context.Parameters.FirstOrDefault(p => p.ColumnName.Equals(filter.ColumnName, StringComparison.InvariantCultureIgnoreCase) && p.TableName.Equals(filter.TableName, StringComparison.InvariantCultureIgnoreCase));

                                            if (parameter != null)
                                                DbManager.SetParameterValue(selectIncrementalChangesCommand, parameter.ColumnName, parameter.Value);
                                        }
                                    }
                                }

                                this.AddTrackingColumns<int>(dmTable, "sync_row_is_tombstone");

                                // Statistics
                                SelectedChanges selectedChanges = new SelectedChanges();
                                selectedChanges.TableName = tableDescription.TableName;

                                // Get the reader
                                using (var dataReader = selectIncrementalChangesCommand.ExecuteReader())
                                {
                                    while (dataReader.Read())
                                    {
                                        DmRow dmRow = CreateRowFromReader(dataReader, dmTable);

                                        DmRowState state = DmRowState.Unchanged;

                                        state = GetStateFromDmRow(dmRow, scopeInfo);

                                        // If the row is not deleted inserted or modified, go next
                                        if (state != DmRowState.Deleted && state != DmRowState.Modified && state != DmRowState.Added)
                                            continue;

                                        var fieldsSize = DmTableSurrogate.GetRowSizeFromDataRow(dmRow);
                                        var dmRowSize = fieldsSize / 1024d;

                                        if (dmRowSize > configuration.DownloadBatchSizeInKB)
                                        {
                                            var exc = $"Row is too big ({dmRowSize} kb.) for the current Configuration.DownloadBatchSizeInKB ({configuration.DownloadBatchSizeInKB} kb.) Aborting Sync...";
                                            Logger.Current.Error(exc);
                                            throw new Exception(exc);
                                        }

                                        // Calculate the new memory size
                                        memorySizeFromDmRows = memorySizeFromDmRows + dmRowSize;

                                        // add row
                                        dmTable.Rows.Add(dmRow);
                                        selectedChanges.TotalChanges++;

                                        // acceptchanges before modifying 
                                        dmRow.AcceptChanges();

                                        // Set the correct state to be applied
                                        if (state == DmRowState.Deleted)
                                        {
                                            dmRow.Delete();
                                            selectedChanges.Deletes++;
                                        }
                                        else if (state == DmRowState.Added)
                                        {
                                            dmRow.SetAdded();
                                            selectedChanges.Inserts++;
                                        }
                                        else if (state == DmRowState.Modified)
                                        {
                                            dmRow.SetModified();
                                            selectedChanges.Updates++;
                                        }

                                        // We exceed the memorySize, so we can add it to a batch
                                        if (memorySizeFromDmRows > configuration.DownloadBatchSizeInKB)
                                        {
                                            // Since we dont need this column anymore, remove it
                                            this.RemoveTrackingColumns(dmTable, "sync_row_is_tombstone");

                                            changesSet.Tables.Add(dmTable);

                                            // generate the batch part info
                                            batchInfo.GenerateBatchInfo(batchIndex, changesSet, configuration.BatchDirectory);

                                            // increment batch index
                                            batchIndex++;

                                            changesSet.Clear();

                                            // Recreate an empty DmSet, then a dmTable clone
                                            changesSet = new DmSet(configuration.ScopeSet.DmSetName);
                                            dmTable = dmTable.Clone();
                                            this.AddTrackingColumns<int>(dmTable, "sync_row_is_tombstone");

                                            // Init the row memory size
                                            memorySizeFromDmRows = 0L;

                                            // raise SyncProgress Event
                                            var existSelectedChanges = changesStatistics.SelectedChanges.FirstOrDefault(sc => string.Equals(sc.TableName, tableDescription.TableName));
                                            if (existSelectedChanges == null)
                                            {
                                                existSelectedChanges = selectedChanges;
                                                changesStatistics.SelectedChanges.Add(selectedChanges);
                                            }
                                            else
                                            {
                                                existSelectedChanges.Deletes += selectedChanges.Deletes;
                                                existSelectedChanges.Inserts += selectedChanges.Inserts;
                                                existSelectedChanges.Updates += selectedChanges.Updates;
                                                existSelectedChanges.TotalChanges += selectedChanges.TotalChanges;
                                            }
                                            // Event progress
                                            var progEventArgs = new SyncProgressEventArgs
                                            {
                                                ProviderTypeName = this.ProviderTypeName,
                                                Context = context,
                                                Action = ChangeApplicationAction.Continue
                                            };
                                            progEventArgs.ChangesStatistics = changesStatistics;

                                            this.SyncProgress?.Invoke(this, progEventArgs);

                                            if (progEventArgs.Action == ChangeApplicationAction.Rollback)
                                                throw SyncException.CreateRollbackException(context.SyncStage);

                                            // reinit stats 
                                            selectedChanges = new SelectedChanges();
                                            selectedChanges.TableName = tableDescription.TableName;

                                            // TODO : Rollback possible here ?
                                            if (progEventArgs.Action == ChangeApplicationAction.Rollback)
                                            {
                                                // ?
                                            }
                                        }
                                    }

                                    // Since we dont need this column anymore, remove it
                                    this.RemoveTrackingColumns(dmTable, "sync_row_is_tombstone");

                                    context.SyncStage = SyncStage.SelectedChanges;

                                    changesSet.Tables.Add(dmTable);

                                    // Init the row memory size
                                    memorySizeFromDmRows = 0L;

                                    // raise SyncProgress Event
                                    var esc = changesStatistics.SelectedChanges.FirstOrDefault(sc => string.Equals(sc.TableName, tableDescription.TableName));
                                    if (esc == null)
                                    {
                                        esc = selectedChanges;
                                        changesStatistics.SelectedChanges.Add(esc);
                                    }
                                    else
                                    {
                                        esc.Deletes += selectedChanges.Deletes;
                                        esc.Inserts += selectedChanges.Inserts;
                                        esc.Updates += selectedChanges.Updates;
                                        esc.TotalChanges += selectedChanges.TotalChanges;
                                    }
                                    // Event progress
                                    var progressEventArgs = new SyncProgressEventArgs
                                    {
                                        ProviderTypeName = this.ProviderTypeName,
                                        Context = context,
                                        Action = ChangeApplicationAction.Continue
                                    };
                                    progressEventArgs.ChangesStatistics = changesStatistics;

                                    this.SyncProgress?.Invoke(this, progressEventArgs);

                                    if (progressEventArgs.Action == ChangeApplicationAction.Rollback)
                                        throw SyncException.CreateRollbackException(context.SyncStage);
                                }
                            }
                            catch (Exception dbException)
                            {
                                Logger.Current.Error($"Caught exception while enumerating changes\n{dbException}\n");
                                throw;
                            }
                            finally
                            {

                                Logger.Current.Info($"--- End Table \"{tableDescription.TableName}\" ---");
                                Logger.Current.Info("");
                            }
                        }

                        // We are in batch mode, and we are at the last batchpart info
                        var batchPartInfo = batchInfo.GenerateBatchInfo(batchIndex, changesSet, configuration.BatchDirectory);

                        if (batchPartInfo != null)
                            batchPartInfo.IsLastBatch = true;

                        transaction.Commit();
                    }

                }
                catch (Exception)
                {
                    throw;
                }
                finally
                {
                    if (connection != null && connection.State == ConnectionState.Open)
                        connection.Close();
                }


            }
            Logger.Current.Info($"--- End Enumerating Changes for Scope \"{scopeInfo.Name}\" ---");
            Logger.Current.Info("");

            return (batchInfo, changesStatistics);
        }

        /// <summary>
        /// Create a DmRow from a IDataReader
        /// </summary>
        private DmRow CreateRowFromReader(IDataReader dataReader, DmTable dmTable)
        {
            // we have an insert / update or delete
            DmRow dataRow = dmTable.NewRow();

            for (int i = 0; i < dataReader.FieldCount; i++)
            {
                var columnName = dataReader.GetName(i);
                var dmRowObject = dataReader.GetValue(i);

                if (dmRowObject != DBNull.Value)
                {
                    if (dmRowObject != null)
                    {
                        var columnType = dmTable.Columns[columnName].DataType;
                        var dmRowObjectType = dmRowObject.GetType();

                        if (dmRowObjectType != columnType && columnType != typeof(object))
                        {
                            if (columnType == typeof(Guid) && (dmRowObject as string) != null)
                                dmRowObject = new Guid(dmRowObject.ToString());
                            else if (columnType == typeof(Int32) && dmRowObjectType != typeof(Int32))
                                dmRowObject = Convert.ToInt32(dmRowObject);
                            else if (columnType == typeof(UInt32) && dmRowObjectType != typeof(UInt32))
                                dmRowObject = Convert.ToUInt32(dmRowObject);
                            else if (columnType == typeof(Int16) && dmRowObjectType != typeof(Int16))
                                dmRowObject = Convert.ToInt16(dmRowObject);
                            else if (columnType == typeof(UInt16) && dmRowObjectType != typeof(UInt16))
                                dmRowObject = Convert.ToUInt16(dmRowObject);
                            else if (columnType == typeof(Int64) && dmRowObjectType != typeof(Int64))
                                dmRowObject = Convert.ToInt64(dmRowObject);
                            else if (columnType == typeof(UInt64) && dmRowObjectType != typeof(UInt64))
                                dmRowObject = Convert.ToUInt64(dmRowObject);
                            else if (columnType == typeof(Byte) && dmRowObjectType != typeof(Byte))
                                dmRowObject = Convert.ToByte(dmRowObject);
                            else if (columnType == typeof(Char) && dmRowObjectType != typeof(Char))
                                dmRowObject = Convert.ToChar(dmRowObject);
                            else if (columnType == typeof(DateTime) && dmRowObjectType != typeof(DateTime))
                                dmRowObject = Convert.ToDateTime(dmRowObject);
                            else if (columnType == typeof(Decimal) && dmRowObjectType != typeof(Decimal))
                                dmRowObject = Convert.ToDecimal(dmRowObject);
                            else if (columnType == typeof(Double) && dmRowObjectType != typeof(Double))
                                dmRowObject = Convert.ToDouble(dmRowObject);
                            else if (columnType == typeof(SByte) && dmRowObjectType != typeof(SByte))
                                dmRowObject = Convert.ToSByte(dmRowObject);
                            else if (columnType == typeof(Single) && dmRowObjectType != typeof(Single))
                                dmRowObject = Convert.ToSingle(dmRowObject);
                            else if (columnType == typeof(String) && dmRowObjectType != typeof(String))
                                dmRowObject = Convert.ToString(dmRowObject);
                            else if (columnType == typeof(Boolean) && dmRowObjectType != typeof(Boolean))
                                dmRowObject = Convert.ToBoolean(dmRowObject);
                            else if (dmRowObjectType != columnType)
                            {
                                var t = dmRowObject.GetType();
                                var converter = columnType.GetConverter();
                                if (converter != null && converter.CanConvertFrom(t))
                                    dmRowObject = converter.ConvertFrom(dmRowObject);
                            }
                        }
                    }
                    dataRow[columnName] = dmRowObject;
                }
            }

            return dataRow;
        }

        /// <summary>
        /// Get a DmRow state to know is we have an inserted, updated, or deleted row to apply
        /// </summary>
        private DmRowState GetStateFromDmRow(DmRow dataRow, ScopeInfo scopeInfo)
        {
            DmRowState dmRowState = DmRowState.Unchanged;

            var isTombstone = Convert.ToInt64(dataRow["sync_row_is_tombstone"]) > 0;

            if (isTombstone)
                dmRowState = DmRowState.Deleted;
            else
            {
                var createdTimeStamp = DbManager.ParseTimestamp(dataRow["create_timestamp"]);
                var updatedTimeStamp = DbManager.ParseTimestamp(dataRow["update_timestamp"]);
                var isLocallyCreated = dataRow["create_scope_id"] == DBNull.Value || dataRow["create_scope_id"] == null;
                var islocallyUpdated = dataRow["update_scope_id"] == DBNull.Value || dataRow["update_scope_id"] == null || (Guid)dataRow["update_scope_id"] != scopeInfo.Id;


                //if (scopeInfo.IsNewScope || (isLocallyCreated && createdTimeStamp > scopeInfo.LastTimestamp))
                //    dmRowState = DmRowState.Added;
                //else
                //    dmRowState = DmRowState.Modified;

                // Check if a row is modified :
                // 1) Row is not new
                // 2) Row update is AFTER last sync of asker
                // 3) Row insert is BEFORE last sync of asker (if insert is after last sync, it's not an update, it's an insert)
                if (!scopeInfo.IsNewScope && islocallyUpdated && updatedTimeStamp > scopeInfo.LastTimestamp && (createdTimeStamp < scopeInfo.LastTimestamp || !isLocallyCreated))
                    dmRowState = DmRowState.Modified;
                else if (scopeInfo.IsNewScope || (isLocallyCreated && createdTimeStamp > scopeInfo.LastTimestamp))
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
        /// Apply changes : Insert / Updates Delete
        /// the fromScope is local client scope when this method is called from server
        /// the fromScope is server scope when this method is called from client
        /// </summary>
        public virtual async Task<(SyncContext, ChangesStatistics)> ApplyChangesAsync(SyncContext context, ScopeInfo fromScope, BatchInfo changes)
        {
            try
            {
                ChangeApplicationAction changeApplicationAction;
                DbTransaction applyTransaction = null;
                ChangesStatistics changesStatistics = new ChangesStatistics();
                SyncProgressEventArgs progressEventArgs;

                // just before applying changes
                context.SyncStage = SyncStage.ApplyingChanges;

                // Event progress
                progressEventArgs = new SyncProgressEventArgs
                {
                    ProviderTypeName = this.ProviderTypeName,
                    Action = ChangeApplicationAction.Continue,
                    Context = context,
                    ChangesStatistics = changesStatistics
                };

                this.SyncProgress?.Invoke(this, progressEventArgs);

                if (progressEventArgs.Action == ChangeApplicationAction.Rollback)
                    throw SyncException.CreateRollbackException(context.SyncStage);

                using (var connection = this.CreateConnection())
                {
                    try
                    {
                        await connection.OpenAsync();

                        // Create a transaction
                        applyTransaction = connection.BeginTransaction();

                        Logger.Current.Info($"----- Applying Changes for Scope \"{fromScope.Name}\" -----");
                        Logger.Current.Info("");

                        // -----------------------------------------------------
                        // 1) Applying deletes. Do not apply deletes if we are in a new database
                        // -----------------------------------------------------
                        if (!fromScope.IsNewScope)
                        {
                            changeApplicationAction = this.ApplyChangesInternal(context, connection, applyTransaction, fromScope, changes, DmRowState.Deleted, changesStatistics);

                            // Rollback
                            if (changeApplicationAction == ChangeApplicationAction.Rollback)
                                throw SyncException.CreateRollbackException(context.SyncStage);
                        }

                        // -----------------------------------------------------
                        // 1) Applying Inserts
                        // -----------------------------------------------------
                        changeApplicationAction = this.ApplyChangesInternal(context, connection, applyTransaction, fromScope, changes, DmRowState.Added, changesStatistics);

                        // Rollback
                        if (changeApplicationAction == ChangeApplicationAction.Rollback)
                            throw SyncException.CreateRollbackException(context.SyncStage);

                        // -----------------------------------------------------
                        // 1) Applying updates
                        // -----------------------------------------------------
                        changeApplicationAction = this.ApplyChangesInternal(context, connection, applyTransaction, fromScope, changes, DmRowState.Modified, changesStatistics);

                        // Rollback
                        if (changeApplicationAction == ChangeApplicationAction.Rollback)
                            throw SyncException.CreateRollbackException(context.SyncStage);

                        // Insert / Delete / Update applied, so change stage
                        context.SyncStage = SyncStage.AppliedChanges;

                        // Event progress on applied change
                        progressEventArgs = new SyncProgressEventArgs
                        {
                            ProviderTypeName = this.ProviderTypeName,
                            Action = ChangeApplicationAction.Continue,
                            Context = context,
                            ChangesStatistics = changesStatistics
                        };

                        this.SyncProgress?.Invoke(this, progressEventArgs);

                        if (progressEventArgs.Action == ChangeApplicationAction.Rollback)
                            throw SyncException.CreateRollbackException(context.SyncStage);

                        applyTransaction.Commit();

                        Logger.Current.Info($"--- End Applying Changes for Scope \"{fromScope.Name}\" ---");
                        Logger.Current.Info("");

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
                    return (context, changesStatistics);
                }
            }
            catch (Exception)
            {
                throw;
            }
        }

        /// <summary>
        /// Apply changes internal method for one Insert or Update or Delete for every dbSyncAdapter
        /// </summary>
        internal ChangeApplicationAction ApplyChangesInternal(SyncContext context, DbConnection connection, DbTransaction transaction, ScopeInfo fromScope, BatchInfo changes, DmRowState applyType, ChangesStatistics changesStatistics)
        {
            ChangeApplicationAction changeApplicationAction = ChangeApplicationAction.Continue;

            var configuration = GetCacheConfiguration();

            // Set the good stage
            switch (applyType)
            {
                case DmRowState.Added:
                    context.SyncStage = SyncStage.ApplyingInserts;
                    break;
                case DmRowState.Modified:
                    context.SyncStage = SyncStage.ApplyingUpdates;
                    break;
                case DmRowState.Deleted:
                    context.SyncStage = SyncStage.ApplyingDeletes;
                    break;
            }

            // Event progress
            var progressEventArgs = new SyncProgressEventArgs
            {
                ProviderTypeName = this.ProviderTypeName,
                Action = ChangeApplicationAction.Continue,
                Context = context,
                ChangesStatistics = changesStatistics
            };

            // for each adapters (Zero to End for Insert / Updates -- End to Zero for Deletes
            for (int i = 0; i < configuration.ScopeSet.Tables.Count; i++)
            {
                try
                {
                    // If we have a delete we must go from Up to Down, orthewise Dow to Up index
                    var tableDescription = (applyType != DmRowState.Deleted ?
                            configuration.ScopeSet.Tables[i] :
                            configuration.ScopeSet.Tables[configuration.ScopeSet.Tables.Count - i - 1]);

                    var builder = this.GetDatabaseBuilder(tableDescription);
                    var syncAdapter = builder.CreateSyncAdapter(connection, transaction);
                    syncAdapter.ConflictApplyAction = configuration.GetApplyAction();

                    // Set syncAdapter properties
                    syncAdapter.applyType = applyType;

                    if (syncAdapter.ConflictActionInvoker == null && this.ApplyChangedFailed != null)
                        syncAdapter.ConflictActionInvoker = GetConflictAction;

                    Logger.Current.Info($"----- Operation {applyType.ToString()} for Table \"{tableDescription.TableName}\" -----");


                    if (changes.BatchPartsInfo != null && changes.BatchPartsInfo.Count > 0)
                    {
                        // getting the table to be applied
                        // we may have multiple batch files, so we iterate
                        foreach (var dmTable in changes.GetTable(tableDescription.TableName))
                        {
                            if (dmTable == null || dmTable.Rows.Count == 0)
                                continue;

                            // check and filter
                            var dmChangesView = new DmView(dmTable, (r) => r.RowState == applyType);

                            if (dmChangesView.Count == 0)
                            {
                                Logger.Current.Info($"0 {applyType.ToString()} Applied");
                                Logger.Current.Info($"--- End {applyType.ToString()} for Table \"{tableDescription.TableName}\" ---");
                                Logger.Current.Info($"");
                                continue;
                            }

                            // Conflicts occured when trying to apply rows
                            List<SyncConflict> conflicts = new List<SyncConflict>();

                            int rowsApplied;
                            // applying the bulkchanges command
                            if (configuration.UseBulkOperations && this.SupportBulkOperations)
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

                                    if (changeApplicationAction == ChangeApplicationAction.Continue)
                                    {
                                        // row resolved
                                        if (resolvedRow != null)
                                            rowsApplied++;
                                    }
                                    else
                                    {
                                        context.TotalSyncErrors++;
                                        // TODO : Should we break at the first error ?
                                        return ChangeApplicationAction.Rollback;
                                    }
                                }
                            }

                            // Get all conflicts resolved
                            context.TotalSyncConflicts = conflicts.Where(c => c.Type != ConflictType.ErrorsOccurred).Sum(c => 1);

                            // Handle sync progress for this syncadapter (so this table)
                            var changedFailed = dmChangesView.Count - rowsApplied;

                            // raise SyncProgress Event
                            AppliedChanges appliedChanges = new AppliedChanges();
                            appliedChanges.TableName = tableDescription.TableName;
                            appliedChanges.ChangesApplied = rowsApplied;
                            appliedChanges.ChangesFailed = changedFailed;
                            appliedChanges.State = applyType;

                            var existAppliedChanges = changesStatistics.AppliedChanges.FirstOrDefault(
                                    sc => string.Equals(sc.TableName, tableDescription.TableName) && sc.State == applyType);

                            if (existAppliedChanges == null)
                            {
                                existAppliedChanges = appliedChanges;
                                changesStatistics.AppliedChanges.Add(existAppliedChanges);
                            }
                            else
                            {
                                existAppliedChanges.ChangesApplied += appliedChanges.ChangesApplied;
                                existAppliedChanges.ChangesFailed += appliedChanges.ChangesFailed;
                            }

                            this.SyncProgress?.Invoke(this, progressEventArgs);

                            if (progressEventArgs.Action == ChangeApplicationAction.Rollback)
                                break;

                        }
                    }

                    Logger.Current.Info("");
                    Logger.Current.Info($"--- End {applyType.ToString()} for Table \"{tableDescription.TableName}\" ---");
                    Logger.Current.Info("");

                }
                catch (Exception ex)
                {
                    Logger.Current.Error($"Error during ApplyInternalChanges : {ex.Message}");
                    throw;
                }
            }

            // Check action
            changeApplicationAction = progressEventArgs.Action;

            if (changeApplicationAction == ChangeApplicationAction.Rollback)
                return ChangeApplicationAction.Rollback;


            return ChangeApplicationAction.Continue;
        }

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
        /// A conflict has occured, we try to ask for the solution to the user
        /// </summary>
        internal ApplyAction GetConflictAction(SyncConflict conflict, DbConnection connection, DbTransaction transaction = null)
        {
            Logger.Current.Debug("Raising Apply Change Failed Event");
            var configuration = GetCacheConfiguration();

            var dbApplyChangeFailedEventArg = new ApplyChangeFailedEventArgs(conflict, configuration.GetApplyAction(), connection, transaction);

            this.ApplyChangedFailed?.Invoke(this, dbApplyChangeFailedEventArg);

            ApplyAction action = dbApplyChangeFailedEventArg.Action;
            Logger.Current.Debug($"Action: {action.ToString()}");
            return action;
        }

        public void SetCancellationToken(CancellationToken token)
        {
            this.cancellationToken = token;
        }
    }
}
