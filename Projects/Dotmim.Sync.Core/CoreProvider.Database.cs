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
using System.Diagnostics;
using System.Text;

namespace Dotmim.Sync
{
    /// <summary>
    /// Core provider : should be implemented by any server / client provider
    /// </summary>
    public abstract partial class CoreProvider
    {
        /// <summary>
        /// Deprovision a database. You have to passe a configuration object, containing at least the dmTables
        /// </summary>
        public async Task DeprovisionAsync(string[] tables, SyncProvision provision)
        {
            if (tables == null || tables.Length == 0)
                throw new SyncException("You must set the tables you want to modify");

            // Load the configuration
            var configuration = await this.ReadConfigurationAsync(tables);

            // Open the connection
            using (var connection = this.CreateConnection())
            {
                try
                {
                    await connection.OpenAsync();

                    using (var transaction = connection.BeginTransaction())
                    {
                        for (int i = configuration.Count - 1; i >= 0; i--)
                        {
                            // Get the table
                            var dmTable = configuration[i];

                            // get the builder
                            var builder = GetDatabaseBuilder(dmTable);

                            // adding filters
                            this.AddFilters(configuration, dmTable, builder);

                            if (provision.HasFlag(SyncProvision.TrackingTable) || provision == SyncProvision.All)
                                builder.DropTrackingTable(connection, transaction);

                            if (provision.HasFlag(SyncProvision.StoredProcedures) || provision == SyncProvision.All)
                                builder.DropProcedures(connection, transaction);

                            if (provision.HasFlag(SyncProvision.Triggers) || provision == SyncProvision.All)
                                builder.DropTriggers(connection, transaction);

                            if (provision.HasFlag(SyncProvision.Table) || provision == SyncProvision.All)
                                builder.DropTable(connection, transaction);
                        }


                        transaction.Commit();
                    }

                }
                catch (Exception ex)
                {
                    throw SyncException.CreateUnknowException(SyncStage.BeginSession, ex);
                }
                finally
                {
                    if (connection.State != ConnectionState.Closed)
                        connection.Close();
                }
            }

        }

        /// <summary>
        /// Deprovision a database
        /// </summary>
        public async Task ProvisionAsync(string[] tables, SyncProvision provision)
        {
            if (tables == null || tables.Length == 0)
                throw new SyncException("You must set the tables you want to modify");

            // Load the configuration
            var configuration = await this.ReadConfigurationAsync(tables);

            // Open the connection
            using (var connection = this.CreateConnection())
            {
                try
                {
                    await connection.OpenAsync();

                    using (var transaction = connection.BeginTransaction())
                    {
                        for (int i = 0; i < configuration.Count; i++)
                        {
                            // Get the table
                            var dmTable = configuration[i];

                            // get the builder
                            var builder = GetDatabaseBuilder(dmTable);

                            // adding filters
                            this.AddFilters(configuration, dmTable, builder);

                            if (provision.HasFlag(SyncProvision.Table))
                                builder.CreateTable(connection, transaction);

                            if (provision.HasFlag(SyncProvision.TrackingTable))
                                builder.CreateTrackingTable(connection, transaction);

                            if (provision.HasFlag(SyncProvision.Triggers))
                                builder.CreateTriggers(connection, transaction);

                            if (provision.HasFlag(SyncProvision.StoredProcedures))
                                builder.CreateStoredProcedures(connection, transaction);

                        }
                        transaction.Commit();
                    }

                }
                catch (Exception ex)
                {
                    throw SyncException.CreateUnknowException(SyncStage.BeginSession, ex);
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
        public virtual async Task<SyncContext> EnsureDatabaseAsync(SyncContext context, ScopeInfo scopeInfo)
        {
            var configuration = GetCacheConfiguration();

            // Event progress
            context.SyncStage = SyncStage.DatabaseApplying;
            DatabaseApplyingEventArgs beforeArgs =
                new DatabaseApplyingEventArgs(this.ProviderTypeName, context.SyncStage, configuration);
            this.TryRaiseProgressEvent(beforeArgs, this.DatabaseApplying);

            // if config has been editer by user in event, save again
            this.SetCacheConfiguration(configuration);

            // If scope exists and lastdatetime sync is present, so database exists
            // Check if we don't have an OverwriteConfiguration (if true, we force the check)
            if (scopeInfo.LastSync.HasValue && !beforeArgs.OverwriteConfiguration)
                return context;

            StringBuilder script = new StringBuilder();

            // Open the connection
            using (var connection = this.CreateConnection())
            {
                try
                {
                    await connection.OpenAsync();

                    using (var transaction = connection.BeginTransaction())
                    {
                        foreach (var dmTable in configuration)
                        {
                            var builder = GetDatabaseBuilder(dmTable);

                            // adding filter
                            this.AddFilters(configuration, dmTable, builder);

                            context.SyncStage = SyncStage.DatabaseTableApplying;
                            DatabaseTableApplyingEventArgs beforeTableArgs =
                                new DatabaseTableApplyingEventArgs(this.ProviderTypeName, context.SyncStage, dmTable.TableName);
                            this.TryRaiseProgressEvent(beforeTableArgs, this.DatabaseTableApplying);

                            string currentScript = null;
                            if (beforeArgs.GenerateScript)
                            {
                                currentScript = builder.ScriptTable(connection, transaction);
                                currentScript += builder.ScriptForeignKeys(connection, transaction);
                                script.Append(currentScript);
                            }

                            builder.Create(connection, transaction);
                            builder.CreateForeignKeys(connection, transaction);

                            context.SyncStage = SyncStage.DatabaseTableApplied;
                            DatabaseTableAppliedEventArgs afterTableArgs =
                                new DatabaseTableAppliedEventArgs(this.ProviderTypeName, context.SyncStage, dmTable.TableName, currentScript);
                            this.TryRaiseProgressEvent(afterTableArgs, this.DatabaseTableApplied);
                        }

                        context.SyncStage = SyncStage.DatabaseApplied;
                        var afterArgs = new DatabaseAppliedEventArgs(this.ProviderTypeName, context.SyncStage, script.ToString());
                        this.TryRaiseProgressEvent(afterArgs, this.DatabaseApplied);

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
        /// Adding filters to an existing configuration
        /// </summary>
        private void AddFilters(SyncConfiguration configuration, DmTable dmTable, DbBuilder builder)
        {
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

        }

    }
}
