
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;
using System.Linq;
using System.Data;
using System.Diagnostics;
using Dotmim.Sync.Enumerations;
using System.Threading.Tasks;

namespace Dotmim.Sync.Builders
{
    public abstract partial class DbTableBuilder
    {

        /// <summary>
        /// Gets the table description for the current DbBuilder
        /// </summary>
        public SyncTable TableDescription { get; set; }


        /// <summary>
        /// Gets or Sets Setup, containing naming prefix and suffix if needed
        /// </summary>
        public SyncSetup Setup { get; }

        /// <summary>
        /// Filtered Columns
        /// </summary>
        public SyncFilter Filter { get; set; }

        /// <summary>
        /// Gets or Sets if the Database builder supports bulk procedures
        /// </summary>
        public bool UseBulkProcedures { get; set; } = true;

        /// <summary>
        /// Gets or Sets if the Database builder shoud use change tracking
        /// </summary>
        public bool UseChangeTracking { get; set; } = false;

        /// <summary>
        /// Gets the table parsed name
        /// </summary>
        public ParserName TableName { get; }

        /// <summary>
        /// Gets the tracking table parsed name
        /// </summary>
        public ParserName TrackingTableName { get; }

        /// <summary>
        /// You have to provide a proc builder implementation for your current database
        /// </summary>
        public abstract IDbBuilderProcedureHelper CreateProcBuilder(DbConnection connection, DbTransaction transaction = null);

        /// <summary>
        /// You have to provide a trigger builder implementation for your current database
        /// </summary>
        public abstract IDbBuilderTriggerHelper CreateTriggerBuilder(DbConnection connection, DbTransaction transaction = null);

        /// <summary>
        /// You have to provide a table builder implementation for your current database
        /// </summary>
        public abstract IDbBuilderTableHelper CreateTableBuilder(DbConnection connection, DbTransaction transaction = null);

        /// <summary>
        /// You have to provider a tracking table builder implementation for your current database
        /// </summary>
        public abstract IDbBuilderTrackingTableHelper CreateTrackingTableBuilder(DbConnection connection, DbTransaction transaction = null);

        /// <summary>
        /// Gets the table Sync Adapter in charge of executing all command during sync
        /// </summary>
        public abstract DbSyncAdapter CreateSyncAdapter(DbConnection connection, DbTransaction transaction = null);


        /// <summary>
        /// Create table name and tracking name, accordingly to the setup
        /// </summary>
        public abstract (ParserName tableName, ParserName trackingName) GetParsers(SyncTable tableDescription, SyncSetup setup);

        /// <summary>
        /// Construct a DbBuilder
        /// </summary>
        public DbTableBuilder(SyncTable tableDescription, SyncSetup setup)
        {
            this.TableDescription = tableDescription;
            this.Setup = setup;

            (this.TableName, this.TrackingTableName) = GetParsers(tableDescription, setup);
        }

        /// <summary>
        /// Apply config.
        /// Create relations if needed
        /// </summary>
        public async Task CreateForeignKeysAsync(DbConnection connection, DbTransaction transaction = null)
        {
            if (TableDescription.PrimaryKeys.Count <= 0)
                throw new MissingPrimaryKeyException(TableDescription.TableName);

            var alreadyOpened = connection.State != ConnectionState.Closed;

            if (!alreadyOpened)
                await connection.OpenAsync().ConfigureAwait(false);

            var tableBuilder = CreateTableBuilder(connection, transaction);

            // Get all parent table and create the foreign key on it
            foreach (var constraint in this.TableDescription.GetRelations())
            {
                // Check if we need to create the foreign key constraint
                if (await tableBuilder.NeedToCreateForeignKeyConstraintsAsync(constraint).ConfigureAwait(false))
                {
                    await tableBuilder.CreateForeignKeyConstraintsAsync(constraint).ConfigureAwait(false);
                }
            }

            if (!alreadyOpened)
                connection.Close();


        }

        public async Task RenameTrackingTableAsync(ParserName oldTableName, DbConnection connection, DbTransaction transaction = null)
        {
            if (TableDescription.PrimaryKeys.Count <= 0)
                throw new MissingPrimaryKeyException(TableDescription.TableName);

            var alreadyOpened = connection.State != ConnectionState.Closed;

            if (!alreadyOpened)
                await connection.OpenAsync().ConfigureAwait(false);

            var trackingTableBuilder = CreateTrackingTableBuilder(connection, transaction);

            // be sure the table actually exists
            var hasbeenCreated = await CreateTrackingTableAsync(connection, transaction);

            if (!hasbeenCreated)
                await trackingTableBuilder.RenameTableAsync(oldTableName);

            if (!alreadyOpened)
                connection.Close();
        }

        public async Task<bool> CreateTrackingTableAsync(DbConnection connection, DbTransaction transaction = null)
        {
            if (TableDescription.PrimaryKeys.Count <= 0)
                throw new MissingPrimaryKeyException(TableDescription.TableName);

            var alreadyOpened = connection.State != ConnectionState.Closed;

            if (!alreadyOpened)
                await connection.OpenAsync().ConfigureAwait(false);


            var hasBeenCreated = false;

            var trackingTableBuilder = CreateTrackingTableBuilder(connection, transaction);
            var tableBuilder = CreateTableBuilder(connection, transaction);

            if (await trackingTableBuilder.NeedToCreateTrackingTableAsync().ConfigureAwait(false))
            {
                if (await tableBuilder.NeedToCreateSchemaAsync().ConfigureAwait(false))
                    await tableBuilder.CreateSchemaAsync().ConfigureAwait(false);

                await trackingTableBuilder.CreateTableAsync().ConfigureAwait(false);
                await trackingTableBuilder.CreatePkAsync().ConfigureAwait(false);
                await trackingTableBuilder.CreateIndexAsync().ConfigureAwait(false);
                hasBeenCreated = true;
            }

            if (!alreadyOpened)
                connection.Close();

            return hasBeenCreated;
        }

        public async Task CreateStoredProceduresAsync(DbConnection connection, DbTransaction transaction = null)
        {
            if (TableDescription.PrimaryKeys.Count <= 0)
                throw new MissingPrimaryKeyException(TableDescription.TableName);

            // Check if we have mutables columns
            var hasMutableColumns = TableDescription.GetMutableColumns(false).Any();

            var alreadyOpened = connection.State != ConnectionState.Closed;

            if (!alreadyOpened)
                await connection.OpenAsync().ConfigureAwait(false);

            // could be null
            var procBuilder = CreateProcBuilder(connection, transaction);
            if (procBuilder == null)
                return;

            if (await procBuilder.NeedToCreateProcedureAsync(DbCommandType.SelectChanges).ConfigureAwait(false))
                await procBuilder.CreateSelectIncrementalChangesAsync(this.Filter).ConfigureAwait(false);

            if (await procBuilder.NeedToCreateProcedureAsync(DbCommandType.SelectInitializedChanges).ConfigureAwait(false))
                await procBuilder.CreateSelectInitializedChangesAsync(this.Filter).ConfigureAwait(false);

            if (await procBuilder.NeedToCreateProcedureAsync(DbCommandType.SelectRow).ConfigureAwait(false))
                await procBuilder.CreateSelectRowAsync().ConfigureAwait(false);

            if (await procBuilder.NeedToCreateProcedureAsync(DbCommandType.UpdateRow).ConfigureAwait(false))
                await procBuilder.CreateUpdateAsync(hasMutableColumns).ConfigureAwait(false);

            if (await procBuilder.NeedToCreateProcedureAsync(DbCommandType.DeleteRow).ConfigureAwait(false))
                await procBuilder.CreateDeleteAsync().ConfigureAwait(false);

            if (await procBuilder.NeedToCreateProcedureAsync(DbCommandType.DeleteMetadata).ConfigureAwait(false))
                await procBuilder.CreateDeleteMetadataAsync().ConfigureAwait(false);

            if (await procBuilder.NeedToCreateProcedureAsync(DbCommandType.Reset).ConfigureAwait(false))
                await procBuilder.CreateResetAsync().ConfigureAwait(false);

            if (this.UseBulkProcedures && await procBuilder.NeedToCreateTypeAsync(DbCommandType.BulkTableType).ConfigureAwait(false))
            {
                await procBuilder.CreateTVPTypeAsync().ConfigureAwait(false);
                await procBuilder.CreateBulkUpdateAsync(hasMutableColumns).ConfigureAwait(false);
                await procBuilder.CreateBulkDeleteAsync().ConfigureAwait(false);
            }

            if (!alreadyOpened)
                connection.Close();

        }

        public async Task CreateTableAsync(DbConnection connection, DbTransaction transaction = null)
        {
            if (TableDescription.Columns.Count <= 0)
                throw new MissingsColumnException(TableDescription.TableName);

            if (TableDescription.PrimaryKeys.Count <= 0)
                throw new MissingPrimaryKeyException(TableDescription.TableName);

            var tableBuilder = CreateTableBuilder(connection, transaction);

            var alreadyOpened = connection.State != ConnectionState.Closed;

            if (!alreadyOpened)
                await connection.OpenAsync().ConfigureAwait(false);

            // Check if we need to create the tables
            if (await tableBuilder.NeedToCreateTableAsync().ConfigureAwait(false))
            {
                if (await tableBuilder.NeedToCreateSchemaAsync().ConfigureAwait(false))
                    await tableBuilder.CreateSchemaAsync().ConfigureAwait(false);

                await tableBuilder.CreateTableAsync().ConfigureAwait(false);
                await tableBuilder.CreatePrimaryKeyAsync().ConfigureAwait(false);
            }

            if (!alreadyOpened)
                connection.Close();

        }

        /// <summary>
        /// Apply the config.
        /// Create the table if needed
        /// </summary>
        public async Task CreateAsync(SyncProvision provision, DbConnection connection, DbTransaction transaction = null)
        {
            if (TableDescription.Columns.Count <= 0)
                throw new MissingsColumnException(TableDescription.TableName);

            if (TableDescription.PrimaryKeys.Count <= 0)
                throw new MissingPrimaryKeyException(TableDescription.TableName);

            var alreadyOpened = connection.State != ConnectionState.Closed;

            if (!alreadyOpened)
                await connection.OpenAsync().ConfigureAwait(false);

            if (provision.HasFlag(SyncProvision.Table))
                await this.CreateTableAsync(connection, transaction).ConfigureAwait(false);

            if (provision.HasFlag(SyncProvision.TrackingTable))
                await this.CreateTrackingTableAsync(connection, transaction).ConfigureAwait(false);

            if (provision.HasFlag(SyncProvision.Triggers))
                await this.CreateTriggersAsync(connection, transaction).ConfigureAwait(false);

            if (provision.HasFlag(SyncProvision.StoredProcedures))
                await this.CreateStoredProceduresAsync(connection, transaction).ConfigureAwait(false);

            if (!alreadyOpened)
                connection.Close();
        }


        public async Task DropStoredProceduresAsync(DbConnection connection, DbTransaction transaction = null)
        {
            var alreadyOpened = connection.State != ConnectionState.Closed;

            if (!alreadyOpened)
                await connection.OpenAsync().ConfigureAwait(false);

            if (!alreadyOpened)
                await connection.OpenAsync().ConfigureAwait(false);

            var procBuilder = CreateProcBuilder(connection, transaction);

            // Could be null
            if (procBuilder == null)
                return;

            if (!await procBuilder.NeedToCreateProcedureAsync(DbCommandType.SelectChanges).ConfigureAwait(false))
                await procBuilder.DropSelectIncrementalChangesAsync(this.Filter).ConfigureAwait(false);
            if (!await procBuilder.NeedToCreateProcedureAsync(DbCommandType.SelectInitializedChanges).ConfigureAwait(false))
                await procBuilder.DropSelectInitializedChangesAsync(this.Filter).ConfigureAwait(false);
            if (!await procBuilder.NeedToCreateProcedureAsync(DbCommandType.SelectRow).ConfigureAwait(false))
                await procBuilder.DropSelectRowAsync().ConfigureAwait(false);
            if (!await procBuilder.NeedToCreateProcedureAsync(DbCommandType.UpdateRow).ConfigureAwait(false))
                await procBuilder.DropUpdateAsync().ConfigureAwait(false);
            if (!await procBuilder.NeedToCreateProcedureAsync(DbCommandType.DeleteRow).ConfigureAwait(false))
                await procBuilder.DropDeleteAsync().ConfigureAwait(false);
            if (!await procBuilder.NeedToCreateProcedureAsync(DbCommandType.DeleteMetadata).ConfigureAwait(false))
                await procBuilder.DropDeleteMetadataAsync().ConfigureAwait(false);
            if (!await procBuilder.NeedToCreateProcedureAsync(DbCommandType.Reset).ConfigureAwait(false))
                await procBuilder.DropResetAsync().ConfigureAwait(false);

            if (this.UseBulkProcedures && !await procBuilder.NeedToCreateTypeAsync(DbCommandType.BulkTableType).ConfigureAwait(false))
            {
                await procBuilder.DropBulkUpdateAsync().ConfigureAwait(false);
                await procBuilder.DropBulkDeleteAsync().ConfigureAwait(false);
                await procBuilder.DropTVPTypeAsync().ConfigureAwait(false);
            }

            if (!alreadyOpened)
                connection.Close();

        }


        public async Task DropTrackingTableAsync(DbConnection connection, DbTransaction transaction = null)
        {
            var alreadyOpened = connection.State != ConnectionState.Closed;

            if (!alreadyOpened)
                await connection.OpenAsync().ConfigureAwait(false);

            var trackingTableBuilder = CreateTrackingTableBuilder(connection, transaction);

            if (!await trackingTableBuilder.NeedToCreateTrackingTableAsync().ConfigureAwait(false))
                await trackingTableBuilder.DropTableAsync().ConfigureAwait(false);

            if (!alreadyOpened)
                connection.Close();

        }

        /// <summary>
        /// Deprovision table
        /// </summary>
        public async Task DropTableAsync(DbConnection connection, DbTransaction transaction = null)
        {
            var alreadyOpened = connection.State != ConnectionState.Closed;

            if (!alreadyOpened)
                await connection.OpenAsync().ConfigureAwait(false);

            var tableBuilder = CreateTableBuilder(connection, transaction);

            if (!await tableBuilder.NeedToCreateTableAsync().ConfigureAwait(false))
                await tableBuilder.DropTableAsync().ConfigureAwait(false);

            if (!alreadyOpened)
                connection.Close();

        }

        /// <summary>
        /// Deprovision table
        /// </summary>
        public async Task DropAsync(SyncProvision provision, DbConnection connection, DbTransaction transaction = null)
        {
            var alreadyOpened = connection.State != ConnectionState.Closed;

            if (!alreadyOpened)
                await connection.OpenAsync().ConfigureAwait(false);

            if (provision.HasFlag(SyncProvision.StoredProcedures)) { }
                await this.DropStoredProceduresAsync(connection, transaction).ConfigureAwait(false);

            if (provision.HasFlag(SyncProvision.Triggers))
                await this.DropTriggersAsync(connection, transaction).ConfigureAwait(false);

            if (provision.HasFlag(SyncProvision.TrackingTable))
                await this.DropTrackingTableAsync(connection, transaction).ConfigureAwait(false);

            if (provision.HasFlag(SyncProvision.Table))
                await this.DropTableAsync(connection, transaction).ConfigureAwait(false);

            if (!alreadyOpened)
                connection.Close();

        }
    }
}
