
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
        public abstract IDbBuilderProcedureHelper CreateProcBuilder();

        /// <summary>
        /// You have to provide a trigger builder implementation for your current database
        /// </summary>
        public abstract IDbBuilderTriggerHelper CreateTriggerBuilder();

        /// <summary>
        /// You have to provide a table builder implementation for your current database
        /// </summary>
        public abstract IDbBuilderTableHelper CreateTableBuilder();

        /// <summary>
        /// You have to provider a tracking table builder implementation for your current database
        /// </summary>
        public abstract IDbBuilderTrackingTableHelper CreateTrackingTableBuilder();

        /// <summary>
        /// Gets the table Sync Adapter in charge of executing all command during sync
        /// </summary>
        public abstract SyncAdapter CreateSyncAdapter();


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
        public async Task CreateForeignKeysAsync(DbConnection connection, DbTransaction transaction)
        {
            if (TableDescription.PrimaryKeys.Count <= 0)
                throw new MissingPrimaryKeyException(TableDescription.TableName);

            var alreadyOpened = connection.State != ConnectionState.Closed;

            if (!alreadyOpened)
                await connection.OpenAsync().ConfigureAwait(false);

            var tableBuilder = CreateTableBuilder();

            // Get all parent table and create the foreign key on it
            foreach (var constraint in this.TableDescription.GetRelations())
            {
                // Check if we need to create the foreign key constraint
                if (await tableBuilder.NeedToCreateForeignKeyConstraintsAsync(constraint, connection, transaction).ConfigureAwait(false))
                {
                    await tableBuilder.CreateForeignKeyConstraintsAsync(constraint, connection, transaction).ConfigureAwait(false);
                }
            }

            if (!alreadyOpened)
                connection.Close();


        }

        public async Task RenameTrackingTableAsync(ParserName oldTableName, DbConnection connection, DbTransaction transaction)
        {
            if (TableDescription.PrimaryKeys.Count <= 0)
                throw new MissingPrimaryKeyException(TableDescription.TableName);

            var alreadyOpened = connection.State != ConnectionState.Closed;

            if (!alreadyOpened)
                await connection.OpenAsync().ConfigureAwait(false);

            var trackingTableBuilder = CreateTrackingTableBuilder();

            // be sure the table actually exists
            var hasbeenCreated = await this.CreateTrackingTableAsync(connection, transaction);

            if (!hasbeenCreated)
                await trackingTableBuilder.RenameTableAsync(oldTableName, connection, transaction);

            if (!alreadyOpened)
                connection.Close();
        }

        public async Task<bool> CreateTrackingTableAsync(DbConnection connection, DbTransaction transaction)
        {
            if (TableDescription.PrimaryKeys.Count <= 0)
                throw new MissingPrimaryKeyException(TableDescription.TableName);

            var alreadyOpened = connection.State != ConnectionState.Closed;

            if (!alreadyOpened)
                await connection.OpenAsync().ConfigureAwait(false);


            var hasBeenCreated = false;

            var trackingTableBuilder = CreateTrackingTableBuilder();
            var tableBuilder = CreateTableBuilder();

            if (await trackingTableBuilder.NeedToCreateTrackingTableAsync(connection, transaction).ConfigureAwait(false))
            {
                if (await tableBuilder.NeedToCreateSchemaAsync(connection, transaction).ConfigureAwait(false))
                    await tableBuilder.CreateSchemaAsync(connection, transaction).ConfigureAwait(false);

                await trackingTableBuilder.CreateTableAsync(connection, transaction).ConfigureAwait(false);
                await trackingTableBuilder.CreatePkAsync(connection, transaction).ConfigureAwait(false);
                await trackingTableBuilder.CreateIndexAsync(connection, transaction).ConfigureAwait(false);
                hasBeenCreated = true;
            }

            if (!alreadyOpened)
                connection.Close();

            return hasBeenCreated;
        }

        public async Task CreateStoredProceduresAsync(DbConnection connection, DbTransaction transaction)
        {
            if (TableDescription.PrimaryKeys.Count <= 0)
                throw new MissingPrimaryKeyException(TableDescription.TableName);

            // Check if we have mutables columns
            var hasMutableColumns = TableDescription.GetMutableColumns(false).Any();

            var alreadyOpened = connection.State != ConnectionState.Closed;

            if (!alreadyOpened)
                await connection.OpenAsync().ConfigureAwait(false);

            // could be null
            var procBuilder = CreateProcBuilder();
            if (procBuilder == null)
                return;

            if (await procBuilder.NeedToCreateProcedureAsync(DbCommandType.SelectChanges, connection, transaction).ConfigureAwait(false))
                await procBuilder.CreateSelectIncrementalChangesAsync(this.Filter, connection, transaction).ConfigureAwait(false);

            if (await procBuilder.NeedToCreateProcedureAsync(DbCommandType.SelectInitializedChanges, connection, transaction).ConfigureAwait(false))
                await procBuilder.CreateSelectInitializedChangesAsync(this.Filter, connection, transaction).ConfigureAwait(false);

            if (await procBuilder.NeedToCreateProcedureAsync(DbCommandType.SelectRow, connection, transaction).ConfigureAwait(false))
                await procBuilder.CreateSelectRowAsync(connection, transaction).ConfigureAwait(false);

            if (await procBuilder.NeedToCreateProcedureAsync(DbCommandType.UpdateRow, connection, transaction).ConfigureAwait(false))
                await procBuilder.CreateUpdateAsync(hasMutableColumns, connection, transaction).ConfigureAwait(false);

            if (await procBuilder.NeedToCreateProcedureAsync(DbCommandType.DeleteRow, connection, transaction).ConfigureAwait(false))
                await procBuilder.CreateDeleteAsync(connection, transaction).ConfigureAwait(false);

            if (await procBuilder.NeedToCreateProcedureAsync(DbCommandType.DeleteMetadata, connection, transaction).ConfigureAwait(false))
                await procBuilder.CreateDeleteMetadataAsync(connection, transaction).ConfigureAwait(false);

            if (await procBuilder.NeedToCreateProcedureAsync(DbCommandType.Reset, connection, transaction).ConfigureAwait(false))
                await procBuilder.CreateResetAsync(connection, transaction).ConfigureAwait(false);

            if (this.UseBulkProcedures && await procBuilder.NeedToCreateTypeAsync(DbCommandType.BulkTableType, connection, transaction).ConfigureAwait(false))
            {
                await procBuilder.CreateTVPTypeAsync(connection, transaction).ConfigureAwait(false);
                await procBuilder.CreateBulkUpdateAsync(hasMutableColumns, connection, transaction).ConfigureAwait(false);
                await procBuilder.CreateBulkDeleteAsync(connection, transaction).ConfigureAwait(false);
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

            var tableBuilder = CreateTableBuilder();

            var alreadyOpened = connection.State != ConnectionState.Closed;

            if (!alreadyOpened)
                await connection.OpenAsync().ConfigureAwait(false);

            // Check if we need to create the tables
            if (await tableBuilder.NeedToCreateTableAsync(connection, transaction).ConfigureAwait(false))
            {
                if (await tableBuilder.NeedToCreateSchemaAsync(connection, transaction).ConfigureAwait(false))
                    await tableBuilder.CreateSchemaAsync(connection, transaction).ConfigureAwait(false);

                await tableBuilder.CreateTableAsync(connection, transaction).ConfigureAwait(false);
                await tableBuilder.CreatePrimaryKeyAsync(connection, transaction).ConfigureAwait(false);
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

            var procBuilder = this.CreateProcBuilder();

            // Could be null
            if (procBuilder == null)
                return;

            if (!await procBuilder.NeedToCreateProcedureAsync(DbCommandType.SelectChanges, connection, transaction).ConfigureAwait(false))
                await procBuilder.DropSelectIncrementalChangesAsync(this.Filter, connection, transaction).ConfigureAwait(false);
            if (!await procBuilder.NeedToCreateProcedureAsync(DbCommandType.SelectInitializedChanges, connection, transaction).ConfigureAwait(false))
                await procBuilder.DropSelectInitializedChangesAsync(this.Filter, connection, transaction).ConfigureAwait(false);
            if (!await procBuilder.NeedToCreateProcedureAsync(DbCommandType.SelectRow, connection, transaction).ConfigureAwait(false))
                await procBuilder.DropSelectRowAsync(connection, transaction).ConfigureAwait(false);
            if (!await procBuilder.NeedToCreateProcedureAsync(DbCommandType.UpdateRow, connection, transaction).ConfigureAwait(false))
                await procBuilder.DropUpdateAsync(connection, transaction).ConfigureAwait(false);
            if (!await procBuilder.NeedToCreateProcedureAsync(DbCommandType.DeleteRow, connection, transaction).ConfigureAwait(false))
                await procBuilder.DropDeleteAsync(connection, transaction).ConfigureAwait(false);
            if (!await procBuilder.NeedToCreateProcedureAsync(DbCommandType.DeleteMetadata, connection, transaction).ConfigureAwait(false))
                await procBuilder.DropDeleteMetadataAsync(connection, transaction).ConfigureAwait(false);
            if (!await procBuilder.NeedToCreateProcedureAsync(DbCommandType.Reset, connection, transaction).ConfigureAwait(false))
                await procBuilder.DropResetAsync(connection, transaction).ConfigureAwait(false);

            if (this.UseBulkProcedures && !await procBuilder.NeedToCreateTypeAsync(DbCommandType.BulkTableType, connection, transaction).ConfigureAwait(false))
            {
                await procBuilder.DropBulkUpdateAsync( connection, transaction).ConfigureAwait(false);
                await procBuilder.DropBulkDeleteAsync( connection, transaction).ConfigureAwait(false);
                await procBuilder.DropTVPTypeAsync(connection, transaction).ConfigureAwait(false);
            }

            if (!alreadyOpened)
                connection.Close();

        }


        public async Task DropTrackingTableAsync(DbConnection connection, DbTransaction transaction = null)
        {
            var alreadyOpened = connection.State != ConnectionState.Closed;

            if (!alreadyOpened)
                await connection.OpenAsync().ConfigureAwait(false);

            var trackingTableBuilder = this.CreateTrackingTableBuilder();

            if (!await trackingTableBuilder.NeedToCreateTrackingTableAsync(connection, transaction).ConfigureAwait(false))
                await trackingTableBuilder.DropTableAsync(connection, transaction).ConfigureAwait(false);

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

            var tableBuilder = this.CreateTableBuilder();

            if (!await tableBuilder.NeedToCreateTableAsync(connection, transaction).ConfigureAwait(false))
                await tableBuilder.DropTableAsync(connection, transaction).ConfigureAwait(false);

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

            if (provision.HasFlag(SyncProvision.StoredProcedures))
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
