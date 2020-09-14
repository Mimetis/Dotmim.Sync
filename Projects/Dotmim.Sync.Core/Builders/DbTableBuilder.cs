
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
        /// Gets Setup, containing naming prefix and suffix if needed
        /// </summary>
        public SyncSetup Setup { get; }

        /// <summary>
        /// Gets Scopename
        /// </summary>
        public string ScopeName { get; }

        /// <summary>
        /// Filtered Columns
        /// </summary>
        public SyncFilter Filter { get; set; }

        /// <summary>
        /// Gets or Sets if the Database builder supports bulk procedures
        /// </summary>
        //public bool UseBulkProcedures { get; set; } = true;

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
        public DbTableBuilder(SyncTable tableDescription, SyncSetup setup, string scopeName)
        {
            this.TableDescription = tableDescription;
            this.Setup = setup;
            this.ScopeName = scopeName;

            (this.TableName, this.TrackingTableName) = GetParsers(tableDescription, setup);
        }

        public async Task RenameTrackingTableAsync(ParserName oldTableName, DbConnection connection, DbTransaction transaction)
        {
            if (TableDescription.PrimaryKeys.Count <= 0)
                throw new MissingPrimaryKeyException(TableDescription.TableName);

            var alreadyOpened = connection.State != ConnectionState.Closed;

            if (!alreadyOpened)
                await connection.OpenAsync().ConfigureAwait(false);

            var trackingTableBuilder = CreateTrackingTableBuilder();

            //// TODO be sure the table actually exists
            //var hasbeenCreated = await this.CreateTrackingTableAsync(connection, transaction);
            //if (!hasbeenCreated)

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

            var trackingTableBuilder = CreateTrackingTableBuilder();
            var tableBuilder = CreateTableBuilder();

            await tableBuilder.CreateSchemaAsync(connection, transaction).ConfigureAwait(false);
            await trackingTableBuilder.CreateTableAsync(connection, transaction).ConfigureAwait(false);

            if (!alreadyOpened)
                connection.Close();

            return true;
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

            await tableBuilder.CreateSchemaAsync(connection, transaction).ConfigureAwait(false);
            await tableBuilder.CreateTableAsync(connection, transaction).ConfigureAwait(false);

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

            System.Diagnostics.Stopwatch stopwatch = new System.Diagnostics.Stopwatch();
            stopwatch.Start();

            if (provision.HasFlag(SyncProvision.Table))
                await this.CreateTableAsync(connection, transaction).ConfigureAwait(false);

            if (provision.HasFlag(SyncProvision.TrackingTable))
                await this.CreateTrackingTableAsync(connection, transaction).ConfigureAwait(false);

            if (provision.HasFlag(SyncProvision.Triggers))
                await this.CreateTriggersAsync(connection, transaction).ConfigureAwait(false);

            stopwatch.Stop();
            var str = $"{stopwatch.Elapsed.Minutes}:{stopwatch.Elapsed.Seconds}.{stopwatch.Elapsed.Milliseconds}";
            System.Diagnostics.Debug.WriteLine(str);

            if (!alreadyOpened)
                connection.Close();
        }

        public async Task DropTrackingTableAsync(DbConnection connection, DbTransaction transaction = null)
        {
            var alreadyOpened = connection.State != ConnectionState.Closed;

            if (!alreadyOpened)
                await connection.OpenAsync().ConfigureAwait(false);

            var trackingTableBuilder = this.CreateTrackingTableBuilder();

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
