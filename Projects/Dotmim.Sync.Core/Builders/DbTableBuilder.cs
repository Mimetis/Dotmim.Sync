
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;
using System.Linq;
using System.Data;
using Dotmim.Sync.Log;

using System.Diagnostics;

namespace Dotmim.Sync.Builders
{
    public abstract partial class DbTableBuilder
    {

        /// <summary>
        /// Gets the table description for the current DbBuilder
        /// </summary>
        public SyncTable TableDescription { get; set; }

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
        /// Construct a DbBuilder
        /// </summary>
        public DbTableBuilder(SyncTable tableDescription) => this.TableDescription = tableDescription;

        /// <summary>
        /// Apply config.
        /// Create relations if needed
        /// </summary>
        public void CreateForeignKeys(DbConnection connection, DbTransaction transaction = null)
        {
            if (TableDescription.PrimaryKeys.Count <= 0)
                throw new MissingPrimaryKeyException(TableDescription.TableName);

            var alreadyOpened = connection.State != ConnectionState.Closed;

            if (!alreadyOpened)
                connection.Open();

            var tableBuilder = CreateTableBuilder(connection, transaction);

            // Get all parent table and create the foreign key on it
            foreach (var constraint in this.TableDescription.GetRelations())
            {
                // Check if we need to create the foreign key constraint
                if (tableBuilder.NeedToCreateForeignKeyConstraints(constraint))
                {
                    tableBuilder.CreateForeignKeyConstraints(constraint);
                }
            }

            if (!alreadyOpened)
                connection.Close();


        }

        public void CreateTrackingTable(DbConnection connection, DbTransaction transaction = null)
        {
            if (TableDescription.PrimaryKeys.Count <= 0)
                throw new MissingPrimaryKeyException(TableDescription.TableName);

            var alreadyOpened = connection.State != ConnectionState.Closed;

            var trackingTableBuilder = CreateTrackingTableBuilder(connection, transaction);

            if (trackingTableBuilder.NeedToCreateTrackingTable())
            {
                trackingTableBuilder.CreateTable();
                trackingTableBuilder.CreatePk();
                trackingTableBuilder.CreateIndex();
            }

            if (!alreadyOpened)
                connection.Close();

        }

 
        public void CreateStoredProcedures(DbConnection connection, DbTransaction transaction = null)
        {
            if (TableDescription.PrimaryKeys.Count <= 0)
                throw new MissingPrimaryKeyException(TableDescription.TableName);

            // Check if we have mutables columns
            var hasMutableColumns = TableDescription.GetMutableColumns(false).Any();

            var alreadyOpened = connection.State != ConnectionState.Closed;


            // could be null
            var procBuilder = CreateProcBuilder(connection, transaction);
            if (procBuilder == null)
                return;

            if (procBuilder.NeedToCreateProcedure(DbCommandType.SelectChanges))
                procBuilder.CreateSelectIncrementalChanges(this.Filter);
            if (procBuilder.NeedToCreateProcedure(DbCommandType.SelectInitializedChanges))
                procBuilder.CreateSelectInitializedChanges(this.Filter);

            if (procBuilder.NeedToCreateProcedure(DbCommandType.SelectRow))
                procBuilder.CreateSelectRow();

            if (procBuilder.NeedToCreateProcedure(DbCommandType.UpdateRow))
                procBuilder.CreateUpdate(hasMutableColumns);

            if (procBuilder.NeedToCreateProcedure(DbCommandType.DeleteRow))
                procBuilder.CreateDelete();

            if (procBuilder.NeedToCreateProcedure(DbCommandType.DeleteMetadata))
                procBuilder.CreateDeleteMetadata();
            if (procBuilder.NeedToCreateProcedure(DbCommandType.Reset))
                procBuilder.CreateReset();

            if (this.UseBulkProcedures && procBuilder.NeedToCreateType(DbCommandType.BulkTableType))
            {
                procBuilder.CreateTVPType();
                procBuilder.CreateBulkUpdate(hasMutableColumns);
                procBuilder.CreateBulkDelete();
            }

            if (!alreadyOpened)
                connection.Close();

        }

        public void CreateTable(DbConnection connection, DbTransaction transaction = null)
        {
            if (TableDescription.PrimaryKeys.Count <= 0)
                throw new MissingPrimaryKeyException(TableDescription.TableName);

            var tableBuilder = CreateTableBuilder(connection, transaction);

            var alreadyOpened = connection.State != ConnectionState.Closed;


            // Check if we need to create the tables
            if (tableBuilder.NeedToCreateTable())
            {
                if (tableBuilder.NeedToCreateSchema())
                    tableBuilder.CreateSchema();

                tableBuilder.CreateTable();
                tableBuilder.CreatePrimaryKey();
            }

            if (!alreadyOpened)
                connection.Close();

        }

        /// <summary>
        /// Apply the config.
        /// Create the table if needed
        /// </summary>
        public void Create(DbConnection connection, DbTransaction transaction = null)
        {
            if (TableDescription.PrimaryKeys.Count <= 0)
                throw new MissingPrimaryKeyException(TableDescription.TableName);

            var alreadyOpened = connection.State != ConnectionState.Closed;

            if (!alreadyOpened)
                connection.Open();

            this.CreateTable(connection, transaction);

            this.CreateTrackingTable(connection, transaction);

            this.CreateTriggers(connection, transaction);

            this.CreateStoredProcedures(connection, transaction);

            if (!alreadyOpened)
                connection.Close();
        }


        public void DropProcedures(DbConnection connection, DbTransaction transaction = null)
        {
            var alreadyOpened = connection.State != ConnectionState.Closed;

            if (!alreadyOpened)
                connection.Open();

            var procBuilder = CreateProcBuilder(connection, transaction);

            // Could be null
            if (procBuilder == null)
                return;

            if (!procBuilder.NeedToCreateProcedure(DbCommandType.SelectChanges))
                procBuilder.DropSelectIncrementalChanges(this.Filter);
            if (!procBuilder.NeedToCreateProcedure(DbCommandType.SelectInitializedChanges))
                procBuilder.DropSelectInitializedChanges(this.Filter);
            if (!procBuilder.NeedToCreateProcedure(DbCommandType.SelectRow))
                procBuilder.DropSelectRow();
            if (!procBuilder.NeedToCreateProcedure(DbCommandType.UpdateRow))
                procBuilder.DropUpdate();
            if (!procBuilder.NeedToCreateProcedure(DbCommandType.DeleteRow))
                procBuilder.DropDelete();
            if (!procBuilder.NeedToCreateProcedure(DbCommandType.DeleteMetadata))
                procBuilder.DropDeleteMetadata();
            if (!procBuilder.NeedToCreateProcedure(DbCommandType.Reset))
                procBuilder.DropReset();

            if (this.UseBulkProcedures && !procBuilder.NeedToCreateType(DbCommandType.BulkTableType))
            {
                procBuilder.DropBulkUpdate();
                procBuilder.DropBulkDelete();
                procBuilder.DropTVPType();
            }

            if (!alreadyOpened)
                connection.Close();

        }

 
        public void DropTrackingTable(DbConnection connection, DbTransaction transaction = null)
        {
            var alreadyOpened = connection.State != ConnectionState.Closed;

            if (!alreadyOpened)
                connection.Open();

            var trackingTableBuilder = CreateTrackingTableBuilder(connection, transaction);

            if (!trackingTableBuilder.NeedToCreateTrackingTable())
                trackingTableBuilder.DropTable();

            if (!alreadyOpened)
                connection.Close();

        }

        /// <summary>
        /// Deprovision table
        /// </summary>
        public void DropTable(DbConnection connection, DbTransaction transaction = null)
        {
            var alreadyOpened = connection.State != ConnectionState.Closed;

            if (!alreadyOpened)
                connection.Open();

            var tableBuilder = CreateTableBuilder(connection, transaction);

            if (!tableBuilder.NeedToCreateTable())
                tableBuilder.DropTable();

            if (!alreadyOpened)
                connection.Close();

        }

        /// <summary>
        /// Deprovision table
        /// </summary>
        public void Drop(DbConnection connection, DbTransaction transaction = null)
        {
            var alreadyOpened = connection.State != ConnectionState.Closed;

            if (!alreadyOpened)
                connection.Open();

            this.DropProcedures(connection, transaction);

            this.DropTriggers(connection, transaction);

            this.DropTrackingTable(connection, transaction);

            this.DropTable(connection, transaction);

            if (!alreadyOpened)
                connection.Close();

        }
    }
}
