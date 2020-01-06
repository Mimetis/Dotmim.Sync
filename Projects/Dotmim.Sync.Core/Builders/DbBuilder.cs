using Dotmim.Sync.Data;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;
using System.Linq;
using System.Data;
using Dotmim.Sync.Log;
using Dotmim.Sync.Filter;
using System.Diagnostics;

namespace Dotmim.Sync.Builders
{
    public abstract class DbBuilder
    {

        /// <summary>
        /// Gets the table description for the current DbBuilder
        /// </summary>
        public SyncTable TableDescription { get; set; }

        /// <summary>
        /// Filtered Columns
        /// </summary>
        public SyncFilters FilterColumns { get; set; } = new SyncFilters();

        /// <summary>
        /// Gets or Sets if the Database builder supports bulk procedures
        /// </summary>
        public bool UseBulkProcedures { get; set; } = true;

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
        public DbBuilder(SyncTable tableDescription)
        {
            this.TableDescription = tableDescription;
        }


        /// <summary>
        /// Apply config.
        /// Create relations if needed
        /// </summary>
        public void CreateForeignKeys(DbConnection connection, DbTransaction transaction = null)
        {
            if (TableDescription.PrimaryKeys.Count <= 0)
                throw new InvalidOperationException($"Create foreign keys: Table {TableDescription.TableName} must have at least one dmColumn as Primary key");

            var alreadyOpened = connection.State != ConnectionState.Closed;

            try
            {
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
            }
            finally
            {
                if (!alreadyOpened)
                    connection.Close();
            }

        }

        public void CreateTrackingTable(DbConnection connection, DbTransaction transaction = null)
        {
            if (TableDescription.PrimaryKeys.Count <= 0)
                throw new InvalidOperationException($"Create tracking table: Table {TableDescription.TableName} must have at least one dmColumn as Primary key");

            var alreadyOpened = connection.State != ConnectionState.Closed;

            try
            {
                var trackingTableBuilder = CreateTrackingTableBuilder(connection, transaction);
                trackingTableBuilder.Filters = this.FilterColumns.GetColumnFilters();

                if (trackingTableBuilder.NeedToCreateTrackingTable())
                {
                    trackingTableBuilder.CreateTable();
                    trackingTableBuilder.CreatePk();
                    trackingTableBuilder.CreateIndex();

                    // Fill the tracking table with actual rows from base table
                    trackingTableBuilder.PopulateFromBaseTable();
                }
            }
            finally
            {
                if (!alreadyOpened)
                    connection.Close();
            }
        }

        public void CreateTriggers(DbConnection connection, DbTransaction transaction = null)
        {
            if (TableDescription.PrimaryKeys.Count <= 0)
                throw new InvalidOperationException($"Create triggers: Table {TableDescription.TableName} must have at least one dmColumn as Primary key");

            var alreadyOpened = connection.State != ConnectionState.Closed;

            try
            {
                var triggerBuilder = CreateTriggerBuilder(connection, transaction);
                triggerBuilder.Filters = this.FilterColumns.GetColumnFilters();

                if (triggerBuilder.NeedToCreateTrigger(DbTriggerType.Insert))
                    triggerBuilder.CreateInsertTrigger();
                if (triggerBuilder.NeedToCreateTrigger(DbTriggerType.Update))
                    triggerBuilder.CreateUpdateTrigger();
                if (triggerBuilder.NeedToCreateTrigger(DbTriggerType.Delete))
                    triggerBuilder.CreateDeleteTrigger();
            }
            finally
            {
                if (!alreadyOpened)
                    connection.Close();
            }
        }

        public void CreateStoredProcedures(DbConnection connection, DbTransaction transaction = null)
        {
            if (TableDescription.PrimaryKeys.Count <= 0)
                throw new InvalidOperationException($"Create stored procedures: Table {TableDescription.TableName} must have at least one dmColumn as Primary key");

            // Check if we have mutables columns
            var hasMutableColumns = TableDescription.GetMutableColumns(false).Any();

            var alreadyOpened = connection.State != ConnectionState.Closed;

            try
            {
                // could be null
                var procBuilder = CreateProcBuilder(connection, transaction);
                if (procBuilder == null)
                    return;

                procBuilder.Filters = this.FilterColumns;

                if (procBuilder.NeedToCreateProcedure(DbCommandType.SelectChanges))
                    procBuilder.CreateSelectIncrementalChanges();
                if (procBuilder.NeedToCreateProcedure(DbCommandType.SelectInitializedChanges))
                    procBuilder.CreateSelectInitializedChanges();

                if (procBuilder.NeedToCreateProcedure(DbCommandType.SelectRow))
                    procBuilder.CreateSelectRow();

                if (procBuilder.NeedToCreateProcedure(DbCommandType.UpdateRow))
                    procBuilder.CreateUpdate(hasMutableColumns);

                if (procBuilder.NeedToCreateProcedure(DbCommandType.DeleteRow))
                    procBuilder.CreateDelete();
                if (procBuilder.NeedToCreateProcedure(DbCommandType.UpdateMetadata))
                    procBuilder.CreateUpdateMetadata();
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
            }
            finally
            {
                if (!alreadyOpened)
                    connection.Close();
            }
        }

        public void CreateTable(DbConnection connection, DbTransaction transaction = null)
        {
            if (TableDescription.PrimaryKeys.Count <= 0)
                throw new InvalidOperationException($"Create table: Table {TableDescription.TableName} must have at least one dmColumn as Primary key");

            var tableBuilder = CreateTableBuilder(connection, transaction);

            var alreadyOpened = connection.State != ConnectionState.Closed;

            try
            {
                // Check if we need to create the tables
                if (tableBuilder.NeedToCreateTable())
                {
                    if (tableBuilder.NeedToCreateSchema())
                        tableBuilder.CreateSchema();

                    tableBuilder.CreateTable();
                    tableBuilder.CreatePrimaryKey();
                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine(ex);
                throw ex;
            }
            finally
            {
                if (!alreadyOpened)
                    connection.Close();
            }
        }

        /// <summary>
        /// Apply the config.
        /// Create the table if needed
        /// </summary>
        public void Create(DbConnection connection, DbTransaction transaction = null)
        {
            if (TableDescription.PrimaryKeys.Count <= 0)
                throw new InvalidOperationException($"Before creating table: Table {TableDescription.TableName} must have at least one dmColumn as Primary key");

            var alreadyOpened = connection.State != ConnectionState.Closed;

            try
            {
                if (!alreadyOpened)
                    connection.Open();

                this.CreateTable(connection, transaction);

                this.CreateTrackingTable(connection, transaction);

                this.CreateTriggers(connection, transaction);

                this.CreateStoredProcedures(connection, transaction);
            }
            finally
            {
                if (!alreadyOpened)
                    connection.Close();
            }


        }


        public void DropProcedures(DbConnection connection, DbTransaction transaction = null)
        {
            var alreadyOpened = connection.State != ConnectionState.Closed;

            // Check if we have mutables columns
            var hasMutableColumns = TableDescription.GetMutableColumns(false).Any();

            try
            {
                if (!alreadyOpened)
                    connection.Open();

                var procBuilder = CreateProcBuilder(connection, transaction);

                // Could be null
                if (procBuilder == null)
                    return;

                procBuilder.Filters = this.FilterColumns;

                if (!procBuilder.NeedToCreateProcedure(DbCommandType.SelectChanges))
                    procBuilder.DropSelectIncrementalChanges();
                if (!procBuilder.NeedToCreateProcedure(DbCommandType.SelectInitializedChanges))
                    procBuilder.DropSelectInitializedChanges();
                if (!procBuilder.NeedToCreateProcedure(DbCommandType.SelectRow))
                    procBuilder.DropSelectRow();
                if (!procBuilder.NeedToCreateProcedure(DbCommandType.UpdateRow))
                    procBuilder.DropUpdate();
                if (!procBuilder.NeedToCreateProcedure(DbCommandType.DeleteRow))
                    procBuilder.DropDelete();
                if (!procBuilder.NeedToCreateProcedure(DbCommandType.UpdateMetadata))
                    procBuilder.DropUpdateMetadata();
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
            }
            finally
            {
                if (!alreadyOpened)
                    connection.Close();
            }
        }

        public void DropTriggers(DbConnection connection, DbTransaction transaction = null)
        {
            var alreadyOpened = connection.State != ConnectionState.Closed;

            try
            {
                if (!alreadyOpened)
                    connection.Open();

                var triggerBuilder = CreateTriggerBuilder(connection, transaction);
                triggerBuilder.Filters = this.FilterColumns.GetColumnFilters();

                if (!triggerBuilder.NeedToCreateTrigger(DbTriggerType.Insert))
                    triggerBuilder.DropInsertTrigger();
                if (!triggerBuilder.NeedToCreateTrigger(DbTriggerType.Update))
                    triggerBuilder.DropUpdateTrigger();
                if (!triggerBuilder.NeedToCreateTrigger(DbTriggerType.Delete))
                    triggerBuilder.DropDeleteTrigger();

            }
            finally
            {
                if (!alreadyOpened)
                    connection.Close();
            }
        }

        public void DropTrackingTable(DbConnection connection, DbTransaction transaction = null)
        {
            var alreadyOpened = connection.State != ConnectionState.Closed;

            try
            {
                if (!alreadyOpened)
                    connection.Open();


                var trackingTableBuilder = CreateTrackingTableBuilder(connection, transaction);
                trackingTableBuilder.Filters = this.FilterColumns.GetColumnFilters();

                if (!trackingTableBuilder.NeedToCreateTrackingTable())
                    trackingTableBuilder.DropTable();

            }
            finally
            {
                if (!alreadyOpened)
                    connection.Close();
            }
        }

        /// <summary>
        /// Deprovision table
        /// </summary>
        public void DropTable(DbConnection connection, DbTransaction transaction = null)
        {
            var alreadyOpened = connection.State != ConnectionState.Closed;

            try
            {
                if (!alreadyOpened)
                    connection.Open();

                var tableBuilder = CreateTableBuilder(connection, transaction);

                if (!tableBuilder.NeedToCreateTable())
                    tableBuilder.DropTable();
            }
            finally
            {
                if (!alreadyOpened)
                    connection.Close();
            }


        }

        /// <summary>
        /// Deprovision table
        /// </summary>
        public void Drop(DbConnection connection, DbTransaction transaction = null)
        {
            var alreadyOpened = connection.State != ConnectionState.Closed;

            try
            {
                if (!alreadyOpened)
                    connection.Open();

                this.DropProcedures(connection, transaction);

                this.DropTriggers(connection, transaction);

                this.DropTrackingTable(connection, transaction);

                this.DropTable(connection, transaction);
            }
            finally
            {
                if (!alreadyOpened)
                    connection.Close();
            }
        }


        public string ScriptForeignKeys(DbConnection connection, DbTransaction transaction = null)
        {
            var alreadyOpened = connection.State != ConnectionState.Closed;

            try
            {
                if (!alreadyOpened)
                    connection.Open();

                var stringBuilder = new StringBuilder();

                var tableBuilder = CreateTableBuilder(connection, transaction);

                foreach (var constraint in this.TableDescription.GetRelations())
                {
                    if (tableBuilder.NeedToCreateForeignKeyConstraints(constraint))
                        stringBuilder.Append(tableBuilder.CreateForeignKeyConstraintsScriptText(constraint));
                }

                return stringBuilder.ToString();
            }
            finally
            {
                if (!alreadyOpened)
                    connection.Close();
            }
        }

        /// <summary>
        /// Generate the creating script string (admin only)
        /// </summary>
        public string ScriptTable(DbConnection connection, DbTransaction transaction = null)
        {
            string str = null;
            var alreadyOpened = connection.State != ConnectionState.Closed;
            bool needToCreateTrackingTable = false;

            try
            {
                if (!alreadyOpened)
                    connection.Open();

                StringBuilder stringBuilder = new StringBuilder();

                var tableBuilder = CreateTableBuilder(connection, transaction);

                // Check if we have mutables columns
                var hasMutableColumns = TableDescription.GetMutableColumns(false).Any();

                // Check if we need to create the tables
                if (tableBuilder.NeedToCreateTable())
                {
                    if (tableBuilder.NeedToCreateSchema())
                        stringBuilder.Append(tableBuilder.CreateSchemaScriptText());

                    stringBuilder.Append(tableBuilder.CreateTableScriptText());
                    stringBuilder.Append(tableBuilder.CreatePrimaryKeyScriptText());
                }

                var trackingTableBuilder = CreateTrackingTableBuilder(connection, transaction);
                trackingTableBuilder.Filters = this.FilterColumns.GetColumnFilters();

                if (trackingTableBuilder.NeedToCreateTrackingTable())
                {
                    stringBuilder.Append(trackingTableBuilder.CreateTableScriptText());
                    stringBuilder.Append(trackingTableBuilder.CreatePkScriptText());
                    stringBuilder.Append(trackingTableBuilder.CreateIndexScriptText());

                    needToCreateTrackingTable = true;
                }
                var triggerBuilder = CreateTriggerBuilder(connection, transaction);
                triggerBuilder.Filters = this.FilterColumns.GetColumnFilters();

                if (triggerBuilder.NeedToCreateTrigger(DbTriggerType.Insert))
                    stringBuilder.Append(triggerBuilder.CreateInsertTriggerScriptText());
                if (triggerBuilder.NeedToCreateTrigger(DbTriggerType.Update))
                    stringBuilder.Append(triggerBuilder.CreateUpdateTriggerScriptText());
                if (triggerBuilder.NeedToCreateTrigger(DbTriggerType.Delete))
                    stringBuilder.Append(triggerBuilder.CreateDeleteTriggerScriptText());

                var procBuilder = CreateProcBuilder(connection, transaction);

                if (procBuilder != null)
                {
                    procBuilder.Filters = this.FilterColumns;

                    if (procBuilder.NeedToCreateProcedure(DbCommandType.SelectChanges))
                        stringBuilder.Append(procBuilder.CreateSelectIncrementalChangesScriptText());
                    if (procBuilder.NeedToCreateProcedure(DbCommandType.SelectInitializedChanges))
                        stringBuilder.Append(procBuilder.CreateSelectInitializedChangesScriptText());
                    if (procBuilder.NeedToCreateProcedure(DbCommandType.SelectRow))
                        stringBuilder.Append(procBuilder.CreateSelectRowScriptText());
                    if (procBuilder.NeedToCreateProcedure(DbCommandType.UpdateRow))
                        stringBuilder.Append(procBuilder.CreateUpdateScriptText(hasMutableColumns));
                    if (procBuilder.NeedToCreateProcedure(DbCommandType.DeleteRow))
                        stringBuilder.Append(procBuilder.CreateDeleteScriptText());
                    if (procBuilder.NeedToCreateProcedure(DbCommandType.UpdateMetadata))
                        stringBuilder.Append(procBuilder.CreateUpdateMetadataScriptText());
                    if (procBuilder.NeedToCreateProcedure(DbCommandType.DeleteMetadata))
                        stringBuilder.Append(procBuilder.CreateDeleteMetadataScriptText());
                    if (procBuilder.NeedToCreateProcedure(DbCommandType.Reset))
                        stringBuilder.Append(procBuilder.CreateResetScriptText());

                    if (this.UseBulkProcedures && procBuilder.NeedToCreateType(DbCommandType.BulkTableType))
                    {
                        stringBuilder.Append(procBuilder.CreateTVPTypeScriptText());
                        stringBuilder.Append(procBuilder.CreateBulkUpdateScriptText(hasMutableColumns));
                        stringBuilder.Append(procBuilder.CreateBulkDeleteScriptText());
                    }
                    if (needToCreateTrackingTable)
                    {
                        stringBuilder.Append(trackingTableBuilder.CreatePopulateFromBaseTableScriptText());
                    }
                }
                str = stringBuilder.ToString();
            }
            finally
            {
                if (!alreadyOpened)
                    connection.Close();
            }
            return str;
        }

    }
}
