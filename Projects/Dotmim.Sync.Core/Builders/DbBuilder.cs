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
        private bool useBulkProcedures = true;

        /// <summary>
        /// Gets the table description for the current DbBuilder
        /// </summary>
        public DmTable TableDescription { get; set; }

        /// <summary>
        /// Specify if we have to check for recreate or not
        /// </summary>
        //public DbBuilderOption BuilderOption { get; set; }

        /// <summary>
        /// Filtered Columns
        /// </summary>
        public FilterClauseCollection FilterColumns { get; set; } = new FilterClauseCollection();

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
        public DbBuilder(DmTable tableDescription)
        {
            this.TableDescription = tableDescription;
        }


        /// <summary>
        /// Apply config.
        /// Create relations if needed
        /// </summary>
        public void CreateForeignKeys(DbConnection connection, DbTransaction transaction = null)
        {
            if (!TableDescription.PrimaryKey.HasValue)
                throw new Exception($"Table {TableDescription.TableName} must have at least one dmColumn as Primary key");

            var alreadyOpened = connection.State != ConnectionState.Closed;

            try
            {
                if (!alreadyOpened)
                    connection.Open();

                var tableBuilder = CreateTableBuilder(connection, transaction);

                if (this.TableDescription.ChildRelations != null && this.TableDescription.ChildRelations.Count > 0)
                {
                    foreach (DmRelation constraint in this.TableDescription.ChildRelations)
                    {
                        // Check if we need to create the foreign key constraint
                        if (tableBuilder.NeedToCreateForeignKeyConstraints(constraint))
                        {
                            tableBuilder.CreateForeignKeyConstraints(constraint);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine(ex.Message);
                throw;
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
        public void CreateTable(DbConnection connection, DbTransaction transaction = null)
        {
            if (!TableDescription.PrimaryKey.HasValue)
                throw new Exception($"Table {TableDescription.TableName} must have at least one dmColumn as Primary key");

            var alreadyOpened = connection.State != ConnectionState.Closed;

            try
            {
                if (!alreadyOpened)
                    connection.Open();

                var tableBuilder = CreateTableBuilder(connection, transaction);

                // Check if we need to create the tables
                if (tableBuilder.NeedToCreateTable())
                {
                    if (tableBuilder.NeedToCreateSchema())
                        tableBuilder.CreateSchema();

                    tableBuilder.CreateTable();
                    tableBuilder.CreatePrimaryKey();
                }

                var trackingTableBuilder = CreateTrackingTableBuilder(connection, transaction);
                trackingTableBuilder.Filters = this.FilterColumns;

                if (trackingTableBuilder.NeedToCreateTrackingTable())
                {
                    trackingTableBuilder.CreateTable();
                    trackingTableBuilder.CreatePk();
                    trackingTableBuilder.CreateIndex();

                    // Fill the tracking table with actual rows from base table
                    trackingTableBuilder.PopulateFromBaseTable();
                }

                var triggerBuilder = CreateTriggerBuilder(connection, transaction);
                triggerBuilder.Filters = this.FilterColumns;

                if (triggerBuilder.NeedToCreateTrigger(DbTriggerType.Insert))
                    triggerBuilder.CreateInsertTrigger();
                if (triggerBuilder.NeedToCreateTrigger(DbTriggerType.Update))
                    triggerBuilder.CreateUpdateTrigger();
                if (triggerBuilder.NeedToCreateTrigger(DbTriggerType.Delete))
                    triggerBuilder.CreateDeleteTrigger();

                // could be null
                var procBuilder = CreateProcBuilder(connection, transaction);
                if (procBuilder == null)
                    return;

                procBuilder.Filters = this.FilterColumns;

                if (procBuilder.NeedToCreateProcedure(DbCommandType.SelectChanges))
                    procBuilder.CreateSelectIncrementalChanges();
                if (procBuilder.NeedToCreateProcedure(DbCommandType.SelectRow))
                    procBuilder.CreateSelectRow();
                if (procBuilder.NeedToCreateProcedure(DbCommandType.InsertRow))
                    procBuilder.CreateInsert();
                if (procBuilder.NeedToCreateProcedure(DbCommandType.UpdateRow))
                    procBuilder.CreateUpdate();
                if (procBuilder.NeedToCreateProcedure(DbCommandType.DeleteRow))
                    procBuilder.CreateDelete();
                if (procBuilder.NeedToCreateProcedure(DbCommandType.InsertMetadata))
                    procBuilder.CreateInsertMetadata();
                if (procBuilder.NeedToCreateProcedure(DbCommandType.UpdateMetadata))
                    procBuilder.CreateUpdateMetadata();
                if (procBuilder.NeedToCreateProcedure(DbCommandType.DeleteMetadata))
                    procBuilder.CreateDeleteMetadata();
                if (procBuilder.NeedToCreateProcedure(DbCommandType.Reset))
                    procBuilder.CreateReset();

                if (this.useBulkProcedures && procBuilder.NeedToCreateType(DbCommandType.BulkTableType))
                {
                    procBuilder.CreateTVPType();
                    procBuilder.CreateBulkInsert();
                    procBuilder.CreateBulkUpdate();
                    procBuilder.CreateBulkDelete();
                }


            }
            catch (Exception ex)
            {
                Debug.WriteLine(ex.Message);
                throw;
            }
            finally
            {
                if (!alreadyOpened)
                    connection.Close();
            }


        }


        public string ScriptForeignKeys(DbConnection connection, DbTransaction transaction = null)
        {
            string str = null;
            var alreadyOpened = connection.State != ConnectionState.Closed;

            try
            {
                if (!alreadyOpened)
                    connection.Open();

                Debug.WriteLine($"----- Scripting Provisioning of Table '{TableDescription.TableName}' -----");

                StringBuilder stringBuilder = new StringBuilder();

                var tableBuilder = CreateTableBuilder(connection, transaction);

                if (this.TableDescription.ChildRelations != null && this.TableDescription.ChildRelations.Count > 0)
                {
                    foreach (DmRelation constraint in this.TableDescription.ChildRelations)
                    {
                        if (tableBuilder.NeedToCreateForeignKeyConstraints(constraint))
                            stringBuilder.Append(tableBuilder.CreateForeignKeyConstraintsScriptText(constraint));
                    }
                }

                str = stringBuilder.ToString();
            }
            catch (Exception exception)
            {
                Debug.WriteLine(exception.Message);
                throw;
            }

            finally
            {
                if (!alreadyOpened)
                    connection.Close();
            }
            return str;
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

                Debug.WriteLine($"----- Scripting Provisioning of Table '{TableDescription.TableName}' -----");

                StringBuilder stringBuilder = new StringBuilder();

                var tableBuilder = CreateTableBuilder(connection, transaction);

                // Check if we need to create the tables
                if (tableBuilder.NeedToCreateTable())
                {
                    if (tableBuilder.NeedToCreateSchema())
                        stringBuilder.Append(tableBuilder.CreateSchemaScriptText());

                    stringBuilder.Append(tableBuilder.CreateTableScriptText());
                    stringBuilder.Append(tableBuilder.CreatePrimaryKeyScriptText());
                }

                var trackingTableBuilder = CreateTrackingTableBuilder(connection, transaction);
                trackingTableBuilder.Filters = this.FilterColumns;

                if (trackingTableBuilder.NeedToCreateTrackingTable())
                {
                    stringBuilder.Append(trackingTableBuilder.CreateTableScriptText());
                    stringBuilder.Append(trackingTableBuilder.CreatePkScriptText());
                    stringBuilder.Append(trackingTableBuilder.CreateIndexScriptText());

                    needToCreateTrackingTable = true;
                }
                var triggerBuilder = CreateTriggerBuilder(connection, transaction);
                triggerBuilder.Filters = this.FilterColumns;

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
                    if (procBuilder.NeedToCreateProcedure(DbCommandType.SelectRow))
                        stringBuilder.Append(procBuilder.CreateSelectRowScriptText());
                    if (procBuilder.NeedToCreateProcedure(DbCommandType.InsertRow))
                        stringBuilder.Append(procBuilder.CreateInsertScriptText());
                    if (procBuilder.NeedToCreateProcedure(DbCommandType.UpdateRow))
                        stringBuilder.Append(procBuilder.CreateUpdateScriptText());
                    if (procBuilder.NeedToCreateProcedure(DbCommandType.DeleteRow))
                        stringBuilder.Append(procBuilder.CreateDeleteScriptText());
                    if (procBuilder.NeedToCreateProcedure(DbCommandType.InsertMetadata))
                        stringBuilder.Append(procBuilder.CreateInsertMetadataScriptText());
                    if (procBuilder.NeedToCreateProcedure(DbCommandType.UpdateMetadata))
                        stringBuilder.Append(procBuilder.CreateUpdateMetadataScriptText());
                    if (procBuilder.NeedToCreateProcedure(DbCommandType.DeleteMetadata))
                        stringBuilder.Append(procBuilder.CreateDeleteMetadataScriptText());
                    if (procBuilder.NeedToCreateProcedure(DbCommandType.Reset))
                        stringBuilder.Append(procBuilder.CreateResetScriptText());

                    if (this.useBulkProcedures && procBuilder.NeedToCreateType(DbCommandType.BulkTableType))
                    {
                        stringBuilder.Append(procBuilder.CreateTVPTypeScriptText());
                        stringBuilder.Append(procBuilder.CreateBulkInsertScriptText());
                        stringBuilder.Append(procBuilder.CreateBulkUpdateScriptText());
                        stringBuilder.Append(procBuilder.CreateBulkDeleteScriptText());
                    }
                    if (needToCreateTrackingTable)
                    {
                        stringBuilder.Append(trackingTableBuilder.CreatePopulateFromBaseTableScriptText());
                    }
                }
                str = stringBuilder.ToString();

            }
            catch (Exception exception)
            {
                Debug.WriteLine(exception.Message);
                throw;
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
