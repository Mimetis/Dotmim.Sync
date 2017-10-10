using Dotmim.Sync.Data;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;
using System.Linq;
using System.Data;
using Dotmim.Sync.Log;
using Dotmim.Sync.Filter;

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
        public DbBuilderOption BuilderOption { get; set; }

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
        public abstract DbSyncAdapter CreateSyncAdapter(DbConnection connection, DbTransaction transaction= null);
     
        /// <summary>
        /// Construct a DbBuilder
        /// </summary>
        public DbBuilder(DmTable tableDescription, DbBuilderOption option = DbBuilderOption.CreateOrUseExistingSchema)
        {
            this.TableDescription = tableDescription;
            this.BuilderOption = option;
        }

        /// <summary>
        /// Apply the config.
        /// Create the table if needed
        /// </summary>
        public void Apply(DbConnection connection, DbTransaction transaction = null)
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
                if (tableBuilder.NeedToCreateTable(this.BuilderOption))
                {
                    if (tableBuilder.NeedToCreateSchema(this.BuilderOption))
                        tableBuilder.CreateSchema();

                    tableBuilder.CreateTable();
                    tableBuilder.CreatePrimaryKey();
                    tableBuilder.CreateForeignKeyConstraints();
                }

                var trackingTableBuilder = CreateTrackingTableBuilder(connection, transaction);
                trackingTableBuilder.Filters = this.FilterColumns;

                if (trackingTableBuilder.NeedToCreateTrackingTable(this.BuilderOption))
                {
                    trackingTableBuilder.CreateTable();
                    trackingTableBuilder.CreatePk();
                    trackingTableBuilder.CreateIndex();

                    // Fill the tracking table with actual rows from base table
                    trackingTableBuilder.PopulateFromBaseTable();
                }

                var triggerBuilder = CreateTriggerBuilder(connection, transaction);
                triggerBuilder.Filters = this.FilterColumns;

                if (triggerBuilder.NeedToCreateTrigger(DbTriggerType.Insert, this.BuilderOption))
                    triggerBuilder.CreateInsertTrigger();
                if (triggerBuilder.NeedToCreateTrigger(DbTriggerType.Update, this.BuilderOption))
                    triggerBuilder.CreateUpdateTrigger();
                if (triggerBuilder.NeedToCreateTrigger(DbTriggerType.Delete, this.BuilderOption))
                    triggerBuilder.CreateDeleteTrigger();

                // could be null
                var procBuilder = CreateProcBuilder(connection, transaction);
                if (procBuilder == null)
                    return;

                procBuilder.Filters = this.FilterColumns;
                
                if (procBuilder.NeedToCreateProcedure(DbCommandType.SelectChanges, this.BuilderOption))
                    procBuilder.CreateSelectIncrementalChanges();
                if (procBuilder.NeedToCreateProcedure(DbCommandType.SelectRow, this.BuilderOption))
                    procBuilder.CreateSelectRow();
                if (procBuilder.NeedToCreateProcedure(DbCommandType.InsertRow, this.BuilderOption))
                    procBuilder.CreateInsert();
                if (procBuilder.NeedToCreateProcedure(DbCommandType.UpdateRow, this.BuilderOption))
                    procBuilder.CreateUpdate();
                if (procBuilder.NeedToCreateProcedure(DbCommandType.DeleteRow, this.BuilderOption))
                    procBuilder.CreateDelete();
                if (procBuilder.NeedToCreateProcedure(DbCommandType.InsertMetadata, this.BuilderOption))
                    procBuilder.CreateInsertMetadata();
                if (procBuilder.NeedToCreateProcedure(DbCommandType.UpdateMetadata, this.BuilderOption))
                    procBuilder.CreateUpdateMetadata();
                if (procBuilder.NeedToCreateProcedure(DbCommandType.DeleteMetadata, this.BuilderOption))
                    procBuilder.CreateDeleteMetadata();

                if (this.useBulkProcedures && procBuilder.NeedToCreateType(DbCommandType.BulkTableType, this.BuilderOption))
                {
                    procBuilder.CreateTVPType();
                    procBuilder.CreateBulkInsert();
                    procBuilder.CreateBulkUpdate();
                    procBuilder.CreateBulkDelete();
                }


            }
            catch (Exception ex)
            {
                Logger.Current.Error(ex.Message);
                throw;
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
        public string Script(DbConnection connection, DbTransaction transaction = null)
        {
            string str = null;
            var alreadyOpened = connection.State != ConnectionState.Closed;
            bool needToCreateTrackingTable = false;

            try
            {
                if (!alreadyOpened)
                    connection.Open();

                Logger.Current.Info($"----- Scripting Provisioning of Table '{TableDescription.TableName}' -----");

                StringBuilder stringBuilder = new StringBuilder();

                var tableBuilder = CreateTableBuilder(connection, transaction);

                // Check if we need to create the tables
                if (tableBuilder.NeedToCreateTable(this.BuilderOption))
                {
                    if (tableBuilder.NeedToCreateSchema(this.BuilderOption))
                        stringBuilder.Append(tableBuilder.CreateSchemaScriptText());

                    stringBuilder.Append(tableBuilder.CreateTableScriptText());
                    stringBuilder.Append(tableBuilder.CreatePrimaryKeyScriptText());
                    stringBuilder.Append(tableBuilder.CreateForeignKeyConstraintsScriptText());
                }

                var trackingTableBuilder = CreateTrackingTableBuilder(connection, transaction);
                trackingTableBuilder.Filters = this.FilterColumns;

                if (trackingTableBuilder.NeedToCreateTrackingTable(this.BuilderOption))
                {
                    stringBuilder.Append(trackingTableBuilder.CreateTableScriptText());
                    stringBuilder.Append(trackingTableBuilder.CreatePkScriptText());
                    stringBuilder.Append(trackingTableBuilder.CreateIndexScriptText());

                    needToCreateTrackingTable = true;
                }
                var triggerBuilder = CreateTriggerBuilder(connection, transaction);
                triggerBuilder.Filters = this.FilterColumns;

                if (triggerBuilder.NeedToCreateTrigger(DbTriggerType.Insert, this.BuilderOption))
                    stringBuilder.Append(triggerBuilder.CreateInsertTriggerScriptText());
                if (triggerBuilder.NeedToCreateTrigger(DbTriggerType.Update, this.BuilderOption))
                    stringBuilder.Append(triggerBuilder.CreateUpdateTriggerScriptText());
                if (triggerBuilder.NeedToCreateTrigger(DbTriggerType.Delete, this.BuilderOption))
                    stringBuilder.Append(triggerBuilder.CreateDeleteTriggerScriptText());


                var procBuilder = CreateProcBuilder(connection, transaction);

                if (procBuilder != null)
                {
                    procBuilder.Filters = this.FilterColumns;
                 
                    if (procBuilder.NeedToCreateProcedure(DbCommandType.SelectChanges, this.BuilderOption))
                        stringBuilder.Append(procBuilder.CreateSelectIncrementalChangesScriptText());
                    if (procBuilder.NeedToCreateProcedure(DbCommandType.SelectRow, this.BuilderOption))
                        stringBuilder.Append(procBuilder.CreateSelectRowScriptText());
                    if (procBuilder.NeedToCreateProcedure(DbCommandType.InsertRow, this.BuilderOption))
                        stringBuilder.Append(procBuilder.CreateInsertScriptText());
                    if (procBuilder.NeedToCreateProcedure(DbCommandType.UpdateRow, this.BuilderOption))
                        stringBuilder.Append(procBuilder.CreateUpdateScriptText());
                    if (procBuilder.NeedToCreateProcedure(DbCommandType.DeleteRow, this.BuilderOption))
                        stringBuilder.Append(procBuilder.CreateDeleteScriptText());
                    if (procBuilder.NeedToCreateProcedure(DbCommandType.InsertMetadata, this.BuilderOption))
                        stringBuilder.Append(procBuilder.CreateInsertMetadataScriptText());
                    if (procBuilder.NeedToCreateProcedure(DbCommandType.UpdateMetadata, this.BuilderOption))
                        stringBuilder.Append(procBuilder.CreateUpdateMetadataScriptText());
                    if (procBuilder.NeedToCreateProcedure(DbCommandType.DeleteMetadata, this.BuilderOption))
                        stringBuilder.Append(procBuilder.CreateDeleteMetadataScriptText());

                    if (this.useBulkProcedures && procBuilder.NeedToCreateType(DbCommandType.BulkTableType, this.BuilderOption))
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
                Logger.Current.Error(exception.Message);
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
