using Dotmim.Sync.Data;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;
using System.Linq;
using System.Data;
using Dotmim.Sync.Core.Log;

namespace Dotmim.Sync.Core.Builders
{
    public abstract class DbBuilder : IDisposable
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
        /// Gets or sets the SQL WHERE clause (without the WHERE keyword) that is used 
        /// to filter the result set from the base table.
        /// </summary>
		public string FilterClause { get; set; }

        /// <summary>
        /// Gets the list of columns that were added by using 
        /// AddFilterColumn(System.String)
        /// </summary>
        public List<DmColumn> FilterColumns { get; } = new List<DmColumn>();

        /// <summary>
        /// Gets the list of filter parameters that are used to control which items are enumerated.
        /// </summary>
        public List<DmColumn> FilterParameters { get; } = new List<DmColumn>();

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
        /// Check if the filter column exist and add the DmColumn
        /// </summary>
        public void AddFilterColumn(string name)
        {
            var column = TableDescription.Columns[name];

            if (column == null)
                throw new Exception($"Can't add this filter column, since the column doesn't exist in the current table {TableDescription.TableName}");

            this.FilterColumns.Add(column);
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
                    tableBuilder.CreateTable();
                    tableBuilder.CreatePrimaryKey();
                    tableBuilder.CreateForeignKeyConstraints();
                }

                var trackingTableBuilder = CreateTrackingTableBuilder(connection, transaction);
                trackingTableBuilder.FilterColumns = this.FilterColumns;

                if (trackingTableBuilder.NeedToCreateTrackingTable(this.BuilderOption))
                {
                    trackingTableBuilder.CreateTable();
                    trackingTableBuilder.CreatePk();
                    trackingTableBuilder.CreateIndex();

                    // Fill the tracking table with actual rows from base table
                    trackingTableBuilder.PopulateFromBaseTable();
                }

                var triggerBuilder = CreateTriggerBuilder(connection, transaction);
                triggerBuilder.FilterColumns = this.FilterColumns;

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

                procBuilder.FilterColumns = this.FilterColumns;
                procBuilder.FilterParameters = this.FilterParameters;

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
                    stringBuilder.Append(tableBuilder.CreateTableScriptText());
                    stringBuilder.Append(tableBuilder.CreatePrimaryKeyScriptText());
                    stringBuilder.Append(tableBuilder.CreateForeignKeyConstraintsScriptText());
                }

                var trackingTableBuilder = CreateTrackingTableBuilder(connection, transaction);
                trackingTableBuilder.FilterColumns = this.FilterColumns;

                if (trackingTableBuilder.NeedToCreateTrackingTable(this.BuilderOption))
                {
                    stringBuilder.Append(trackingTableBuilder.CreateTableScriptText());
                    stringBuilder.Append(trackingTableBuilder.CreatePkScriptText());
                    stringBuilder.Append(trackingTableBuilder.CreateIndexScriptText());

                    needToCreateTrackingTable = true;
                }
                var triggerBuilder = CreateTriggerBuilder(connection, transaction);
                triggerBuilder.FilterColumns = this.FilterColumns;

                if (triggerBuilder.NeedToCreateTrigger(DbTriggerType.Insert, this.BuilderOption))
                    stringBuilder.Append(triggerBuilder.CreateInsertTriggerScriptText());
                if (triggerBuilder.NeedToCreateTrigger(DbTriggerType.Update, this.BuilderOption))
                    stringBuilder.Append(triggerBuilder.CreateUpdateTriggerScriptText());
                if (triggerBuilder.NeedToCreateTrigger(DbTriggerType.Delete, this.BuilderOption))
                    stringBuilder.Append(triggerBuilder.CreateDeleteTriggerScriptText());


                var procBuilder = CreateProcBuilder(connection, transaction);

                if (procBuilder != null)
                {
                    procBuilder.FilterColumns = this.FilterColumns;
                    procBuilder.FilterParameters = this.FilterParameters;

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

        private List<DmColumn> GetExistingFilterColumns(DbTransaction transaction)
        {
            throw new NotImplementedException();
        }

        private void AddNewFilterColumnsToTrackingTable(DbTransaction transaction)
        {
            throw new NotImplementedException();
        }

        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    
                }

                // TODO: free unmanaged resources (unmanaged objects) and override a finalizer below.
                // TODO: set large fields to null.

                disposedValue = true;
            }
        }



        // This code added to correctly implement the disposable pattern.
        public void Dispose()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(true);
            // TODO: uncomment the following line if the finalizer is overridden above.
            // GC.SuppressFinalize(this);
        }
        #endregion
    }
}
