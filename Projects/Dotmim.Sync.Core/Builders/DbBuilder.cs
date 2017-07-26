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
        public DmTable TableDescription { get; private set; }
        
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
        /// ObjectNames
        /// </summary>
        public abstract DbObjectNames ObjectNames { get; }

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
                tableBuilder.TableDescription = this.TableDescription;

                // Check if we need to create the tables
                if (tableBuilder.NeedToCreateTable(this.BuilderOption))
                {
                    tableBuilder.CreateTable();
                    tableBuilder.CreatePrimaryKey();
                    tableBuilder.CreateForeignKeyConstraints();
                }

                var trackingTableBuilder = CreateTrackingTableBuilder(connection, transaction);
                trackingTableBuilder.TableDescription = this.TableDescription;
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
                triggerBuilder.TableDescription = this.TableDescription;
                triggerBuilder.FilterColumns = this.FilterColumns;
                triggerBuilder.ObjectNames = this.ObjectNames;

                if (triggerBuilder.NeedToCreateTrigger(DbTriggerType.Insert, this.BuilderOption))
                    triggerBuilder.CreateInsertTrigger();
                if (triggerBuilder.NeedToCreateTrigger(DbTriggerType.Update, this.BuilderOption))
                    triggerBuilder.CreateUpdateTrigger();
                if (triggerBuilder.NeedToCreateTrigger(DbTriggerType.Delete, this.BuilderOption))
                    triggerBuilder.CreateDeleteTrigger();

                var procBuilder = CreateProcBuilder(connection, transaction);
                procBuilder.TableDescription = this.TableDescription;
                procBuilder.ObjectNames = this.ObjectNames;
                procBuilder.FilterColumns = this.FilterColumns;
                procBuilder.FilterParameters = this.FilterParameters;

                if (procBuilder.NeedToCreateProcedure(ObjectNames.GetObjectName(DbObjectType.SelectChangesProcName), this.BuilderOption))
                    procBuilder.CreateSelectIncrementalChanges(ObjectNames.GetObjectName(DbObjectType.SelectChangesProcName));
                if (procBuilder.NeedToCreateProcedure(ObjectNames.GetObjectName(DbObjectType.SelectRowProcName), this.BuilderOption))
                    procBuilder.CreateSelectRow(ObjectNames.GetObjectName(DbObjectType.SelectRowProcName));
                if (procBuilder.NeedToCreateProcedure(ObjectNames.GetObjectName(DbObjectType.InsertProcName), this.BuilderOption))
                    procBuilder.CreateInsert(ObjectNames.GetObjectName(DbObjectType.InsertProcName));
                if (procBuilder.NeedToCreateProcedure(ObjectNames.GetObjectName(DbObjectType.UpdateProcName), this.BuilderOption))
                    procBuilder.CreateUpdate(ObjectNames.GetObjectName(DbObjectType.UpdateProcName));
                if (procBuilder.NeedToCreateProcedure(ObjectNames.GetObjectName(DbObjectType.DeleteProcName), this.BuilderOption))
                    procBuilder.CreateDelete(ObjectNames.GetObjectName(DbObjectType.DeleteProcName));
                if (procBuilder.NeedToCreateProcedure(ObjectNames.GetObjectName(DbObjectType.InsertMetadataProcName), this.BuilderOption))
                    procBuilder.CreateInsertMetadata(ObjectNames.GetObjectName(DbObjectType.InsertMetadataProcName));
                if (procBuilder.NeedToCreateProcedure(ObjectNames.GetObjectName(DbObjectType.UpdateMetadataProcName), this.BuilderOption))
                    procBuilder.CreateUpdateMetadata(ObjectNames.GetObjectName(DbObjectType.UpdateMetadataProcName));
                if (procBuilder.NeedToCreateProcedure(ObjectNames.GetObjectName(DbObjectType.DeleteMetadataProcName), this.BuilderOption))
                    procBuilder.CreateDeleteMetadata(ObjectNames.GetObjectName(DbObjectType.DeleteMetadataProcName));

                if (this.useBulkProcedures && procBuilder.NeedToCreateType(ObjectNames.GetObjectName(DbObjectType.BulkTableTypeName), this.BuilderOption))
                {
                    procBuilder.CreateTVPType(ObjectNames.GetObjectName(DbObjectType.BulkTableTypeName));
                    procBuilder.CreateBulkInsert(ObjectNames.GetObjectName(DbObjectType.BulkInsertProcName));
                    procBuilder.CreateBulkUpdate(ObjectNames.GetObjectName(DbObjectType.BulkUpdateProcName));
                    procBuilder.CreateBulkDelete(ObjectNames.GetObjectName(DbObjectType.BulkDeleteProcName));
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
                tableBuilder.TableDescription = this.TableDescription;

                // Check if we need to create the tables
                if (tableBuilder.NeedToCreateTable(this.BuilderOption))
                {
                    stringBuilder.Append(tableBuilder.CreateTableScriptText());
                    stringBuilder.Append(tableBuilder.CreatePrimaryKeyScriptText());
                    stringBuilder.Append(tableBuilder.CreateForeignKeyConstraintsScriptText());
                }

                var trackingTableBuilder = CreateTrackingTableBuilder(connection, transaction);
                trackingTableBuilder.TableDescription = this.TableDescription;
                trackingTableBuilder.FilterColumns = this.FilterColumns;

                if (trackingTableBuilder.NeedToCreateTrackingTable(this.BuilderOption))
                {
                    stringBuilder.Append(trackingTableBuilder.CreateTableScriptText());
                    stringBuilder.Append(trackingTableBuilder.CreatePkScriptText());
                    stringBuilder.Append(trackingTableBuilder.CreateIndexScriptText());

                    needToCreateTrackingTable = true;
                }
                var triggerBuilder = CreateTriggerBuilder(connection, transaction);
                triggerBuilder.TableDescription = TableDescription;
                triggerBuilder.ObjectNames = this.ObjectNames;
                triggerBuilder.FilterColumns = this.FilterColumns;

                if (triggerBuilder.NeedToCreateTrigger(DbTriggerType.Insert, this.BuilderOption))
                    stringBuilder.Append(triggerBuilder.CreateInsertTriggerScriptText());
                if (triggerBuilder.NeedToCreateTrigger(DbTriggerType.Update, this.BuilderOption))
                    stringBuilder.Append(triggerBuilder.CreateUpdateTriggerScriptText());
                if (triggerBuilder.NeedToCreateTrigger(DbTriggerType.Delete, this.BuilderOption))
                    stringBuilder.Append(triggerBuilder.CreateDeleteTriggerScriptText());

                var procBuilder = CreateProcBuilder(connection, transaction);
                procBuilder.TableDescription = this.TableDescription;
                procBuilder.ObjectNames = this.ObjectNames;
                procBuilder.FilterColumns = this.FilterColumns;
                procBuilder.FilterParameters = this.FilterParameters;

                if (procBuilder.NeedToCreateProcedure(ObjectNames.GetObjectName(DbObjectType.SelectChangesProcName), this.BuilderOption))
                    stringBuilder.Append(procBuilder.CreateSelectIncrementalChangesScriptText(ObjectNames.GetObjectName(DbObjectType.SelectChangesProcName)));
                if (procBuilder.NeedToCreateProcedure(ObjectNames.GetObjectName(DbObjectType.SelectRowProcName), this.BuilderOption))
                    stringBuilder.Append(procBuilder.CreateSelectRowScriptText(ObjectNames.GetObjectName(DbObjectType.SelectRowProcName)));
                if (procBuilder.NeedToCreateProcedure(ObjectNames.GetObjectName(DbObjectType.InsertProcName), this.BuilderOption))
                    stringBuilder.Append(procBuilder.CreateInsertScriptText(ObjectNames.GetObjectName(DbObjectType.InsertProcName)));
                if (procBuilder.NeedToCreateProcedure(ObjectNames.GetObjectName(DbObjectType.UpdateProcName), this.BuilderOption))
                    stringBuilder.Append(procBuilder.CreateUpdateScriptText(ObjectNames.GetObjectName(DbObjectType.UpdateProcName)));
                if (procBuilder.NeedToCreateProcedure(ObjectNames.GetObjectName(DbObjectType.DeleteProcName), this.BuilderOption))
                    stringBuilder.Append(procBuilder.CreateDeleteScriptText(ObjectNames.GetObjectName(DbObjectType.DeleteProcName)));
                if (procBuilder.NeedToCreateProcedure(ObjectNames.GetObjectName(DbObjectType.InsertMetadataProcName), this.BuilderOption))
                    stringBuilder.Append(procBuilder.CreateInsertMetadataScriptText(ObjectNames.GetObjectName(DbObjectType.InsertMetadataProcName)));
                if (procBuilder.NeedToCreateProcedure(ObjectNames.GetObjectName(DbObjectType.UpdateMetadataProcName), this.BuilderOption))
                    stringBuilder.Append(procBuilder.CreateUpdateMetadataScriptText(ObjectNames.GetObjectName(DbObjectType.UpdateMetadataProcName)));
                if (procBuilder.NeedToCreateProcedure(ObjectNames.GetObjectName(DbObjectType.DeleteMetadataProcName), this.BuilderOption))
                    stringBuilder.Append(procBuilder.CreateDeleteMetadataScriptText(ObjectNames.GetObjectName(DbObjectType.DeleteMetadataProcName)));

                if (this.useBulkProcedures && procBuilder.NeedToCreateType(ObjectNames.GetObjectName(DbObjectType.BulkTableTypeName), this.BuilderOption))
                {
                    stringBuilder.Append(procBuilder.CreateTVPTypeScriptText(ObjectNames.GetObjectName(DbObjectType.BulkTableTypeName)));
                    stringBuilder.Append(procBuilder.CreateBulkInsertScriptText(ObjectNames.GetObjectName(DbObjectType.BulkInsertProcName)));
                    stringBuilder.Append(procBuilder.CreateBulkUpdateScriptText(ObjectNames.GetObjectName(DbObjectType.BulkUpdateProcName)));
                    stringBuilder.Append(procBuilder.CreateBulkDeleteScriptText(ObjectNames.GetObjectName(DbObjectType.BulkDeleteProcName)));
                }
                if (needToCreateTrackingTable)
                {
                    stringBuilder.Append(trackingTableBuilder.CreatePopulateFromBaseTableScriptText());
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
