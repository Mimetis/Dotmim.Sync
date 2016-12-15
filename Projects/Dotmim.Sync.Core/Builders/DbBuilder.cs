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
    public abstract class DbBuilder
    {
        private DbConnection connection;
        private bool useBulkProcedures;

        private DmTable tableDescription;


        /// <summary>
        /// Specify if we have to check for recreate or not
        /// </summary>
        public DbBuilderOption BuilderOption { get; set; }
        /// <summary>
        /// Gets or sets whether to insert metadata into the change-tracking table 
        /// for rows that already exist in the base table.
        /// </summary>
		public DbBuilderOption PopulateTrackingTable { get; set; }

        /// <summary>
        /// Gets or sets a value that indicates whether the bulk procedures 
        /// should be created during provisioning.
        /// </summary>
        public bool UseBulkProcedures { get; set; }

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
        /// You have to provider an proc builder implementation for your current database
        /// </summary>
        public abstract IDbBuilderProcedureHelper ProcBuilder { get; }

        /// <summary>
        /// You have to provider an trigger builder implementation for your current database
        /// </summary>
        public abstract IDbBuilderTriggerHelper TriggerBuilder { get; }

        /// <summary>
        /// You have to provider an trigger builder implementation for your current database
        /// </summary>
        public abstract IDbBuilderTableHelper TableBuilder { get; }

        /// <summary>
        /// You have to provider an tracking table builder implementation for your current database
        /// </summary>
        public abstract IDbBuilderTrackingTableHelper TrackingTableBuilder { get; }


        /// <summary>
        /// Construct a DbBuilder. You should provide
        /// </summary>
        public DbBuilder(DmTable table, DbConnection connection, DbBuilderOption option = DbBuilderOption.Create)
        {
            if (!table.PrimaryKey.HasValue)
                throw new Exception($"Table {table.TableName} must have at least one dmColumn as Primary key");


            this.tableDescription = table;
            this.connection = connection;


        }

        /// <summary>
        /// Check if the filter column exist and add the DmColumn
        /// </summary>
        public void AddFilterColumn(string name)
        {
            var column = this.tableDescription.Columns[name];

            if (column == null)
                throw new Exception($"Can't add this filter column, since the column doesn't exist in the current table {this.tableDescription.TableName}");

            this.FilterColumns.Add(column);
        }



        /// <summary>
        /// Apply the config.
        /// Create the table if needed
        /// </summary>
        public void Apply()
        {

            var alreadyOpened = this.connection.State != ConnectionState.Closed;


            try
            {
                if (!alreadyOpened)
                    this.connection.Open();

                using (var transaction = connection.BeginTransaction())
                {

                    if (this.TableBuilder.NeedToCreateTable(transaction, this.tableDescription))
                    {
                        this.TableBuilder.CreateTable(transaction);
                        this.TableBuilder.CreatePk(transaction);
                        this.TableBuilder.CreateForeignKeyConstraints(transaction);
                    }


                    if (this.TrackingTableBuilder.NeedToCreateTrackingTable(transaction, this.tableDescription, this.BuilderOption))
                    {
                        this.TrackingTableBuilder.FilterColumns = this.FilterColumns;
                        this.TrackingTableBuilder.CreateTable(transaction);
                        this.TrackingTableBuilder.CreatePk(transaction);
                        this.TrackingTableBuilder.CreateIndex(transaction);

                        if (this.PopulateTrackingTable == DbBuilderOption.Create || this.PopulateTrackingTable == DbBuilderOption.CreateOrUseExisting)
                            this.TrackingTableBuilder.PopulateFromBaseTable(transaction);

                    }
                    else if (this.FilterColumns.Count > 0)
                    {
                        List<string> strs = this.TableBuilder.GetColumnForTable(transaction, this.tableDescription.TableName);
                        bool flag = false;
                        foreach (var filterColumn in this.FilterColumns)
                        {
                            if (strs.Contains(filterColumn.ColumnName))
                                continue;

                            flag = true;
                            this.TrackingTableBuilder.AddFilterColumn(transaction, filterColumn);

                            this.TrackingTableBuilder.PopulateNewFilterColumnFromBaseTable(transaction, filterColumn);
                        }
                        if (flag)
                        {
                            this.TriggerBuilder.FilterColumns = this.GetExistingFilterColumns(transaction);
                            this.TriggerBuilder.AlterInsertTrigger(transaction);
                            this.TriggerBuilder.AlterUpdateTrigger(transaction);
                            this.TriggerBuilder.AlterDeleteTrigger(transaction);
                        }
                    }

                    this.TriggerBuilder.FilterColumns = this.FilterColumns;
                    this.TriggerBuilder.CreateInsertTrigger(transaction, this.BuilderOption);
                    this.TriggerBuilder.CreateUpdateTrigger(transaction, this.BuilderOption);
                    this.TriggerBuilder.CreateDeleteTrigger(transaction, this.BuilderOption);

                    this.ProcBuilder.FilterColumns = this.FilterColumns;
                    this.ProcBuilder.FilterParameters = this.FilterParameters;

                    this.ProcBuilder.CreateSelectRow(transaction, this.BuilderOption);
                    this.ProcBuilder.CreateInsert(transaction, this.BuilderOption);
                    this.ProcBuilder.CreateUpdate(transaction, this.BuilderOption);
                    this.ProcBuilder.CreateDelete(transaction, this.BuilderOption);
                    this.ProcBuilder.CreateInsertMetadata(transaction, this.BuilderOption);
                    this.ProcBuilder.CreateUpdateMetadata(transaction, this.BuilderOption);
                    this.ProcBuilder.CreateDeleteMetadata(transaction, this.BuilderOption);
                    if (this.useBulkProcedures)
                    {
                        this.ProcBuilder.CreateTVPType(transaction, this.BuilderOption);
                        this.ProcBuilder.CreateBulkInsert(transaction, this.BuilderOption);
                        this.ProcBuilder.CreateBulkUpdate(transaction, this.BuilderOption);
                        this.ProcBuilder.CreateBulkDelete(transaction, this.BuilderOption);
                    }


                    //commiting transaction
                    transaction.Commit();
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
                    this.connection.Close();
            }


        }


        /// <summary>
        /// Generate the creating script string (admin only)
        /// </summary>
        /// <returns></returns>
        public string Script()
        {
            string str = null;
            var alreadyOpened = this.connection.State != ConnectionState.Closed;
            bool needToCreateTrackingTable = false;

            try
            {
                if (!alreadyOpened)
                    this.connection.Open();

                using (var transaction = connection.BeginTransaction())
                {
                    Logger.Current.Info($"----- Scripting Provisioning of Table '{this.tableDescription.TableName}' -----");

                    StringBuilder stringBuilder = new StringBuilder();
                    if (this.TableBuilder.NeedToCreateTable(transaction, this.tableDescription))
                    {
                        stringBuilder.Append(this.TableBuilder.CreateTableScriptText());
                        stringBuilder.Append(this.TableBuilder.CreatePkScriptText());
                        stringBuilder.Append(this.TableBuilder.CreateForeignKeyConstraintsScriptText());
                    }


                    if (this.TrackingTableBuilder.NeedToCreateTrackingTable(transaction, this.tableDescription, this.BuilderOption))
                    {
                        this.TrackingTableBuilder.FilterColumns = this.FilterColumns;
                        stringBuilder.Append(this.TrackingTableBuilder.CreateTableScriptText());
                        stringBuilder.Append(this.TrackingTableBuilder.CreatePkScriptText());
                        stringBuilder.Append(this.TrackingTableBuilder.CreateIndexScriptText());
                        needToCreateTrackingTable = true;
                    }
                    else if (this.FilterColumns.Count > 0)
                    {
                        // Get the column from table
                        List<string> strs = this.TableBuilder.GetColumnForTable(transaction, this.tableDescription.TableName);

                        bool flag2 = false;
                        foreach (var filterColumn in this.FilterColumns)
                        {
                            if (strs.Contains(filterColumn.ColumnName))
                                continue;

                            Logger.Current.Info($"Filter column '{filterColumn.ColumnName}' needs to be added to triggers and side table");

                            flag2 = true;
                            stringBuilder.Append(this.TrackingTableBuilder.ScriptAddFilterColumn(filterColumn));
                            stringBuilder.Append(this.TrackingTableBuilder.ScriptPopulateNewFilterColumnFromBaseTable(filterColumn));
                        }
                        if (flag2)
                        {
                            this.TriggerBuilder.FilterColumns = this.GetExistingFilterColumns(transaction);
                            stringBuilder.Append(this.TriggerBuilder.AlterInsertTriggerScriptText());
                            stringBuilder.Append(this.TriggerBuilder.AlterUpdateTriggerScriptText());
                            stringBuilder.Append(this.TriggerBuilder.AlterDeleteTriggerScriptText());
                        }
                    }
                    this.TriggerBuilder.FilterColumns = this.FilterColumns;

                    stringBuilder.Append(this.TriggerBuilder.CreateInsertTriggerScriptText(transaction, this.BuilderOption));
                    stringBuilder.Append(this.TriggerBuilder.CreateUpdateTriggerScriptText(transaction, this.BuilderOption));
                    stringBuilder.Append(this.TriggerBuilder.CreateDeleteTriggerScriptText(transaction, this.BuilderOption));

                    this.ProcBuilder.FilterColumns = this.FilterColumns;
                    this.ProcBuilder.FilterParameters = this.FilterParameters;

                    stringBuilder.Append(this.ProcBuilder.CreateSelectRowScriptText(transaction, this.BuilderOption));
                    stringBuilder.Append(this.ProcBuilder.CreateInsertScriptText(transaction, this.BuilderOption));
                    stringBuilder.Append(this.ProcBuilder.CreateUpdateScriptText(transaction, this.BuilderOption));
                    stringBuilder.Append(this.ProcBuilder.CreateDeleteScriptText(transaction, this.BuilderOption));
                    stringBuilder.Append(this.ProcBuilder.CreateInsertMetadataScriptText(transaction, this.BuilderOption));
                    stringBuilder.Append(this.ProcBuilder.CreateUpdateMetadataScriptText(transaction, this.BuilderOption));
                    stringBuilder.Append(this.ProcBuilder.CreateDeleteMetadataScriptText(transaction, this.BuilderOption));

                    if (this.useBulkProcedures)
                    {
                        stringBuilder.Append(this.ProcBuilder.CreateTVPTypeScriptText(transaction, this.BuilderOption));
                        stringBuilder.Append(this.ProcBuilder.CreateBulkInsertScriptText(transaction, this.BuilderOption));
                        stringBuilder.Append(this.ProcBuilder.CreateBulkUpdateScriptText(transaction, this.BuilderOption));
                        stringBuilder.Append(this.ProcBuilder.CreateBulkDeleteScriptText(transaction, this.BuilderOption));
                    }
                    if (needToCreateTrackingTable && (this.PopulateTrackingTable == DbBuilderOption.Create || this.PopulateTrackingTable == DbBuilderOption.CreateOrUseExisting))
                    {
                        stringBuilder.Append(this.TrackingTableBuilder.CreatePopulateFromBaseTableScriptText());
                    }
                    str = stringBuilder.ToString();
                }
            }
            catch (Exception exception)
            {
                Logger.Current.Error(exception.Message);
                throw;
            }

            finally
            {
                if (!alreadyOpened)
                    this.connection.Close();
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
    }
}
