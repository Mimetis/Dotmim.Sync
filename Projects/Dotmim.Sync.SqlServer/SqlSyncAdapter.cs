using Dotmim.Sync.Builders;
using Dotmim.Sync.SqlServer.Manager;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using Microsoft.Data.SqlClient;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient.Server;
using Dotmim.Sync.Enumerations;

namespace Dotmim.Sync.SqlServer.Builders
{
    public partial class SqlSyncAdapter : DbSyncAdapter
    {

        // Derive Parameters cache
        // Be careful, we can have collision between databases
        // this static class could be shared accross databases with same command name
        // but different table schema
        // So the string should contains the connection string as well
        private static ConcurrentDictionary<string, List<SqlParameter>> derivingParameters
            = new ConcurrentDictionary<string, List<SqlParameter>>();

        public static DateTime SqlDateMin = new DateTime(1753, 1, 1);

        public static DateTime SqlSmallDateMin = new DateTime(1900, 1, 1);
        public SqlObjectNames SqlObjectNames { get; }
        public SqlDbMetadata SqlMetadata { get; }

        private readonly ParserName tableName;
        private readonly ParserName trackingName;

        public SqlSyncAdapter(SyncTable tableDescription, ParserName tableName, ParserName trackingName, SyncSetup setup, string scopeName, bool useBulkOperations) : base(tableDescription, setup, scopeName, useBulkOperations)
        {
            this.SqlObjectNames = new SqlObjectNames(tableDescription, tableName, trackingName, setup, scopeName);
            this.SqlMetadata = new SqlDbMetadata();
            this.tableName = tableName;
            this.trackingName = trackingName;
        }


        public override (DbCommand, bool) GetCommand(DbCommandType nameType, SyncFilter filter)
        {
            var command = new SqlCommand();
            bool isBatch;
            switch (nameType)
            {
                case DbCommandType.SelectChanges:
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandText = this.SqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.SelectChanges, filter);
                    isBatch = false;
                    break;
                case DbCommandType.SelectInitializedChanges:
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandText = this.SqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.SelectInitializedChanges, filter);
                    isBatch = false;
                    break;
                case DbCommandType.SelectInitializedChangesWithFilters:
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandText = this.SqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.SelectInitializedChangesWithFilters, filter);
                    isBatch = false;
                    break;
                case DbCommandType.SelectChangesWithFilters:
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandText = this.SqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.SelectChangesWithFilters, filter);
                    isBatch = false;
                    break;
                case DbCommandType.SelectRow:
                    command.CommandType = CommandType.Text;
                    command.CommandText = this.SqlObjectNames.GetCommandName(DbCommandType.SelectRow, filter);
                    isBatch = false;
                    break;
                case DbCommandType.UpdateRow:
                case DbCommandType.InsertRow:
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandText = this.SqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.UpdateRow, filter);
                    isBatch = false;
                    break;
                case DbCommandType.UpdateRows:
                case DbCommandType.InsertRows:
                    command.CommandType = CommandType.StoredProcedure;
                    if (this.UseBulkOperations)
                    {
                        command.CommandText = this.SqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.BulkUpdateRows, filter);
                        isBatch = true;
                    }
                    else
                    {
                        command.CommandText = this.SqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.UpdateRow, filter);
                        isBatch = false;
                    }
                    break;
                case DbCommandType.DeleteRow:
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandText = this.SqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.DeleteRow, filter);
                    isBatch = false;
                    break;
                case DbCommandType.DeleteRows:
                    command.CommandType = CommandType.StoredProcedure;
                    if (this.UseBulkOperations)
                    {
                        command.CommandText = this.SqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.BulkDeleteRows, filter);
                        isBatch = true;
                    }
                    else
                    {
                        command.CommandText = this.SqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.DeleteRow, filter);
                        isBatch = false;
                    }
                    break;
                case DbCommandType.DisableConstraints:
                    command.CommandType = CommandType.Text;
                    command.CommandText = this.SqlObjectNames.GetCommandName(DbCommandType.DisableConstraints, filter);
                    isBatch = false;
                    break;
                case DbCommandType.EnableConstraints:
                    command.CommandType = CommandType.Text;
                    command.CommandText = this.SqlObjectNames.GetCommandName(DbCommandType.EnableConstraints, filter);
                    isBatch = false;
                    break;
                case DbCommandType.DeleteMetadata:
                    //command.CommandType = CommandType.StoredProcedure;
                    //command.CommandText = this.SqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.DeleteMetadata, filter);
                    //isBatch = false;
                    //break;
                    return (default, false);

                case DbCommandType.UpdateMetadata:
                    //command.CommandType = CommandType.Text;
                    //command.CommandText = this.SqlObjectNames.GetCommandName(DbCommandType.UpdateMetadata, filter);
                    //isBatch = false;
                    //break;
                    return (default, false);

                case DbCommandType.SelectMetadata:
                    //command.CommandType = CommandType.Text;
                    //command.CommandText = this.SqlObjectNames.GetCommandName(DbCommandType.SelectMetadata, filter);
                    //isBatch = false;
                    //break;
                    return (default, false);
                case DbCommandType.InsertTrigger:
                    command.CommandType = CommandType.Text;
                    command.CommandText = this.SqlObjectNames.GetTriggerCommandName(DbTriggerType.Insert, filter);
                    isBatch = false;
                    break;
                case DbCommandType.UpdateTrigger:
                    command.CommandType = CommandType.Text;
                    command.CommandText = this.SqlObjectNames.GetTriggerCommandName(DbTriggerType.Update, filter);
                    isBatch = false;
                    break;
                case DbCommandType.DeleteTrigger:
                    command.CommandType = CommandType.Text;
                    command.CommandText = this.SqlObjectNames.GetTriggerCommandName(DbTriggerType.Delete, filter);
                    isBatch = false;
                    break;
                case DbCommandType.BulkTableType:
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandText = this.SqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.BulkTableType, filter);
                    isBatch = false;
                    break;
                case DbCommandType.UpdateUntrackedRows:
                    command.CommandType = CommandType.Text;
                    command.CommandText = this.SqlObjectNames.GetCommandName(DbCommandType.UpdateUntrackedRows, filter);
                    isBatch = false;
                    break;
                case DbCommandType.Reset:
                    command.CommandType = CommandType.Text;
                    command.CommandText = this.SqlObjectNames.GetCommandName(DbCommandType.Reset, filter);
                    isBatch = false;
                    break;
                case DbCommandType.PreDeleteRow:
                case DbCommandType.PreDeleteRows:
                case DbCommandType.PreInsertRow:
                case DbCommandType.PreInsertRows:
                case DbCommandType.PreUpdateRow:
                case DbCommandType.PreUpdateRows:
                    return (default, false);
                default:
                    throw new NotImplementedException($"This command type {nameType} is not implemented");
            }

            return (command, isBatch);
        }


        public override void AddCommandParameterValue(DbParameter parameter, object value, DbCommand command, DbCommandType commandType)
            => base.AddCommandParameterValue(parameter, value, command, commandType);

        public override DbCommand EnsureCommandParameters(DbCommand command, DbCommandType commandType, DbConnection connection, DbTransaction transaction, SyncFilter filter = null)
        {

            if ((commandType == DbCommandType.InsertRows || commandType == DbCommandType.UpdateRows || commandType == DbCommandType.DeleteRows) && this.UseBulkOperations)
            {
                if (command.Parameters != null && command.Parameters.Count > 0)
                    command.Parameters.Clear();
                
                DeriveParameters(command, connection, transaction);
            }
            if (commandType == DbCommandType.UpdateMetadata || commandType == DbCommandType.SelectMetadata || commandType == DbCommandType.SelectRow)
            {
                var p = GetParameter(command, "sync_row_count");
                if (p != null)
                    command.Parameters.Remove(p);
            }

            return command;
        }

        public override DbCommand EnsureCommandParametersValues(DbCommand command, DbCommandType commandType, DbConnection connection, DbTransaction transaction)
        {
            if (commandType == DbCommandType.DeleteMetadata)
            {
                // For some reason, we still have pkey as parameter of delete metadata stored proc ...
                // just set DBNull.Value
                foreach (var column in this.TableDescription.GetPrimaryKeysColumns())
                {
                    var unquotedColumn = ParserName.Parse(column).Normalized().Unquoted().ToString();
                    var parameter = GetParameter(command, unquotedColumn);
                    if (parameter != null)
                        parameter.Value = DBNull.Value;
                }
            }

            return command;
        }

        public override DbParameter GetParameter(DbCommand command, string parameterName)
            => base.GetParameter(command, parameterName);


        private void DeriveParameters(DbCommand command, DbConnection connection, DbTransaction transaction)
        {
            bool alreadyOpened = connection.State == ConnectionState.Open;
            try
            {

                if (!alreadyOpened)
                    connection.Open();

                command.Transaction = transaction;

                var textParser = ParserName.Parse(command.CommandText).Unquoted().Normalized().ToString();

                var source = connection.Database;

                textParser = $"{source}-{textParser}";

                if (derivingParameters.TryGetValue(textParser, out var parameters))
                {
                    foreach (var p in parameters)
                        command.Parameters.Add(p.Clone());
                }
                else
                {
                    // Using the SqlCommandBuilder.DeriveParameters() method is not working yet, 
                    // because default value is not well done handled on the Dotmim.Sync framework
                    // TODO: Fix SqlCommandBuilder.DeriveParameters
                    //SqlCommandBuilder.DeriveParameters((SqlCommand)command);

                    ((SqlConnection)connection).DeriveParameters((SqlCommand)command, false, (SqlTransaction)transaction);

                    var arrayParameters = new List<SqlParameter>();
                    foreach (var p in command.Parameters)
                        arrayParameters.Add(((SqlParameter)p).Clone());

                    derivingParameters.TryAdd(textParser, arrayParameters);
                }

                if (command.Parameters.Count > 0 && command.Parameters[0].ParameterName == "@RETURN_VALUE")
                    command.Parameters.RemoveAt(0);

            }
            catch (Exception ex)
            {
                Debug.WriteLine($"DeriveParameters failed : {ex}");
                throw;
            }
            finally
            {
                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();
            }


            foreach (var parameter in command.Parameters)
            {
                var sqlParameter = (SqlParameter)parameter;

                // try to get the source column (from the SchemaTable)
                var sqlParameterName = sqlParameter.ParameterName.Replace("@", "");
                var colDesc = TableDescription.Columns.FirstOrDefault(c => c.ColumnName.Equals(sqlParameterName, SyncGlobalization.DataSourceStringComparison));

                if (colDesc != null && !string.IsNullOrEmpty(colDesc.ColumnName))
                    sqlParameter.SourceColumn = colDesc.ColumnName;
            }
        }

       
    }
}
