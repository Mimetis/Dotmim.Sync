using Dotmim.Sync.Builders;
using Dotmim.Sync.DatabaseStringParsers;
using Dotmim.Sync.SqlServer.Manager;
using Microsoft.Data.SqlClient;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;

namespace Dotmim.Sync.SqlServer.Builders
{
    /// <inheritdoc />
    public partial class SqlSyncAdapter : DbSyncAdapter
    {

        // Derive Parameters cache
        // Be careful, we can have collision between databases
        // this static class could be shared accross databases with same command name
        // but different table schema
        // So the string should contains the connection string as well
        private static ConcurrentDictionary<string, List<SqlParameter>> derivingParameters = new();
        private static DateTime sqlDateMin = new(1753, 1, 1);
        private static DateTime sqlSmallDateMin = new(1900, 1, 1);

        /// <summary>
        /// Gets the SqlObjectNames.
        /// </summary>
        public SqlObjectNames SqlObjectNames { get; }

        /// <summary>
        /// Gets the SqlDbMetadata.
        /// </summary>
        public SqlDbMetadata SqlMetadata { get; }

        /// <inheritdoc />
        public SqlSyncAdapter(SyncTable tableDescription, ScopeInfo scopeInfo, bool useBulkOperations)
            : base(tableDescription, scopeInfo, useBulkOperations)
        {
            this.SqlObjectNames = new SqlObjectNames(tableDescription, scopeInfo);
            this.SqlMetadata = new SqlDbMetadata();
        }

        /// <inheritdoc/>
        public override DbColumnNames GetParsedColumnNames(string name)
        {
            var columnParser = new ObjectParser(name, SqlObjectNames.LeftQuote, SqlObjectNames.RightQuote);
            return new DbColumnNames(columnParser.QuotedShortName, columnParser.NormalizedShortName);
        }

        /// <summary>
        /// Get the table builder. Table builder builds table, stored procedures and triggers.
        /// </summary>
        public override DbTableBuilder GetTableBuilder() => new SqlTableBuilder(this.TableDescription, this.ScopeInfo);

        /// <inheritdoc/>
        public override (DbCommand, bool) GetCommand(SyncContext context, DbCommandType commandType, SyncFilter filter)
        {
            using var command = new SqlCommand();
            bool isBatch;
            switch (commandType)
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
                    command.CommandType = CommandType.Text;
                    command.CommandText = this.SqlObjectNames.GetCommandName(DbCommandType.DeleteMetadata);
                    isBatch = false;
                    break;
                case DbCommandType.InsertTrigger:
                    command.CommandType = CommandType.Text;
                    command.CommandText = this.SqlObjectNames.GetTriggerCommandName(DbTriggerType.Insert);
                    isBatch = false;
                    break;
                case DbCommandType.UpdateTrigger:
                    command.CommandType = CommandType.Text;
                    command.CommandText = this.SqlObjectNames.GetTriggerCommandName(DbTriggerType.Update);
                    isBatch = false;
                    break;
                case DbCommandType.DeleteTrigger:
                    command.CommandType = CommandType.Text;
                    command.CommandText = this.SqlObjectNames.GetTriggerCommandName(DbTriggerType.Delete);
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
                case DbCommandType.UpdateMetadata:
                case DbCommandType.SelectMetadata:
                case DbCommandType.PreDeleteRow:
                case DbCommandType.PreDeleteRows:
                case DbCommandType.PreInsertRow:
                case DbCommandType.PreInsertRows:
                case DbCommandType.PreUpdateRow:
                case DbCommandType.PreUpdateRows:
                    return (default, false);
                default:
                    throw new NotImplementedException($"This command type {commandType} is not implemented");
            }

            return (command, isBatch);
        }

        /// <inheritdoc/>
        public override void AddCommandParameterValue(SyncContext context, DbParameter parameter, object value, DbCommand command, DbCommandType commandType)
            => base.AddCommandParameterValue(context, parameter, value, command, commandType);

        /// <inheritdoc/>
        public override DbCommand EnsureCommandParameters(SyncContext context, DbCommand command, DbCommandType commandType, DbConnection connection, DbTransaction transaction, SyncFilter filter = null)
        {

            if ((commandType == DbCommandType.InsertRows || commandType == DbCommandType.UpdateRows || commandType == DbCommandType.DeleteRows) && this.UseBulkOperations)
            {
                if (command.Parameters != null && command.Parameters.Count > 0)
                    command.Parameters.Clear();

                this.DeriveParameters(command, connection, transaction);
            }

            if (commandType == DbCommandType.UpdateMetadata || commandType == DbCommandType.DeleteMetadata || commandType == DbCommandType.SelectMetadata || commandType == DbCommandType.SelectRow)
            {
                var p = this.GetParameter(context, command, "sync_row_count");
                if (p != null)
                    command.Parameters.Remove(p);
            }

            return command;
        }

        /// <inheritdoc/>
        public override DbCommand EnsureCommandParametersValues(SyncContext context, DbCommand command, DbCommandType commandType, DbConnection connection, DbTransaction transaction)
        {
            if (commandType == DbCommandType.DeleteMetadata)
            {
                // For some reason, we still have pkey as parameter of delete metadata stored proc ...
                // just set DBNull.Value
                foreach (var column in this.TableDescription.GetPrimaryKeysColumns())
                {
                    var objectParser = new ObjectParser(column.ColumnName, SqlObjectNames.LeftQuote, SqlObjectNames.RightQuote);
                    var parameter = this.GetParameter(context, command, objectParser.NormalizedShortName);
                    if (parameter != null)
                        parameter.Value = DBNull.Value;
                }
            }

            return command;
        }

        /// <inheritdoc/>
        public override DbParameter GetParameter(SyncContext context, DbCommand command, string parameterName)
            => base.GetParameter(context, command, parameterName);

        private void DeriveParameters(DbCommand command, DbConnection connection, DbTransaction transaction)
        {
            bool alreadyOpened = connection.State == ConnectionState.Open;
            try
            {

                if (!alreadyOpened)
                    connection.Open();

                command.Transaction = transaction;
                var textParser = new ObjectParser(command.CommandText, SqlObjectNames.LeftQuote, SqlObjectNames.RightQuote);

                var source = connection.Database;

                var text = $"{source}-{textParser.NormalizedShortName}";

                if (derivingParameters.TryGetValue(text, out var parameters))
                {
                    foreach (var p in parameters)
                        command.Parameters.Add(p.Clone());
                }
                else
                {
                    // Using the SqlCommandBuilder.DeriveParameters() method is not working yet,
                    // because default value is not well done handled on the Dotmim.Sync framework
                    // TODO: Fix SqlCommandBuilder.DeriveParameters
                    // SqlCommandBuilder.DeriveParameters((SqlCommand)command);
                    ((SqlConnection)connection).DeriveParameters((SqlCommand)command, false, (SqlTransaction)transaction);

                    var arrayParameters = new List<SqlParameter>();
                    foreach (var p in command.Parameters)
                        arrayParameters.Add(((SqlParameter)p).Clone());

                    derivingParameters.TryAdd(text, arrayParameters);
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
                var sqlParameterName = sqlParameter.ParameterName.Replace("@", string.Empty, SyncGlobalization.DataSourceStringComparison);
                var colDesc = this.TableDescription.Columns.FirstOrDefault(c => c.ColumnName.Equals(sqlParameterName, SyncGlobalization.DataSourceStringComparison));

                if (colDesc != null && !string.IsNullOrEmpty(colDesc.ColumnName))
                    sqlParameter.SourceColumn = colDesc.ColumnName;
            }
        }
    }
}