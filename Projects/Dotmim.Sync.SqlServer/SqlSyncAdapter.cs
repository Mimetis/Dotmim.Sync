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
        public SqlObjectNames SqlObjectNames { get; set; }
        public SqlDbMetadata SqlMetadata { get; set; }

        private readonly ParserName tableName;
        private readonly ParserName trackingName;

        public SqlSyncAdapter(SyncTable tableDescription, ParserName tableName, ParserName trackingName, SyncSetup setup, string scopeName, bool useBulkOperations) : base(tableDescription, setup, scopeName, useBulkOperations)
        {
            this.SqlObjectNames = new SqlObjectNames(tableDescription, tableName, trackingName, setup, scopeName);
            this.SqlMetadata = new SqlDbMetadata();
            this.tableName = tableName;
            this.trackingName = trackingName;
        }

        private SqlMetaData GetSqlMetadaFromType(SyncColumn column)
        {
            long maxLength = column.MaxLength;
            var dataType = column.GetDataType();
            var dbType = column.GetDbType();

            var sqlDbType = this.TableDescription.OriginalProvider == SqlSyncProvider.ProviderType ?
                this.SqlMetadata.GetSqlDbType(column) : this.SqlMetadata.GetOwnerDbTypeFromDbType(column);

            // Since we validate length before, it's not mandatory here.
            // let's say.. just in case..
            if (sqlDbType == SqlDbType.VarChar || sqlDbType == SqlDbType.NVarChar)
            {
                // set value for (MAX) 
                maxLength = maxLength <= 0 ? SqlMetaData.Max : maxLength;

                // If max length is specified (not (MAX) )
                if (maxLength > 0)
                    maxLength = sqlDbType == SqlDbType.NVarChar ? Math.Min(maxLength, 4000) : Math.Min(maxLength, 8000);

                return new SqlMetaData(column.ColumnName, sqlDbType, maxLength);
            }


            if (dataType == typeof(char))
                return new SqlMetaData(column.ColumnName, sqlDbType, 1);

            if (sqlDbType == SqlDbType.Char || sqlDbType == SqlDbType.NChar)
            {
                maxLength = maxLength <= 0 ? (sqlDbType == SqlDbType.NChar ? 4000 : 8000) : maxLength;
                return new SqlMetaData(column.ColumnName, sqlDbType, maxLength);
            }

            if (sqlDbType == SqlDbType.Binary)
            {
                maxLength = maxLength <= 0 ? 8000 : maxLength;
                return new SqlMetaData(column.ColumnName, sqlDbType, maxLength);
            }

            if (sqlDbType == SqlDbType.VarBinary)
            {
                // set value for (MAX) 
                maxLength = maxLength <= 0 ? SqlMetaData.Max : maxLength;

                return new SqlMetaData(column.ColumnName, sqlDbType, maxLength);
            }

            if (sqlDbType == SqlDbType.Decimal)
            {
                var (p, s) = this.SqlMetadata.GetPrecisionAndScale(column);
                if (p > 0 && p > s)
                {
                    return new SqlMetaData(column.ColumnName, sqlDbType, p, s);
                }
                else
                {
                    if (p == 0)
                        p = 18;
                    if (s == 0)
                        s = Math.Min((byte)(p - 1), (byte)6);
                    return new SqlMetaData(column.ColumnName, sqlDbType, p, s);
                }

            }

            return new SqlMetaData(column.ColumnName, sqlDbType);

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
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandText = this.SqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.SelectRow, filter);
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
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandText = this.SqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.DeleteMetadata, filter);
                    isBatch = false;
                    break;
                case DbCommandType.UpdateMetadata:
                    command.CommandType = CommandType.Text;
                    command.CommandText = this.SqlObjectNames.GetCommandName(DbCommandType.UpdateMetadata, filter);
                    isBatch = false;
                    break;
                case DbCommandType.SelectMetadata:
                    command.CommandType = CommandType.Text;
                    command.CommandText = this.SqlObjectNames.GetCommandName(DbCommandType.SelectMetadata, filter);
                    isBatch = false;
                    break;
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
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandText = this.SqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.Reset, filter);
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

                if (derivingParameters.ContainsKey(textParser))
                {
                    foreach (var p in derivingParameters[textParser])
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

        ///// <summary>
        ///// Set a stored procedure parameters or text parameters
        ///// </summary>
        //public override async Task AddCommandParametersAsync(DbCommandType commandType, DbCommand command, DbConnection connection, DbTransaction transaction = null, SyncFilter filter = null)
        //{
        //    if (command == null)
        //        return;

        //    if (command.Parameters != null && command.Parameters.Count > 0)
        //        return;

        //    // special case for constraint
        //    if (commandType == DbCommandType.DisableConstraints || commandType == DbCommandType.EnableConstraints)
        //        return;


        //    // special case for UpdateMetadata
        //    if (commandType == DbCommandType.UpdateMetadata)
        //    {
        //        this.SetUpdateRowParameters(command);
        //        return;
        //    }
        //    if (commandType == DbCommandType.SelectMetadata)
        //    {
        //        this.SetSelectRowParameters(command);
        //        return;
        //    }
        //    if (commandType == DbCommandType.SelectChanges || commandType == DbCommandType.SelectChangesWithFilters ||
        //        commandType == DbCommandType.SelectInitializedChanges || commandType == DbCommandType.SelectInitializedChangesWithFilters)
        //    {
        //        this.SetSelectChangesParameters(command, commandType, filter);
        //        return;
        //    }

        //    // if we don't have stored procedure, return, because we don't want to derive parameters
        //    if (command.CommandType != CommandType.StoredProcedure)
        //        return;

        //    bool alreadyOpened = connection.State == ConnectionState.Open;

        //    try
        //    {
        //        if (!alreadyOpened)
        //            await connection.OpenAsync().ConfigureAwait(false);

        //        command.Transaction = transaction;

        //        var textParser = ParserName.Parse(command.CommandText).Unquoted().Normalized().ToString();

        //        var source = connection.Database;

        //        textParser = $"{source}-{textParser}";

        //        if (derivingParameters.ContainsKey(textParser))
        //        {
        //            foreach (var p in derivingParameters[textParser])
        //                command.Parameters.Add(p.Clone());
        //        }
        //        else
        //        {
        //            // Using the SqlCommandBuilder.DeriveParameters() method is not working yet, 
        //            // because default value is not well done handled on the Dotmim.Sync framework
        //            // TODO: Fix SqlCommandBuilder.DeriveParameters
        //            //SqlCommandBuilder.DeriveParameters((SqlCommand)command);

        //            await ((SqlConnection)connection).DeriveParametersAsync((SqlCommand)command, false, (SqlTransaction)transaction).ConfigureAwait(false);

        //            var arrayParameters = new List<SqlParameter>();
        //            foreach (var p in command.Parameters)
        //                arrayParameters.Add(((SqlParameter)p).Clone());

        //            derivingParameters.TryAdd(textParser, arrayParameters);
        //        }

        //        if (command.Parameters.Count > 0 && command.Parameters[0].ParameterName == "@RETURN_VALUE")
        //            command.Parameters.RemoveAt(0);

        //    }
        //    catch (Exception ex)
        //    {
        //        Debug.WriteLine($"DeriveParameters failed : {ex}");
        //        throw;
        //    }
        //    finally
        //    {
        //        if (!alreadyOpened && connection.State != ConnectionState.Closed)
        //            connection.Close();
        //    }


        //    foreach (var parameter in command.Parameters)
        //    {
        //        var sqlParameter = (SqlParameter)parameter;

        //        // try to get the source column (from the SchemaTable)
        //        var sqlParameterName = sqlParameter.ParameterName.Replace("@", "");
        //        var colDesc = TableDescription.Columns.FirstOrDefault(c => c.ColumnName.Equals(sqlParameterName, SyncGlobalization.DataSourceStringComparison));

        //        if (colDesc != null && !string.IsNullOrEmpty(colDesc.ColumnName))
        //            sqlParameter.SourceColumn = colDesc.ColumnName;
        //    }
        //}

        //private void SetSelectChangesParameters(DbCommand command, DbCommandType commandType, SyncFilter filter = null)
        //{
        //    var originalProvider = SqlSyncProvider.ProviderType;

        //    var p = command.CreateParameter();
        //    p.ParameterName = "sync_min_timestamp";
        //    p.DbType = DbType.Int64;
        //    command.Parameters.Add(p);

        //    if (commandType == DbCommandType.SelectChanges || commandType == DbCommandType.SelectChangesWithFilters)
        //    {
        //        p = command.CreateParameter();
        //        p.ParameterName = "sync_scope_id";
        //        p.DbType = DbType.Guid;
        //        command.Parameters.Add(p);
        //    }

        //    if (filter == null)
        //        return;

        //    var parameters = filter.Parameters;

        //    if (parameters.Count == 0)
        //        return;

        //    foreach (var param in parameters)
        //    {
        //        if (param.DbType.HasValue)
        //        {
        //            // Get column name and type
        //            var columnName = ParserName.Parse(param.Name).Unquoted().Normalized().ToString();
        //            var syncColumn = new SyncColumn(columnName)
        //            {
        //                DbType = (int)param.DbType.Value,
        //                MaxLength = param.MaxLength,
        //            };
        //            var sqlDbType = this.SqlMetadata.GetOwnerDbTypeFromDbType(syncColumn);

        //            var customParameterFilter = new SqlParameter($"@{columnName}", sqlDbType);
        //            customParameterFilter.Size = param.MaxLength;
        //            customParameterFilter.IsNullable = param.AllowNull;
        //            customParameterFilter.Value = param.DefaultValue;

        //            command.Parameters.Add(customParameterFilter);
        //        }
        //        else
        //        {
        //            var tableFilter = this.TableDescription.Schema.Tables[param.TableName, param.SchemaName];
        //            if (tableFilter == null)
        //                throw new FilterParamTableNotExistsException(param.TableName);

        //            var columnFilter = tableFilter.Columns[param.Name];
        //            if (columnFilter == null)
        //                throw new FilterParamColumnNotExistsException(param.Name, param.TableName);

        //            // Get column name and type
        //            var columnName = ParserName.Parse(columnFilter).Normalized().Unquoted().ToString();

        //            var sqlDbType = tableFilter.OriginalProvider == originalProvider ?
        //                this.SqlMetadata.GetSqlDbType(columnFilter) : this.SqlMetadata.GetOwnerDbTypeFromDbType(columnFilter);

        //            // Add it as parameter
        //            var sqlParamFilter = new SqlParameter($"@{columnName}", sqlDbType);
        //            sqlParamFilter.Size = columnFilter.MaxLength;
        //            sqlParamFilter.IsNullable = param.AllowNull;
        //            sqlParamFilter.Value = param.DefaultValue;
        //            command.Parameters.Add(sqlParamFilter);
        //        }
        //    }
        //}

        //private void SetUpdateRowParameters(DbCommand command)
        //{
        //    DbParameter p;

        //    foreach (var column in this.TableDescription.GetPrimaryKeysColumns().Where(c => !c.IsReadOnly))
        //    {
        //        var unquotedColumn = ParserName.Parse(column).Normalized().Unquoted().ToString();
        //        p = command.CreateParameter();
        //        p.ParameterName = $"@{unquotedColumn}";
        //        p.DbType = column.GetDbType();
        //        p.SourceColumn = column.ColumnName;
        //        p.Size = column.MaxLength;
        //        command.Parameters.Add(p);
        //    }

        //    p = command.CreateParameter();
        //    p.ParameterName = "@sync_scope_id";
        //    p.DbType = DbType.Guid;
        //    p.Size = 32;
        //    command.Parameters.Add(p);

        //    p = command.CreateParameter();
        //    p.ParameterName = "@sync_row_is_tombstone";
        //    p.DbType = DbType.Boolean;
        //    p.Size = 2;
        //    command.Parameters.Add(p);

        //}

        //private void SetSelectRowParameters(DbCommand command)
        //{
        //    DbParameter p;

        //    foreach (var column in this.TableDescription.GetPrimaryKeysColumns().Where(c => !c.IsReadOnly))
        //    {
        //        var unquotedColumn = ParserName.Parse(column).Normalized().Unquoted().ToString();
        //        p = command.CreateParameter();
        //        p.ParameterName = $"@{unquotedColumn}";
        //        p.DbType = column.GetDbType();
        //        p.SourceColumn = column.ColumnName;
        //        p.Size = column.MaxLength;
        //        command.Parameters.Add(p);
        //    }

        //}
    }
}
