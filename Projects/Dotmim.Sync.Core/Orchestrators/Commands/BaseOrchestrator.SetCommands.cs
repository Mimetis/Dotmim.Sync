
using Dotmim.Sync.Builders;
using System;
using System.Collections.Concurrent;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    public abstract partial class BaseOrchestrator
    {

        /// <summary>
        /// Set the GetChanges stored procedure parameters, with Filter or without filter.
        /// </summary>
        internal DbCommand InternalSetSelectChangesParameters(DbCommand command, DbSyncAdapter syncAdapter)
        {
            var p = command.CreateParameter();
            p.ParameterName = $"{syncAdapter.ParameterPrefix}sync_min_timestamp";
            p.DbType = DbType.Int64;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = $"{syncAdapter.ParameterPrefix}sync_scope_id";
            p.DbType = DbType.Guid;
            command.Parameters.Add(p);

            SyncFilter filter = null;

            if (this.Provider != null && this.Provider.CanBeServerProvider) // Sqlite can't be server
                filter = syncAdapter.TableDescription.GetFilter();

            if (filter == null)
                return command;

            var parameters = filter.Parameters;

            if (parameters.Count == 0)
                return command;

            foreach (var param in parameters)
            {
                string parameterName;
                DbType parameterDbType;
                int size;
                string defaultValue;

                if (param.DbType.HasValue)
                {
                    // Get column name and type
                    parameterName = ParserName.Parse(param.Name, syncAdapter.QuotePrefix, syncAdapter.QuoteSuffix).Unquoted().Normalized().ToString();
                    parameterDbType = param.DbType.Value;
                    size = param.MaxLength;
                    defaultValue = param.DefaultValue;
                }
                else
                {
                    var tableFilter = syncAdapter.TableDescription.Schema.Tables[param.TableName, param.SchemaName];
                    if (tableFilter == null)
                        throw new FilterParamTableNotExistsException(param.TableName);

                    var columnFilter = tableFilter.Columns[param.Name];
                    if (columnFilter == null)
                        throw new FilterParamColumnNotExistsException(param.Name, param.TableName);

                    // Get column name and type
                    parameterName = ParserName.Parse(columnFilter, syncAdapter.QuotePrefix, syncAdapter.QuoteSuffix).Unquoted().Normalized().ToString();
                    parameterDbType = columnFilter.GetDbType();
                    size = columnFilter.GetDataType() == typeof(string) && columnFilter.MaxLength > 0 ? columnFilter.MaxLength : -1;
                    defaultValue = param.DefaultValue;

                }

                var customParameterFilter = command.CreateParameter();
                customParameterFilter.ParameterName = $"{syncAdapter.ParameterPrefix}{parameterName}";
                customParameterFilter.DbType = parameterDbType;
                customParameterFilter.Size = size;
                if (defaultValue != null)
                    customParameterFilter.Value = defaultValue;

                command.Parameters.Add(customParameterFilter);
            }
            return command;
        }

        /// <summary>
        /// Set the Initialize stored procedure parameters, with Filter or without filter.
        /// </summary>
        internal DbCommand InternalSetSelectInitializeChangesParameters(DbCommand command, DbSyncAdapter syncAdapter)
        {
            var p = command.CreateParameter();
            p.ParameterName = $"{syncAdapter.ParameterPrefix}sync_min_timestamp";
            p.DbType = DbType.Int64;
            command.Parameters.Add(p);

            SyncFilter filter = null;

            if (this.Provider != null && this.Provider.CanBeServerProvider) // Sqlite can't be server
                filter = syncAdapter.TableDescription.GetFilter();

            if (filter == null)
                return command;

            var parameters = filter.Parameters;

            if (parameters.Count == 0)
                return command;

            foreach (var param in parameters)
            {
                string parameterName;
                DbType parameterDbType;
                int size;
                string defaultValue;

                if (param.DbType.HasValue)
                {
                    // Get column name and type
                    parameterName = ParserName.Parse(param.Name, syncAdapter.QuotePrefix, syncAdapter.QuoteSuffix).Unquoted().Normalized().ToString();
                    parameterDbType = param.DbType.Value;
                    size = param.MaxLength;
                    defaultValue = param.DefaultValue;
                }
                else
                {
                    var tableFilter = syncAdapter.TableDescription.Schema.Tables[param.TableName, param.SchemaName];
                    if (tableFilter == null)
                        throw new FilterParamTableNotExistsException(param.TableName);

                    var columnFilter = tableFilter.Columns[param.Name];
                    if (columnFilter == null)
                        throw new FilterParamColumnNotExistsException(param.Name, param.TableName);

                    // Get column name and type
                    parameterName = ParserName.Parse(columnFilter, syncAdapter.QuotePrefix, syncAdapter.QuoteSuffix).Unquoted().Normalized().ToString();
                    parameterDbType = columnFilter.GetDbType();
                    size = columnFilter.GetDataType() == typeof(string) && columnFilter.MaxLength > 0 ? columnFilter.MaxLength : -1;
                    defaultValue = param.DefaultValue;

                }

                var customParameterFilter = command.CreateParameter();
                customParameterFilter.ParameterName = $"{syncAdapter.ParameterPrefix}{parameterName}";
                customParameterFilter.DbType = parameterDbType;
                customParameterFilter.Size = size;
                if (defaultValue != null)
                    customParameterFilter.Value = defaultValue;
                command.Parameters.Add(customParameterFilter);
            }
            return command;
        }

        /// <summary>
        /// Set the Upserts stored procedure parameters
        /// </summary>
        internal DbCommand InternalSetUpsertsParameters(DbCommand command, DbSyncAdapter syncAdapter)
        {
            DbParameter p;

            foreach (var column in syncAdapter.TableDescription.Columns.Where(c => !c.IsReadOnly))
            {
                var columnName = ParserName.Parse(column, syncAdapter.QuotePrefix, syncAdapter.QuoteSuffix).Unquoted().Normalized().ToString();

                p = command.CreateParameter();
                p.ParameterName = $"{syncAdapter.ParameterPrefix}{columnName}";
                p.DbType = column.GetDbType();
                p.SourceColumn = column.ColumnName;
                command.Parameters.Add(p);
            }

            p = command.CreateParameter();
            p.ParameterName = $"{syncAdapter.ParameterPrefix}sync_scope_id";
            p.DbType = DbType.Guid;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = $"{syncAdapter.ParameterPrefix}sync_force_write";
            p.DbType = DbType.Int64;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = $"{syncAdapter.ParameterPrefix}sync_min_timestamp";
            p.DbType = DbType.Int64;
            command.Parameters.Add(p);

            if (syncAdapter.SupportsOutputParameters)
            {
                p = command.CreateParameter();
                p.ParameterName = $"{syncAdapter.ParameterPrefix}sync_row_count";
                p.DbType = DbType.Int32;
                p.Direction = ParameterDirection.Output;
                command.Parameters.Add(p);
            }

            return command;
        }

        /// <summary>
        /// Set the SelectRow stored procedure parameters
        /// </summary>
        internal DbCommand InternalSetSelectRowParameters(DbCommand command, DbSyncAdapter syncAdapter)
        {
            DbParameter p;

            foreach (var column in syncAdapter.TableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(column, syncAdapter.QuotePrefix, syncAdapter.QuoteSuffix).Unquoted().Normalized().ToString();


                p = command.CreateParameter();
                p.ParameterName = $"{syncAdapter.ParameterPrefix}{columnName}";
                p.DbType = column.GetDbType();
                p.SourceColumn = column.ColumnName;
                p.Size = column.GetDataType() == typeof(string) && column.MaxLength > 0 ? column.MaxLength : -1;
                command.Parameters.Add(p);
            }

            p = command.CreateParameter();
            p.ParameterName = $"{syncAdapter.ParameterPrefix}sync_scope_id";
            p.DbType = DbType.Guid;
            command.Parameters.Add(p);

            if (syncAdapter.SupportsOutputParameters)
            {
                p = command.CreateParameter();
                p.ParameterName = $"{syncAdapter.ParameterPrefix}sync_row_count";
                p.DbType = DbType.Int32;
                p.Direction = ParameterDirection.Output;
                command.Parameters.Add(p);
            }
            return command;
        }

        /// <summary>
        /// Set the DeleteRow stored procedure parameters
        /// </summary>
        internal DbCommand InternalSetDeleteRowParameters(DbCommand command, DbSyncAdapter syncAdapter)
        {
            DbParameter p;

            foreach (var column in syncAdapter.TableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(column, syncAdapter.QuotePrefix, syncAdapter.QuoteSuffix).Unquoted().Normalized().ToString();

                p = command.CreateParameter();
                p.ParameterName = $"{syncAdapter.ParameterPrefix}{columnName}";
                p.DbType = column.GetDbType();
                p.SourceColumn = column.ColumnName;
                p.Size = column.GetDataType() == typeof(string) && column.MaxLength > 0 ? column.MaxLength : -1;
                command.Parameters.Add(p);
            }

            p = command.CreateParameter();
            p.ParameterName = $"{syncAdapter.ParameterPrefix}sync_scope_id";
            p.DbType = DbType.Guid;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = $"{syncAdapter.ParameterPrefix}sync_force_write";
            p.DbType = DbType.Int64;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = $"{syncAdapter.ParameterPrefix}sync_min_timestamp";
            p.DbType = DbType.Int64;
            command.Parameters.Add(p);

            if (syncAdapter.SupportsOutputParameters)
            {
                p = command.CreateParameter();
                p.ParameterName = $"{syncAdapter.ParameterPrefix}sync_row_count";
                p.DbType = DbType.Int32;
                p.Direction = ParameterDirection.Output;
                command.Parameters.Add(p);
            }
            return command;
        }

        /// <summary>
        /// Set the DeleteMetadata stored procedure parameters
        /// </summary>
        internal DbCommand InternalSetDeleteMetadataParameters(DbCommand command, DbSyncAdapter syncAdapter)
        {
            DbParameter p;

            foreach (var column in syncAdapter.TableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(column, syncAdapter.QuotePrefix, syncAdapter.QuoteSuffix).Unquoted().Normalized().ToString();

                p = command.CreateParameter();
                p.ParameterName = $"{syncAdapter.ParameterPrefix}{columnName}";
                p.DbType = column.GetDbType();
                p.SourceColumn = column.ColumnName;
                p.Size = column.GetDataType() == typeof(string) && column.MaxLength > 0 ? column.MaxLength : -1;
                command.Parameters.Add(p);
            }

            p = command.CreateParameter();
            p.ParameterName = $"{syncAdapter.ParameterPrefix}sync_row_timestamp";
            p.DbType = DbType.Int64;
            command.Parameters.Add(p);

            if (syncAdapter.SupportsOutputParameters)
            {
                p = command.CreateParameter();
                p.ParameterName = $"{syncAdapter.ParameterPrefix}sync_row_count";
                p.DbType = DbType.Int32;
                p.Direction = ParameterDirection.Output;
                command.Parameters.Add(p);
            }

            return command;
        }


        /// <summary>
        /// Set the UpdateMetadata stored procedure parameters
        /// </summary>
        internal DbCommand InternalSetUpdateMetadataParameters(DbCommand command, DbSyncAdapter syncAdapter)
        {
            DbParameter p;

            foreach (var column in syncAdapter.TableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(column, syncAdapter.QuotePrefix, syncAdapter.QuoteSuffix).Unquoted().Normalized().ToString();

                p = command.CreateParameter();
                p.ParameterName = $"{syncAdapter.ParameterPrefix}{columnName}";
                p.DbType = column.GetDbType();
                p.SourceColumn = column.ColumnName;
                p.Size = column.GetDataType() == typeof(string) && column.MaxLength > 0 ? column.MaxLength : -1;
                command.Parameters.Add(p);
            }

            p = command.CreateParameter();
            p.ParameterName = $"{syncAdapter.ParameterPrefix}sync_scope_id";
            p.DbType = DbType.Guid;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = $"{syncAdapter.ParameterPrefix}sync_min_timestamp";
            p.DbType = DbType.Int64;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = $"{syncAdapter.ParameterPrefix}sync_row_is_tombstone";
            p.DbType = DbType.Int16;
            command.Parameters.Add(p);

            if (syncAdapter.SupportsOutputParameters)
            {
                p = command.CreateParameter();
                p.ParameterName = $"{syncAdapter.ParameterPrefix}sync_row_count";
                p.DbType = DbType.Int32;
                p.Direction = ParameterDirection.Output;
                command.Parameters.Add(p);
            }

            return command;
        }

        /// <summary>
        /// Set the SelectMetadata stored procedure parameters
        /// </summary>
        internal DbCommand InternalSetSelectMetadataParameters(DbCommand command, DbSyncAdapter syncAdapter)
        {
            DbParameter p;

            foreach (var column in syncAdapter.TableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(column, syncAdapter.QuotePrefix, syncAdapter.QuoteSuffix).Unquoted().Normalized().ToString();

                p = command.CreateParameter();
                p.ParameterName = $"{syncAdapter.ParameterPrefix}{columnName}";
                p.DbType = column.GetDbType();
                p.SourceColumn = column.ColumnName;
                p.Size = column.GetDataType() == typeof(string) && column.MaxLength > 0 ? column.MaxLength : -1;
                command.Parameters.Add(p);
            }


            if (syncAdapter.SupportsOutputParameters)
            {
                p = command.CreateParameter();
                p.ParameterName = $"{syncAdapter.ParameterPrefix}sync_row_count";
                p.DbType = DbType.Int32;
                p.Direction = ParameterDirection.Output;
                command.Parameters.Add(p);
            }

            return command;
        }

        /// <summary>
        /// Set the SelectMetadata stored procedure parameters
        /// </summary>
        internal DbCommand InternalSetResetParameters(DbCommand command, DbSyncAdapter syncAdapter)
        {
            DbParameter p;

            if (syncAdapter.SupportsOutputParameters)
            {
                p = command.CreateParameter();
                p.ParameterName = $"{syncAdapter.ParameterPrefix}sync_row_count";
                p.DbType = DbType.Int32;
                p.Direction = ParameterDirection.Output;
                command.Parameters.Add(p);
            }

            return command;
        }
    }
}