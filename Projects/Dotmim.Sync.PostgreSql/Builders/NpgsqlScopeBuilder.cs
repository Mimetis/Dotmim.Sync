using Dotmim.Sync.Builders;
using Dotmim.Sync.DatabaseStringParsers;
using Dotmim.Sync.PostgreSql.Builders;
using System.Data;
using System.Data.Common;

namespace Dotmim.Sync.PostgreSql.Scope
{
    /// <summary>
    /// Represents a scope builder for Npgsql.
    /// </summary>
    public class NpgsqlScopeBuilder : DbScopeBuilder
    {

        /// <summary>
        /// Gets the scope info table names.
        /// </summary>
        protected DbTableNames ScopeInfoTableNames { get; }

        /// <summary>
        /// Gets the scope info client table names.
        /// </summary>
        protected DbTableNames ScopeInfoClientTableNames { get; }

        /// <inheritdoc cref="NpgsqlScopeBuilder"/>
        public NpgsqlScopeBuilder(string scopeInfoTableName)
            : base(scopeInfoTableName)
        {
            var tableParser = new TableParser(scopeInfoTableName, NpgsqlObjectNames.LeftQuote, NpgsqlObjectNames.RightQuote);

            var schema = NpgsqlManagementUtils.GetUnquotedSqlSchemaName(tableParser);

            var scopeInfoFullTableName = $"\"{schema}\".\"{tableParser.TableName}\"";
            var scopeInfoClientFullTableName = $"\"{schema}\".\"{tableParser.TableName}_client\"";

            tableParser = new TableParser(scopeInfoFullTableName, NpgsqlObjectNames.LeftQuote, NpgsqlObjectNames.RightQuote);

            this.ScopeInfoTableNames = new DbTableNames(NpgsqlObjectNames.LeftQuote, NpgsqlObjectNames.RightQuote,
                tableParser.TableName, tableParser.NormalizedFullName, tableParser.NormalizedShortName,
                tableParser.QuotedFullName, tableParser.QuotedShortName, tableParser.SchemaName);

            tableParser = new TableParser(scopeInfoClientFullTableName, NpgsqlObjectNames.LeftQuote, NpgsqlObjectNames.RightQuote);

            this.ScopeInfoClientTableNames = new DbTableNames(NpgsqlObjectNames.LeftQuote, NpgsqlObjectNames.RightQuote,
                tableParser.TableName, tableParser.NormalizedFullName, tableParser.NormalizedShortName,
                tableParser.QuotedFullName, tableParser.QuotedShortName, tableParser.SchemaName);
        }

        /// <inheritdoc/>
        public override DbCommand GetAllScopeInfoClientsCommand(DbConnection connection, DbTransaction transaction)
        {
            var commandText =
                $@"SELECT  sync_scope_id
                         , sync_scope_name
                         , sync_scope_hash
                         , sync_scope_parameters
                         , scope_last_sync_timestamp
                         , scope_last_server_sync_timestamp
                         , scope_last_sync_duration
                         , scope_last_sync
                         , sync_scope_errors
                         , sync_scope_properties
                    FROM  {this.ScopeInfoClientTableNames.QuotedFullName}";

            var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = commandText;

            return command;
        }

        /// <inheritdoc/>
        public override DbCommand GetAllScopeInfosCommand(DbConnection connection, DbTransaction transaction)
        {
            var commandText =
                $@"SELECT sync_scope_name, 
                          sync_scope_schema, 
                          sync_scope_setup, 
                          sync_scope_version,
                          sync_scope_last_clean_timestamp,
                          sync_scope_properties
                    FROM  {this.ScopeInfoTableNames.QuotedFullName}";

            var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = commandText;

            return command;
        }

        /// <inheritdoc/>
        public override DbCommand GetCreateScopeInfoClientTableCommand(DbConnection connection, DbTransaction transaction)
        {
            var commandText =
                $@"
                    CREATE TABLE {this.ScopeInfoClientTableNames.QuotedFullName}
                    (
                        sync_scope_id uuid NOT NULL,
                        sync_scope_name character varying(100) NOT NULL,
                        sync_scope_hash character varying(100) NOT NULL,
                        sync_scope_parameters character varying,
                        scope_last_sync_timestamp bigint,
                        scope_last_server_sync_timestamp bigint,
                        scope_last_sync_duration bigint,
                        scope_last_sync timestamp with time zone,
                        sync_scope_errors character varying,
                        sync_scope_properties character varying,
                        CONSTRAINT PKey_{this.ScopeInfoClientTableNames.NormalizedFullName} PRIMARY KEY (sync_scope_id, sync_scope_name, sync_scope_hash)
                    );";

            var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = commandText;
            return command;
        }

        /// <inheritdoc/>
        public override DbCommand GetCreateScopeInfoTableCommand(DbConnection connection, DbTransaction transaction)
        {
            var commandText =
                $@"CREATE TABLE {this.ScopeInfoTableNames.QuotedFullName} (
                    sync_scope_name varchar(100) NOT NULL,
                    sync_scope_schema varchar NULL,
                    sync_scope_setup varchar NULL,
                    sync_scope_version varchar(10) NULL,
                    sync_scope_last_clean_timestamp bigint NULL,
                    sync_scope_properties varchar NULL,
                    CONSTRAINT PKey_{this.ScopeInfoTableNames.NormalizedFullName} 
                    PRIMARY KEY (sync_scope_name)
                    )";

            var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = commandText;
            return command;
        }

        /// <inheritdoc/>
        public override DbCommand GetDeleteScopeInfoClientCommand(DbConnection connection, DbTransaction transaction)
        {

            var commandText =
                $@"DELETE FROM {this.ScopeInfoClientTableNames.QuotedFullName}
                   WHERE sync_scope_name = @sync_scope_name and sync_scope_id = @sync_scope_id and sync_scope_hash = @sync_scope_hash";

            var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = commandText;

            var p = command.CreateParameter();
            p.ParameterName = "@sync_scope_name";
            p.DbType = DbType.String;
            p.Size = 100;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_id";
            p.DbType = DbType.Guid;
            p.Size = -1;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_hash";
            p.DbType = DbType.String;
            p.Size = 100;
            command.Parameters.Add(p);

            return command;
        }

        /// <inheritdoc/>
        public override DbCommand GetDeleteScopeInfoCommand(DbConnection connection, DbTransaction transaction)
        {
            var commandText = $@"DELETE FROM {this.ScopeInfoTableNames.QuotedFullName} WHERE sync_scope_name = @sync_scope_name";

            var command = connection.CreateCommand();
            command.Transaction = transaction;

            command.CommandText = commandText;

            var p = command.CreateParameter();
            p.ParameterName = "@sync_scope_name";
            p.DbType = DbType.String;
            p.Size = 100;
            command.Parameters.Add(p);

            return command;
        }

        /// <inheritdoc/>
        public override DbCommand GetDropScopeInfoClientTableCommand(DbConnection connection, DbTransaction transaction)
        {

            var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = $"DROP Table {this.ScopeInfoClientTableNames.QuotedFullName}";
            return command;
        }

        /// <inheritdoc/>
        public override DbCommand GetDropScopeInfoTableCommand(DbConnection connection, DbTransaction transaction)
        {
            var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = $"DROP Table {this.ScopeInfoTableNames.QuotedFullName}";
            return command;
        }

        /// <inheritdoc/>
        public override DbCommand GetExistsScopeInfoClientCommand(DbConnection connection, DbTransaction transaction)
        {

            var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = $@"Select count(*) from {this.ScopeInfoClientTableNames.QuotedFullName} 
                                     where sync_scope_id = @sync_scope_id 
                                     and sync_scope_name = @sync_scope_name
                                     and sync_scope_hash = @sync_scope_hash;";

            var p = command.CreateParameter();
            p.ParameterName = "@sync_scope_id";
            p.DbType = DbType.Guid;
            p.Size = -1;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_name";
            p.DbType = DbType.String;
            p.Size = 100;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_hash";
            p.DbType = DbType.String;
            p.Size = 100;
            command.Parameters.Add(p);

            return command;
        }

        /// <inheritdoc/>
        public override DbCommand GetExistsScopeInfoClientTableCommand(DbConnection connection, DbTransaction transaction)
        {

            var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = @"select exists (select from information_schema.tables 
                                        where table_name=@tableName 
                                        and table_schema=@schemaName);";

            var parameter = command.CreateParameter();
            parameter.ParameterName = "@tableName";
            parameter.Value = this.ScopeInfoClientTableNames.Name;
            command.Parameters.Add(parameter);

            parameter = command.CreateParameter();
            parameter.ParameterName = "@schemaName";
            parameter.Value = this.ScopeInfoClientTableNames.SchemaName;
            command.Parameters.Add(parameter);
            return command;
        }

        /// <inheritdoc/>
        public override DbCommand GetExistsScopeInfoCommand(DbConnection connection, DbTransaction transaction)
        {
            var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = $@"Select count(*) from {this.ScopeInfoTableNames.QuotedFullName} where sync_scope_name = @sync_scope_name;";

            var p1 = command.CreateParameter();
            p1.ParameterName = "@sync_scope_name";
            p1.DbType = DbType.String;
            p1.Size = 100;
            command.Parameters.Add(p1);

            return command;
        }

        /// <inheritdoc/>
        public override DbCommand GetExistsScopeInfoTableCommand(DbConnection connection, DbTransaction transaction)
        {

            var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = @"select exists (select from information_schema.tables 
                                                        where table_name=@tableName 
                                                        and table_schema=@schemaName)";

            var parameter = command.CreateParameter();
            parameter.ParameterName = "@tableName";
            parameter.Value = this.ScopeInfoTableNames.Name;
            command.Parameters.Add(parameter);

            parameter = command.CreateParameter();
            parameter.ParameterName = "@schemaName";
            parameter.Value = this.ScopeInfoTableNames.SchemaName;
            command.Parameters.Add(parameter);

            return command;
        }

        /// <inheritdoc/>
        public override DbCommand GetInsertScopeInfoClientCommand(DbConnection connection, DbTransaction transaction)
            => this.GetUpdateScopeInfoClientCommand(connection, transaction);

        /// <inheritdoc/>
        public override DbCommand GetInsertScopeInfoCommand(DbConnection connection, DbTransaction transaction)
             => this.GetUpdateScopeInfoCommand(connection, transaction);

        /// <inheritdoc/>
        public override DbCommand GetLocalTimestampCommand(DbConnection connection, DbTransaction transaction)
        {
            var command = connection.CreateCommand();
            command.Transaction = transaction;

            command.CommandText = $"select {NpgsqlSyncAdapter.TimestampValue}";

            DbParameter p = command.CreateParameter();
            p.ParameterName = "@sync_new_timestamp";
            p.DbType = DbType.Int64;
            p.Direction = ParameterDirection.Output;
            command.Parameters.Add(p);

            return command;
        }

        /// <inheritdoc/>
        public override DbTableNames GetParsedScopeInfoTableNames() => this.ScopeInfoTableNames;

        /// <inheritdoc/>
        public override DbCommand GetScopeInfoClientCommand(DbConnection connection, DbTransaction transaction)
        {
            var commandText =
               $@"SELECT    sync_scope_id
                           , sync_scope_name
                           , sync_scope_hash
                           , sync_scope_parameters
                           , scope_last_sync_timestamp
                           , scope_last_server_sync_timestamp
                           , scope_last_sync_duration
                           , scope_last_sync
                           , sync_scope_errors
                           , sync_scope_properties
                    FROM  {this.ScopeInfoClientTableNames.QuotedFullName}
                    WHERE sync_scope_name = @sync_scope_name 
                    and sync_scope_id = @sync_scope_id::uuid
                    and sync_scope_hash = @sync_scope_hash";

            var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = commandText;

            var p = command.CreateParameter();
            p.ParameterName = "@sync_scope_name";
            p.DbType = DbType.String;
            p.Size = 100;
            command.Parameters.Add(p);

            var p0 = command.CreateParameter();
            p0.ParameterName = "@sync_scope_id";
            p0.DbType = DbType.Guid;
            p0.Size = -1;
            command.Parameters.Add(p0);

            var p1 = command.CreateParameter();
            p1.ParameterName = "@sync_scope_hash";
            p1.DbType = DbType.String;
            p1.Size = 100;
            command.Parameters.Add(p1);

            return command;
        }

        /// <inheritdoc/>
        public override DbCommand GetScopeInfoCommand(DbConnection connection, DbTransaction transaction)
        {
            var commandText =
                    $@"SELECT sync_scope_name, 
                          sync_scope_schema, 
                          sync_scope_setup, 
                          sync_scope_version,
                          sync_scope_last_clean_timestamp,
                          sync_scope_properties
                    FROM  {this.ScopeInfoTableNames.QuotedFullName}
                    WHERE sync_scope_name = @sync_scope_name";

            var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = commandText;

            var p = command.CreateParameter();
            p.ParameterName = "@sync_scope_name";
            p.DbType = DbType.String;
            p.Size = 100;
            command.Parameters.Add(p);

            return command;
        }

        /// <inheritdoc/>
        public override DbCommand GetUpdateScopeInfoClientCommand(DbConnection connection, DbTransaction transaction)
        {

            var commandText = $@"
                                with changes as (
                                        SELECT  @sync_scope_id AS sync_scope_id,  
	                                            @sync_scope_name AS sync_scope_name,  
	                                            @sync_scope_hash AS sync_scope_hash,  
	                                            @sync_scope_parameters AS sync_scope_parameters,  
                                                @scope_last_sync_timestamp AS scope_last_sync_timestamp,
                                                @scope_last_server_sync_timestamp AS scope_last_server_sync_timestamp,
                                                @scope_last_sync_duration AS scope_last_sync_duration,
                                                @scope_last_sync AS scope_last_sync,
                                                @sync_scope_errors AS sync_scope_errors,
                                                @sync_scope_properties AS sync_scope_properties
                                                 )
                                insert into  {this.ScopeInfoClientTableNames.QuotedFullName} (sync_scope_name, sync_scope_id, sync_scope_hash, sync_scope_parameters, scope_last_sync_timestamp,  scope_last_server_sync_timestamp, scope_last_sync, scope_last_sync_duration, sync_scope_errors, sync_scope_properties)
                                                  SELECT sync_scope_name, sync_scope_id, sync_scope_hash, sync_scope_parameters, scope_last_sync_timestamp,  scope_last_server_sync_timestamp, scope_last_sync, scope_last_sync_duration, sync_scope_errors, sync_scope_properties from changes
                                on conflict (sync_scope_id,sync_scope_name,sync_scope_hash)								
                                DO UPDATE SET 
                                                scope_last_sync_timestamp = EXCLUDED.scope_last_sync_timestamp,
                                                scope_last_server_sync_timestamp = EXCLUDED.scope_last_server_sync_timestamp,
                                                scope_last_sync = EXCLUDED.scope_last_sync,
                                                scope_last_sync_duration = EXCLUDED.scope_last_sync_duration,
                                                sync_scope_errors = EXCLUDED.sync_scope_errors,
                                                sync_scope_properties = EXCLUDED.sync_scope_properties
                                    returning	* ";

            var command = connection.CreateCommand();
            if (transaction != null)
                command.Transaction = transaction;

            command.CommandText = commandText;

            var p = command.CreateParameter();
            p.ParameterName = "@sync_scope_name";
            p.DbType = DbType.String;
            p.Size = 100;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_id";
            p.DbType = DbType.Guid;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_hash";
            p.DbType = DbType.String;
            p.Size = 100;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_parameters";
            p.DbType = DbType.String;
            p.Size = -1;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@scope_last_sync_timestamp";
            p.DbType = DbType.Int64;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@scope_last_server_sync_timestamp";
            p.DbType = DbType.Int64;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@scope_last_sync";
            p.DbType = DbType.DateTime;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@scope_last_sync_duration";
            p.DbType = DbType.Int64;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_errors";
            p.DbType = DbType.String;
            p.Size = -1;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_properties";
            p.DbType = DbType.String;
            p.Size = -1;
            command.Parameters.Add(p);

            return command;
        }

        /// <inheritdoc/>
        public override DbCommand GetUpdateScopeInfoCommand(DbConnection connection, DbTransaction transaction)
        {

            var commandText = $@"
                                with changes as (
                                                    SELECT @sync_scope_name AS sync_scope_name,
                                                    @sync_scope_schema AS sync_scope_schema,
                                                    @sync_scope_setup AS sync_scope_setup,
                                                    @sync_scope_version AS sync_scope_version,
                                                    @sync_scope_last_clean_timestamp AS sync_scope_last_clean_timestamp,
                                                    @sync_scope_properties as sync_scope_properties
                                                 )
                                insert into  {this.ScopeInfoTableNames.QuotedFullName} (sync_scope_name, sync_scope_schema, sync_scope_setup, sync_scope_version, sync_scope_last_clean_timestamp, sync_scope_properties)
                                                  SELECT sync_scope_name,sync_scope_schema,sync_scope_setup,sync_scope_version,sync_scope_last_clean_timestamp,sync_scope_properties from changes
                                on conflict (sync_scope_name)								
                                DO UPDATE SET 
                                                sync_scope_name = EXCLUDED.sync_scope_name, 
                                                sync_scope_schema = EXCLUDED.sync_scope_schema, 
                                                sync_scope_setup = EXCLUDED.sync_scope_setup, 
                                                sync_scope_version = EXCLUDED.sync_scope_version,
                                                sync_scope_last_clean_timestamp = EXCLUDED.sync_scope_last_clean_timestamp,
                                                sync_scope_properties = EXCLUDED.sync_scope_properties
                                    returning	* ";

            var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = commandText;

            var p = command.CreateParameter();
            p.ParameterName = "@sync_scope_name";
            p.DbType = DbType.String;
            p.Size = 100;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_schema";
            p.DbType = DbType.String;
            p.Size = -1;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_setup";
            p.DbType = DbType.String;
            p.Size = -1;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_version";
            p.DbType = DbType.String;
            p.Size = 10;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_last_clean_timestamp";
            p.DbType = DbType.Int64;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_properties";
            p.DbType = DbType.String;
            p.Size = -1;
            command.Parameters.Add(p);

            return command;
        }
    }
}