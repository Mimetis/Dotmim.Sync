using Dotmim.Sync.Builders;
using Dotmim.Sync.Postgres;
using Dotmim.Sync.Postgres.Scope;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.Postgres.Builders
{
    public class SqlBuilderTrigger : IDbBuilderTriggerHelper
    {
        private ParserName tableName;
        private ParserName trackingName;
        private readonly SyncTable tableDescription;
        private readonly SyncSetup setup;
        private readonly NpgsqlConnection connection;
        private readonly NpgsqlTransaction transaction;
        private readonly NpgsqlObjectNames sqlObjectNames;
        public SqlBuilderTrigger(SyncTable tableDescription, ParserName tableName, ParserName trackingName, SyncSetup setup, DbConnection connection, DbTransaction transaction = null)
        {
            this.connection = connection as NpgsqlConnection;
            this.transaction = transaction as NpgsqlTransaction;

            this.tableDescription = tableDescription;
            this.setup = setup;
            this.tableName = tableName;
            this.trackingName = trackingName;
            this.sqlObjectNames = new NpgsqlObjectNames(this.tableDescription, this.setup);

        }

        private string DeleteTriggerBodyText()
        {
            var stringBuilder = new StringBuilder();


            var stringBuilderArguments = new StringBuilder();
            var stringBuilderArguments2 = new StringBuilder();
            var stringBuilderArguments3 = new StringBuilder();
            var stringPkAreNull = new StringBuilder();

            string argComma = " ";
            string argAnd = string.Empty;
            var primaryKeys = this.tableDescription.GetPrimaryKeysColumns();

            foreach (var mutableColumn in primaryKeys.Where(c => !c.IsReadOnly))
            {
                var columnName = ParserName.Parse(mutableColumn, "\"").Quoted().ToString();
                stringBuilderArguments.AppendLine($"\t{argComma}OLD.{columnName}");
                stringBuilderArguments2.AppendLine($"\t{argComma}{columnName}");
                stringBuilderArguments3.Append($"{argComma}{columnName}");
                stringPkAreNull.Append($"{argAnd}side.{columnName} IS NULL");
                argComma = ",";
                argAnd = " AND ";
            }

            stringBuilder.AppendLine($"INSERT INTO {trackingName.Schema().Quoted().ToString()} (");
            stringBuilder.Append(stringBuilderArguments2.ToString());
            stringBuilder.AppendLine($"\t,update_scope_id");
            stringBuilder.AppendLine($"\t,sync_row_is_tombstone");
            stringBuilder.AppendLine($"\t,last_change_datetime");
            stringBuilder.AppendLine($"\t,timestamp");
            stringBuilder.AppendLine($") ");
            stringBuilder.AppendLine($"VALUES (");
            stringBuilder.Append(stringBuilderArguments.ToString());
            stringBuilder.AppendLine($"\t,NULL");
            stringBuilder.AppendLine($"\t,true");
            stringBuilder.AppendLine($"\t,now()");
            stringBuilder.AppendLine($"\t,{NpgsqlScopeInfoBuilder.TimestampValue}");
            stringBuilder.AppendLine($")");
            stringBuilder.AppendLine($"ON CONFLICT({stringBuilderArguments3.ToString()})");
            stringBuilder.AppendLine($"DO UPDATE SET");
            stringBuilder.AppendLine($"\tsync_row_is_tombstone = true");
            stringBuilder.AppendLine($"\t,update_scope_id = NULL");
            stringBuilder.AppendLine($"\t,last_change_datetime = now()");
            stringBuilder.AppendLine($"\t,timestamp = {NpgsqlScopeInfoBuilder.TimestampValue};");

            return stringBuilder.ToString();
        }

        public virtual async Task CreateDeleteTriggerAsync()
        {
            bool alreadyOpened = this.connection.State == ConnectionState.Open;

            try
            {
                using (var command = new NpgsqlCommand())
                {
                    if (!alreadyOpened)
                        await connection.OpenAsync().ConfigureAwait(false);

                    if (this.transaction != null)
                        command.Transaction = this.transaction;

                    var delTriggerName = this.sqlObjectNames.GetCommandName(DbCommandType.DeleteTrigger).name;
                    var delTriggerParseName = ParserName.Parse(delTriggerName, "\"");

                    var createTrigger = new StringBuilder();
                    createTrigger.AppendLine($"CREATE OR REPLACE FUNCTION {delTriggerName}()");
                    createTrigger.AppendLine($"RETURNS TRIGGER AS");
                    createTrigger.AppendLine($"$BODY$");
                    createTrigger.AppendLine($"BEGIN");
                    createTrigger.AppendLine(this.DeleteTriggerBodyText());
                    createTrigger.AppendLine($"RETURN NULL;");
                    createTrigger.AppendLine($"END;");
                    createTrigger.AppendLine($"$BODY$");
                    createTrigger.AppendLine($"lANGUAGE 'plpgsql';");
                    createTrigger.AppendLine($"DROP TRIGGER IF EXISTS {delTriggerParseName.Quoted().ToString()} on {tableName.Schema().Quoted().ToString()};");
                    createTrigger.AppendLine($"CREATE TRIGGER {delTriggerParseName.Quoted().ToString()} AFTER DELETE ON {tableName.Schema().Quoted().ToString()}");
                    createTrigger.AppendLine($"FOR EACH ROW EXECUTE FUNCTION {delTriggerName}();");
                    createTrigger.AppendLine($"");

                    command.CommandText = createTrigger.ToString();
                    command.Connection = this.connection;
                    await command.ExecuteNonQueryAsync().ConfigureAwait(false);

                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during CreateDeleteTrigger : {ex}");
                throw;

            }
            finally
            {
                if (!alreadyOpened && this.connection.State != ConnectionState.Closed)
                    this.connection.Close();

            }
        }
        public virtual async Task DropDeleteTriggerAsync()
        {
            bool alreadyOpened = this.connection.State == ConnectionState.Open;

            try
            {
                using (var command = new NpgsqlCommand())
                {
                    if (!alreadyOpened)
                        await connection.OpenAsync().ConfigureAwait(false);

                    if (this.transaction != null)
                        command.Transaction = this.transaction;

                    var delTriggerName = this.sqlObjectNames.GetCommandName(DbCommandType.DeleteTrigger).name;

                    command.CommandText = $"DROP TRIGGER {delTriggerName};";
                    command.Connection = this.connection;
                    await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during DropDeleteTrigger : {ex}");
                throw;

            }
            finally
            {
                if (!alreadyOpened && this.connection.State != ConnectionState.Closed)
                    this.connection.Close();

            }
        }

        public virtual async Task AlterDeleteTriggerAsync()
        {
            bool alreadyOpened = this.connection.State == ConnectionState.Open;

            try
            {
                using (var command = new NpgsqlCommand())
                {
                    if (!alreadyOpened)
                        await connection.OpenAsync().ConfigureAwait(false);

                    if (this.transaction != null)
                        command.Transaction = this.transaction;

                    var delTriggerName = this.sqlObjectNames.GetCommandName(DbCommandType.DeleteTrigger).name;
                    var createTrigger = new StringBuilder($"ALTER TRIGGER {delTriggerName} ON {tableName.Schema().Quoted().ToString()} FOR DELETE AS ");
                    createTrigger.AppendLine();
                    createTrigger.AppendLine(this.DeleteTriggerBodyText());

                    command.CommandText = createTrigger.ToString();
                    command.Connection = this.connection;
                    await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during CreateDeleteTrigger : {ex}");
                throw;

            }
            finally
            {
                if (!alreadyOpened && this.connection.State != ConnectionState.Closed)
                    this.connection.Close();

            }


        }

        private string InsertTriggerBodyText()
        {
            var stringBuilder = new StringBuilder();
      

            var stringBuilderArguments = new StringBuilder();
            var stringBuilderArguments2 = new StringBuilder();
            var stringBuilderArguments3 = new StringBuilder();
            var stringPkAreNull = new StringBuilder();

            string argComma = " ";
            string argAnd = string.Empty;
            var primaryKeys = this.tableDescription.GetPrimaryKeysColumns();

            foreach (var mutableColumn in primaryKeys.Where(c => !c.IsReadOnly))
            {
                var columnName = ParserName.Parse(mutableColumn, "\"").Quoted().ToString();
                stringBuilderArguments.AppendLine($"\t{argComma}NEW.{columnName}");
                stringBuilderArguments2.AppendLine($"\t{argComma}{columnName}");
                stringBuilderArguments3.Append($"{argComma}{columnName}");
                stringPkAreNull.Append($"{argAnd}side.{columnName} IS NULL");
                argComma = ",";
                argAnd = " AND ";
            }

            stringBuilder.AppendLine($"INSERT INTO {trackingName.Schema().Quoted().ToString()} (");
            stringBuilder.Append(stringBuilderArguments2.ToString());
            stringBuilder.AppendLine($"\t,update_scope_id");
            stringBuilder.AppendLine($"\t,sync_row_is_tombstone");
            stringBuilder.AppendLine($"\t,last_change_datetime");
            stringBuilder.AppendLine($"\t,timestamp");
            stringBuilder.AppendLine($") ");
            stringBuilder.AppendLine($"VALUES (");
            stringBuilder.Append(stringBuilderArguments.ToString());
            stringBuilder.AppendLine($"\t,NULL");
            stringBuilder.AppendLine($"\t,false");
            stringBuilder.AppendLine($"\t,now()");
            stringBuilder.AppendLine($"\t,{NpgsqlScopeInfoBuilder.TimestampValue}");
            stringBuilder.AppendLine($")");
            stringBuilder.AppendLine($"ON CONFLICT({stringBuilderArguments3.ToString()})");
            stringBuilder.AppendLine($"DO UPDATE SET");
            stringBuilder.AppendLine($"\tsync_row_is_tombstone = false");
            stringBuilder.AppendLine($"\t,update_scope_id = NULL");
            stringBuilder.AppendLine($"\t,last_change_datetime = now()");
            stringBuilder.AppendLine($"\t,timestamp = {NpgsqlScopeInfoBuilder.TimestampValue};");

            return stringBuilder.ToString();
        }
        public virtual async Task CreateInsertTriggerAsync()
        {
            bool alreadyOpened = this.connection.State == ConnectionState.Open;

            try
            {
                using (var command = new NpgsqlCommand())
                {
                    if (!alreadyOpened)
                        await connection.OpenAsync().ConfigureAwait(false);

                    if (this.transaction != null)
                        command.Transaction = this.transaction;

                    var insTriggerName = this.sqlObjectNames.GetCommandName(DbCommandType.InsertTrigger).name;
                    var insTriggerParseName = ParserName.Parse(insTriggerName, "\"");

                    var createTrigger = new StringBuilder();
                    createTrigger.AppendLine($"CREATE OR REPLACE FUNCTION {insTriggerName}()");
                    createTrigger.AppendLine($"RETURNS TRIGGER AS");
                    createTrigger.AppendLine($"$BODY$");
                    createTrigger.AppendLine($"BEGIN");
                    createTrigger.AppendLine(this.InsertTriggerBodyText());
                    createTrigger.AppendLine($"RETURN NULL;");
                    createTrigger.AppendLine($"END;");
                    createTrigger.AppendLine($"$BODY$");
                    createTrigger.AppendLine($"lANGUAGE 'plpgsql';");
                    createTrigger.AppendLine($"DROP TRIGGER IF EXISTS {insTriggerParseName.Quoted().ToString()} on {tableName.Schema().Quoted().ToString()};");
                    createTrigger.AppendLine($"CREATE TRIGGER {insTriggerParseName.Quoted().ToString()} AFTER INSERT ON {tableName.Schema().Quoted().ToString()}");
                    createTrigger.AppendLine($"FOR EACH ROW EXECUTE FUNCTION {insTriggerName}();");
                    createTrigger.AppendLine($"");

                    command.CommandText = createTrigger.ToString();
                    command.Connection = this.connection;
                    await command.ExecuteNonQueryAsync().ConfigureAwait(false);

                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during CreateDeleteTrigger : {ex}");
                throw;

            }
            finally
            {
                if (!alreadyOpened && this.connection.State != ConnectionState.Closed)
                    this.connection.Close();

            }
        }

        public virtual async Task DropInsertTriggerAsync()
        {
            bool alreadyOpened = this.connection.State == ConnectionState.Open;

            try
            {
                using (var command = new NpgsqlCommand())
                {
                    if (!alreadyOpened)
                        await connection.OpenAsync().ConfigureAwait(false);

                    if (this.transaction != null)
                        command.Transaction = this.transaction;

                    var triggerName = this.sqlObjectNames.GetCommandName(DbCommandType.InsertTrigger).name;

                    command.CommandText = $"DROP TRIGGER {triggerName};";
                    command.Connection = this.connection;
                    await command.ExecuteNonQueryAsync().ConfigureAwait(false);

                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during DropDeleteTrigger : {ex}");
                throw;

            }
            finally
            {
                if (!alreadyOpened && this.connection.State != ConnectionState.Closed)
                    this.connection.Close();

            }
        }

        public virtual async Task AlterInsertTriggerAsync()
        {
            bool alreadyOpened = this.connection.State == ConnectionState.Open;

            try
            {
                using (var command = new NpgsqlCommand())
                {
                    if (!alreadyOpened)
                        await connection.OpenAsync().ConfigureAwait(false);

                    if (this.transaction != null)
                        command.Transaction = this.transaction;

                    var insTriggerName = this.sqlObjectNames.GetCommandName(DbCommandType.InsertTrigger).name;
                    var createTrigger = new StringBuilder($"ALTER TRIGGER {insTriggerName} ON {tableName.Schema().Quoted().ToString()} FOR INSERT AS ");
                    createTrigger.AppendLine();
                    createTrigger.AppendLine(this.InsertTriggerBodyText());

                    command.CommandText = createTrigger.ToString();
                    command.Connection = this.connection;
                    await command.ExecuteNonQueryAsync().ConfigureAwait(false);

                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during CreateDeleteTrigger : {ex}");
                throw;

            }
            finally
            {
                if (!alreadyOpened && this.connection.State != ConnectionState.Closed)
                    this.connection.Close();

            }
        }

        private string UpdateTriggerBodyText()
        {
            var stringBuilder = new StringBuilder();


            var stringBuilderArguments = new StringBuilder();
            var stringBuilderArguments2 = new StringBuilder();
            var stringBuilderArguments3 = new StringBuilder();
            var stringPkAreNull = new StringBuilder();

            string argComma = " ";
            string argAnd = string.Empty;
            var primaryKeys = this.tableDescription.GetPrimaryKeysColumns();

            foreach (var mutableColumn in primaryKeys.Where(c => !c.IsReadOnly))
            {
                var columnName = ParserName.Parse(mutableColumn, "\"").Quoted().ToString();
                stringBuilderArguments.AppendLine($"\t{argComma}NEW.{columnName}");
                stringBuilderArguments2.AppendLine($"\t{argComma}{columnName}");
                stringBuilderArguments3.Append($"{argComma}{columnName}");
                stringPkAreNull.Append($"{argAnd}side.{columnName} IS NULL");
                argComma = ",";
                argAnd = " AND ";
            }

            stringBuilder.AppendLine($"INSERT INTO {trackingName.Schema().Quoted().ToString()} (");
            stringBuilder.Append(stringBuilderArguments2.ToString());
            stringBuilder.AppendLine($"\t,update_scope_id");
            stringBuilder.AppendLine($"\t,last_change_datetime");
            stringBuilder.AppendLine($"\t,timestamp");
            stringBuilder.AppendLine($") ");
            stringBuilder.AppendLine($"VALUES (");
            stringBuilder.Append(stringBuilderArguments.ToString());
            stringBuilder.AppendLine($"\t,NULL");
            stringBuilder.AppendLine($"\t,now()");
            stringBuilder.AppendLine($"\t,{NpgsqlScopeInfoBuilder.TimestampValue}");
            stringBuilder.AppendLine($")");
            stringBuilder.AppendLine($"ON CONFLICT({stringBuilderArguments3.ToString()})");
            stringBuilder.AppendLine($"DO UPDATE SET");
            stringBuilder.AppendLine($"\tsync_row_is_tombstone = false");
            stringBuilder.AppendLine($"\t,update_scope_id = NULL");
            stringBuilder.AppendLine($"\t,last_change_datetime = now()");
            stringBuilder.AppendLine($"\t,timestamp = {NpgsqlScopeInfoBuilder.TimestampValue};");

            return stringBuilder.ToString();
        }
        public virtual async Task CreateUpdateTriggerAsync()
        {
            bool alreadyOpened = this.connection.State == ConnectionState.Open;

            try
            {
                using (var command = new NpgsqlCommand())
                {
                    if (!alreadyOpened)
                        await connection.OpenAsync().ConfigureAwait(false);

                    if (this.transaction != null)
                        command.Transaction = this.transaction;

                    var updTriggerName = this.sqlObjectNames.GetCommandName(DbCommandType.UpdateTrigger).name;
                    var updTriggerParseName = ParserName.Parse(updTriggerName, "\"");

                    var createTrigger = new StringBuilder();
                    createTrigger.AppendLine($"CREATE OR REPLACE FUNCTION {updTriggerName}()");
                    createTrigger.AppendLine($"RETURNS TRIGGER AS");
                    createTrigger.AppendLine($"$BODY$");
                    createTrigger.AppendLine($"BEGIN");
                    createTrigger.AppendLine(this.UpdateTriggerBodyText());
                    createTrigger.AppendLine($"RETURN NULL;");
                    createTrigger.AppendLine($"END;");
                    createTrigger.AppendLine($"$BODY$");
                    createTrigger.AppendLine($"lANGUAGE 'plpgsql';");
                    createTrigger.AppendLine($"DROP TRIGGER IF EXISTS {updTriggerParseName.Quoted().ToString()} on {tableName.Schema().Quoted().ToString()};");
                    createTrigger.AppendLine($"CREATE TRIGGER {updTriggerParseName.Quoted().ToString()} AFTER UPDATE ON {tableName.Schema().Quoted().ToString()}");
                    createTrigger.AppendLine($"FOR EACH ROW EXECUTE FUNCTION {updTriggerName}();");
                    createTrigger.AppendLine($"");

                    command.CommandText = createTrigger.ToString();
                    command.Connection = this.connection;
                    await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during CreateDeleteTrigger : {ex}");
                throw;

            }
            finally
            {
                if (!alreadyOpened && this.connection.State != ConnectionState.Closed)
                    this.connection.Close();

            }
        }

        public virtual async Task DropUpdateTriggerAsync()
        {
            bool alreadyOpened = this.connection.State == ConnectionState.Open;

            try
            {
                using (var command = new NpgsqlCommand())
                {
                    if (!alreadyOpened)
                        await connection.OpenAsync().ConfigureAwait(false);

                    if (this.transaction != null)
                        command.Transaction = this.transaction;

                    var triggerName = this.sqlObjectNames.GetCommandName(DbCommandType.UpdateTrigger).name;

                    command.CommandText = $"DROP TRIGGER {triggerName};";
                    command.Connection = this.connection;
                    await command.ExecuteNonQueryAsync().ConfigureAwait(false);

                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during DropDeleteTrigger : {ex}");
                throw;

            }
            finally
            {
                if (!alreadyOpened && this.connection.State != ConnectionState.Closed)
                    this.connection.Close();

            }
        }


        public virtual async Task AlterUpdateTriggerAsync()
        {
            bool alreadyOpened = this.connection.State == ConnectionState.Open;

            try
            {
                using (var command = new NpgsqlCommand())
                {
                    if (!alreadyOpened)
                        await connection.OpenAsync().ConfigureAwait(false);

                    if (this.transaction != null)
                        command.Transaction = this.transaction;

                    var updTriggerName = this.sqlObjectNames.GetCommandName(DbCommandType.UpdateTrigger).name;
                    var createTrigger = new StringBuilder($"ALTER TRIGGER {updTriggerName} ON {tableName.Schema().Quoted().ToString()} FOR UPDATE AS ");
                    createTrigger.AppendLine();
                    createTrigger.AppendLine(this.UpdateTriggerBodyText());

                    command.CommandText = createTrigger.ToString();
                    command.Connection = this.connection;
                    await command.ExecuteNonQueryAsync().ConfigureAwait(false);

                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during CreateDeleteTrigger : {ex}");
                throw;

            }
            finally
            {
                if (!alreadyOpened && this.connection.State != ConnectionState.Closed)
                    this.connection.Close();

            }
        }

        public virtual async Task<bool> NeedToCreateTriggerAsync(DbTriggerType type)
        {

            var updTriggerName = this.sqlObjectNames.GetCommandName(DbCommandType.UpdateTrigger).name;
            var delTriggerName = this.sqlObjectNames.GetCommandName(DbCommandType.DeleteTrigger).name;
            var insTriggerName = this.sqlObjectNames.GetCommandName(DbCommandType.InsertTrigger).name;

            string triggerName = string.Empty;
            switch (type)
            {
                case DbTriggerType.Insert:
                    {
                        triggerName = insTriggerName;
                        break;
                    }
                case DbTriggerType.Update:
                    {
                        triggerName = updTriggerName;
                        break;
                    }
                case DbTriggerType.Delete:
                    {
                        triggerName = delTriggerName;
                        break;
                    }
            }

            return !await NpgsqlManagementUtils.TriggerExistsAsync(connection, transaction, triggerName).ConfigureAwait(false);
        }


    }
}