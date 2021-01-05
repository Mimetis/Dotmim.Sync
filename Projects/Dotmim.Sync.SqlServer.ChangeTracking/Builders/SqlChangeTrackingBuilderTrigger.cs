using Dotmim.Sync.Builders;



using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using Microsoft.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.SqlServer.Builders
{
    public class SqlChangeTrackingBuilderTrigger : SqlBuilderTrigger
    {
        private ParserName tableName;
        public SqlChangeTrackingBuilderTrigger(SyncTable tableDescription, ParserName tableName, ParserName trackingName, SyncSetup setup)
            : base(tableDescription, tableName, trackingName, setup)
        {
            this.tableName = tableName;
        }

        public override Task<DbCommand> GetCreateTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction)
                        => Task.FromResult<DbCommand>(null);
        public override Task<DbCommand> GetDropTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction)
                        => Task.FromResult<DbCommand>(null);

        public override Task<DbCommand> GetExistsTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction)
        {

            var commandText = $"IF EXISTS (Select top 1 tbl.name as TableName, " +
                              $"sch.name as SchemaName " +
                              $"  from sys.change_tracking_tables tr " +
                              $"  Inner join sys.tables as tbl on tbl.object_id = tr.object_id " +
                              $"  Inner join sys.schemas as sch on tbl.schema_id = sch.schema_id " +
                              $"  Where tbl.name = @tableName and sch.name = @schemaName) SELECT 1 ELSE SELECT 0;";

            var tbl = this.tableName.Unquoted().ToString();
            var schema = SqlManagementUtils.GetUnquotedSqlSchemaName(this.tableName);

            var command = connection.CreateCommand();

            command.Connection = connection;
            command.Transaction = transaction;
            command.CommandText = commandText;

            var parameter = command.CreateParameter();
            parameter.ParameterName = "@tableName";
            parameter.Value = tbl;
            command.Parameters.Add(parameter);

            parameter = command.CreateParameter();
            parameter.ParameterName = "@schemaName";
            parameter.Value = schema;
            command.Parameters.Add(parameter);

            return Task.FromResult(command);

        }

    }


}
