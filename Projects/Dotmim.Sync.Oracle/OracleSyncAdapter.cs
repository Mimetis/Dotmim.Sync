using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.OracleClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Data;
using Dotmim.Sync.Oracle.Builder;
using Dotmim.Sync.Oracle.Manager;

namespace Dotmim.Sync.Oracle
{
    public class OracleSyncAdapter : DbSyncAdapter
    {
        private readonly OracleConnection _connection;
        private readonly OracleTransaction _transaction;
        private OracleObjectNames oracleObjectNames;
        private OracleDbMetadata oracleMetadata;

        // Derive Parameters cache
        private static Dictionary<string, List<OracleParameter>> derivingParameters = new Dictionary<string, List<OracleParameter>>();

        public OracleSyncAdapter(DmTable tableDescription) : base(tableDescription)
        {
        }

        public OracleSyncAdapter(DmTable tableDescription, DbConnection connection, DbTransaction transaction) 
            : this(tableDescription)
        {
            var sqlc = connection as OracleConnection;
            this._connection = sqlc ?? throw new InvalidCastException("Connection should be a OracleConnection");

            _transaction = transaction as OracleTransaction;
            this.oracleObjectNames = new OracleObjectNames(tableDescription);
            this.oracleMetadata = new OracleDbMetadata();
        }

        public override DbConnection Connection => _connection;

        public override DbTransaction Transaction => _transaction;

        public override void ExecuteBatchCommand(DbCommand cmd, DmView applyTable, DmTable failedRows, ScopeInfo scope)
        {
            throw new NotImplementedException();
        }

        public override DbCommand GetCommand(DbCommandType commandType, IEnumerable<string> additionals = null)
        {
            var command = this.Connection.CreateCommand();

            string text;
            if (additionals != null)
                text = this.oracleObjectNames.GetCommandName(commandType, additionals);
            else
                text = this.oracleObjectNames.GetCommandName(commandType);

            command.CommandType = CommandType.Text;
            command.CommandText = text;
            command.Connection = Connection;

            if (Transaction != null)
                command.Transaction = Transaction;

            return command;
        }

        public override bool IsPrimaryKeyViolation(Exception Error)
        {
            OracleException error = Error as OracleException;
            if (error != null && error.Code == 2627)
                return true;
            return false;
        }

        public override void SetCommandParameters(DbCommandType commandType, DbCommand command)
        {
            if (command == null)
                return;

            if (command.Parameters != null && command.Parameters.Count > 0)
                return;

            bool alreadyOpened = this._connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    this._connection.Open();

                if (this._transaction != null)
                    command.Transaction = this._transaction;

                var textParser = new ObjectNameParser(command.CommandText);

                if (derivingParameters.ContainsKey(textParser.UnquotedString))
                {
                    foreach (var p in derivingParameters[textParser.UnquotedString])
                        command.Parameters.Add(p.Clone());
                }
                else
                {
                    var parameters = _connection.DeriveParameters((OracleCommand)command, false, _transaction);

                    var arrayParameters = new List<OracleParameter>();
                    foreach (var p in parameters)
                        arrayParameters.Add(p.Clone());

                    derivingParameters.Add(textParser.UnquotedString, arrayParameters);
                }

                if (command.Parameters[0].ParameterName == "@RETURN_VALUE")
                    command.Parameters.RemoveAt(0);

            }
            catch (Exception ex)
            {
                Debug.WriteLine($"DeriveParameters failed : {ex}");
                throw;
            }
            finally
            {
                if (!alreadyOpened && this._connection.State != ConnectionState.Closed)
                    this._connection.Close();
            }


            foreach (var parameter in command.Parameters)
            {
                var sqlParameter = (OracleParameter)parameter;

                // try to get the source column (from the dmTable)
                var sqlParameterName = sqlParameter.ParameterName.Replace("@", "");
                var colDesc = TableDescription.Columns.FirstOrDefault(c => string.Equals(c.ColumnName, sqlParameterName, StringComparison.CurrentCultureIgnoreCase));

                if (colDesc != null && !string.IsNullOrEmpty(colDesc.ColumnName))
                    sqlParameter.SourceColumn = colDesc.ColumnName;
            }
        }
    }
}
