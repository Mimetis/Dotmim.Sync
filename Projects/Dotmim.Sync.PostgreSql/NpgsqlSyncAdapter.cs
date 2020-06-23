using Dotmim.Sync.Builders;
using Npgsql;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;

namespace Dotmim.Sync.Postgres.Builders
{
    public class NpgsqlSyncAdapter : SyncAdapter
    {
        private NpgsqlObjectNames sqlObjectNames;
        private NpgsqlDbMetadata sqlMetadata;

        // Derive Parameters cache
        // Be careful, we can have collision between databasesNpgsqlParameter
        // this static class could be shared accross databases with same command name
        // but different table schema
        // So the string should contains the connection string as well
        private static ConcurrentDictionary<string, List<NpgsqlParameter>> derivingParameters
            = new ConcurrentDictionary<string, List<NpgsqlParameter>>();

        public NpgsqlSyncAdapter(SyncTable tableDescription, ParserName tableName, ParserName trackingName, SyncSetup setup) : base(tableDescription, setup)
        {
            this.sqlObjectNames = new NpgsqlObjectNames(tableDescription, setup);
            this.sqlMetadata = new NpgsqlDbMetadata();
        }


        /// <summary>
        /// Executing a batch command
        /// </summary>
        public override async Task ExecuteBatchCommandAsync(DbCommand cmd, Guid senderScopeId, IEnumerable<SyncRow> applyRows, SyncTable schemaChangesTable, SyncTable failedRows, long lastTimestamp, DbConnection connection, DbTransaction transaction = null)
        {

            var applyRowsCount = applyRows.Count();

            if (applyRowsCount <= 0)
                return;

            var dataRowState = DataRowState.Unchanged;

            bool alreadyOpened = connection.State == ConnectionState.Open;
            try
            {
                foreach (var row in applyRows)
                {

                }
            }
            catch (DbException ex)
            {
                Debug.WriteLine(ex.Message);
                throw;
            }
            finally
            {

                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();

            }
        }


        private static TypeConverter Int16Converter = TypeDescriptor.GetConverter(typeof(short));
        private static TypeConverter Int32Converter = TypeDescriptor.GetConverter(typeof(int));
        private static TypeConverter Int64Converter = TypeDescriptor.GetConverter(typeof(long));
        private static TypeConverter UInt16Converter = TypeDescriptor.GetConverter(typeof(ushort));
        private static TypeConverter UInt32Converter = TypeDescriptor.GetConverter(typeof(uint));
        private static TypeConverter UInt64Converter = TypeDescriptor.GetConverter(typeof(ulong));
        private static TypeConverter DateTimeConverter = TypeDescriptor.GetConverter(typeof(DateTime));
        private static TypeConverter StringConverter = TypeDescriptor.GetConverter(typeof(string));
        private static TypeConverter ByteConverter = TypeDescriptor.GetConverter(typeof(byte));
        private static TypeConverter BoolConverter = TypeDescriptor.GetConverter(typeof(bool));
        private static TypeConverter GuidConverter = TypeDescriptor.GetConverter(typeof(Guid));
        private static TypeConverter CharConverter = TypeDescriptor.GetConverter(typeof(char));
        private static TypeConverter DecimalConverter = TypeDescriptor.GetConverter(typeof(decimal));
        private static TypeConverter DoubleConverter = TypeDescriptor.GetConverter(typeof(double));
        private static TypeConverter FloatConverter = TypeDescriptor.GetConverter(typeof(float));
        private static TypeConverter SByteConverter = TypeDescriptor.GetConverter(typeof(sbyte));
        private static TypeConverter TimeSpanConverter = TypeDescriptor.GetConverter(typeof(TimeSpan));


        /// <summary>
        /// Check if an exception is a primary key exception
        /// </summary>
        public override bool IsPrimaryKeyViolation(Exception exception)
        {

            return false;
        }


        public override bool IsUniqueKeyViolation(Exception exception)
        {
            return false;
        }

        public override DbCommand GetCommand(DbCommandType nameType, SyncFilter filter)
        {
            var command = new NpgsqlCommand();

            string text;
            bool isStoredProc;

            (text, isStoredProc) = this.sqlObjectNames.GetCommandName(nameType, filter);

            command.CommandType = isStoredProc ? CommandType.StoredProcedure : CommandType.Text;
            command.CommandText = text;

            return command;
        }

        /// <summary>
        /// Set a stored procedure parameters
        /// </summary>
        public override async Task AddCommandParametersAsync(DbCommandType commandType, DbCommand command, DbConnection connection, DbTransaction transaction = null, SyncFilter filter = null)
        {
            if (command == null)
                return;

            if (command.Parameters != null && command.Parameters.Count > 0)
                return;

            // special case for constraint
            if (commandType == DbCommandType.DisableConstraints || commandType == DbCommandType.EnableConstraints)
                return;

            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                if (transaction != null)
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
                    NpgsqlCommandBuilder.DeriveParameters((NpgsqlCommand)command);

                    var arrayParameters = new List<NpgsqlParameter>();
                    foreach (var p in command.Parameters)
                        arrayParameters.Add(((NpgsqlParameter)p).Clone());

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
                var sqlParameter = (NpgsqlParameter)parameter;

                // try to get the source column (from the SchemaTable)
                var sqlParameterName = sqlParameter.ParameterName.Replace("@", "");
                var colDesc = TableDescription.Columns.FirstOrDefault(c => c.ColumnName.Equals(sqlParameterName, SyncGlobalization.DataSourceStringComparison));

                if (colDesc != null && !string.IsNullOrEmpty(colDesc.ColumnName))
                    sqlParameter.SourceColumn = colDesc.ColumnName;
            }
        }

    }
}
