using System;
#if NET6_0 || NET8_0
using MySqlConnector;

#elif NETSTANDARD
using MySql.Data.MySqlClient;
#endif

#if MARIADB
using Dotmim.Sync.MariaDB.Builders;
#elif MYSQL
#endif
#if MARIADB
namespace Dotmim.Sync.MariaDB
#elif MYSQL
namespace Dotmim.Sync.MySql
#endif
{
    /// <summary>
    /// MySql Extensions Methods.
    /// </summary>
    public static class MySqlExtensionsMethods
    {
        /// <summary>
        /// Not yet implemented.
        /// </summary>
        public static MySqlParameter[] DeriveParameters(
            this MySqlConnection connection,
            MySqlCommand cmd, bool includeReturnValueParameter = false,
            MySqlTransaction transaction = null)
        {

            throw new NotImplementedException("Implementation in progress");

            // if (cmd == null) throw new ArgumentNullException("SqlCommand");

            // var textParser = new ObjectNameParser(cmd.CommandText);

            //// Hack to check for schema name in the spName
            // string schemaName = "dbo";
            // string spName = textParser.UnquotedString;
            // int firstDot = spName.IndexOf('.');
            // if (firstDot > 0)
            // {
            //    schemaName = cmd.CommandText.Substring(0, firstDot);
            //    spName = spName.Substring(firstDot + 1);
            // }

            // var alreadyOpened = connection.State == ConnectionState.Open;

            // if (!alreadyOpened)
            //    connection.Open();

            // try
            // {
            //    var dmParameters = GetProcedureParameters(connection, connection.Database, spName);

            // }
            // finally
            // {
            //    if (!alreadyOpened)
            //        connection.Close();
            // }

            // if (!includeReturnValueParameter && cmd.Parameters.Count > 0)
            //    cmd.Parameters.RemoveAt(0);

            // MySqlParameter[] discoveredParameters = new MySqlParameter[cmd.Parameters.Count];

            // cmd.Parameters.CopyTo(discoveredParameters, 0);

            //// Init the parameters with a DBNull value
            // foreach (MySqlParameter discoveredParameter in discoveredParameters)
            //    discoveredParameter.Value = DBNull.Value;

            // return discoveredParameters;
        }

        private static void InitParameterRow(SyncRow parameter, string schema, string procName)
        {
            parameter["SPECIFIC_CATALOG"] = null;
            parameter["SPECIFIC_SCHEMA"] = schema;
            parameter["SPECIFIC_NAME"] = procName;
            parameter["PARAMETER_MODE"] = "IN";
            parameter["ORDINAL_POSITION"] = 0;
        }

        private static SyncTable CreateParametersTable()
        {
            var dt = new SyncTable("Procedure Parameters");
            dt.Columns.Add("SPECIFIC_CATALOG", typeof(string));
            dt.Columns.Add("SPECIFIC_SCHEMA", typeof(string));
            dt.Columns.Add("SPECIFIC_NAME", typeof(string));
            dt.Columns.Add("ORDINAL_POSITION", typeof(int));
            dt.Columns.Add("PARAMETER_MODE", typeof(string));
            dt.Columns.Add("PARAMETER_NAME", typeof(string));
            dt.Columns.Add("DATA_TYPE", typeof(string));
            dt.Columns.Add("CHARACTER_MAXIMUM_LENGTH", typeof(int));
            dt.Columns.Add("CHARACTER_OCTET_LENGTH", typeof(int));
            dt.Columns.Add("NUMERIC_PRECISION", typeof(byte));
            dt.Columns.Add("NUMERIC_SCALE", typeof(int));
            dt.Columns.Add("CHARACTER_SET_NAME", typeof(string));
            dt.Columns.Add("COLLATION_NAME", typeof(string));
            dt.Columns.Add("DTD_IDENTIFIER", typeof(string));
            dt.Columns.Add("ROUTINE_TYPE", typeof(string));
            return dt;
        }

        private static bool IsNumericType(string datatype) => datatype.ToLowerInvariant() switch
        {
            "int" or "int16" or "int24" or "int32" or "int64" or "uint16" or "uint24" or "uint32" or "uint64" or "integer" or
            "numeric" or "decimal" or "dec" or "fixed" or "tinyint" or "mediumint" or "bigint" or "real" or "double" or
            "float" or "serial" or "smallint" => true,
            _ => false,
        };

        private static string GetDataTypeDefaults(string type, SyncRow row)
        {
            string format = "({0},{1})";

            if (IsNumericType(type) && string.IsNullOrEmpty((string)row["NUMERIC_PRECISION"]))
            {
                row["NUMERIC_PRECISION"] = 10;
                row["NUMERIC_SCALE"] = 0;

                if (string.Equals(type, "numeric", SyncGlobalization.DataSourceStringComparison)
                    || string.Equals(type, "decimal", SyncGlobalization.DataSourceStringComparison)
                    || string.Equals(type, "dec", SyncGlobalization.DataSourceStringComparison)
                    || string.Equals(type, "real", SyncGlobalization.DataSourceStringComparison))
                {
                    format = "({0})";
                }

                return string.Format(format, row["NUMERIC_PRECISION"],
                    row["NUMERIC_SCALE"]);
            }

            return string.Empty;
        }

        private static void ParseDataTypeSize(SyncRow row, string size)
        {
            size = size.Trim('(', ')');
            string[] parts = size.Split(',');

            if (!IsNumericType(row["DATA_TYPE"].ToString()))
            {
                row["CHARACTER_MAXIMUM_LENGTH"] = int.Parse(parts[0]);

                // will set octet length in a minute
            }
            else
            {
                row["NUMERIC_PRECISION"] = int.Parse(parts[0]);
                if (parts.Length == 2)
                    row["NUMERIC_SCALE"] = int.Parse(parts[1]);
            }
        }
    }
}