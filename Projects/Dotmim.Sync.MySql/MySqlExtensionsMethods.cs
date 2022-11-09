
using System;
using System.Data;
#if NET5_0 || NET6_0 || NET7_0  || NETCOREAPP3_1
using MySqlConnector;
#elif NETSTANDARD
using MySql.Data.MySqlClient;
#endif

using System.Collections.Generic;

#if MARIADB
using Dotmim.Sync.MariaDB.Builders;
#elif MYSQL
using Dotmim.Sync.MySql.Builders;
#endif

using System.Data.SqlTypes;
using System.Text;
using System.Globalization;

#if MARIADB
namespace Dotmim.Sync.MariaDB
#elif MYSQL
namespace Dotmim.Sync.MySql
#endif
{
    public static class MySqlExtensionsMethods
    {
        public static MySqlParameter[] DeriveParameters(this MySqlConnection connection, 
            MySqlCommand cmd, bool includeReturnValueParameter = false, 
            MySqlTransaction transaction = null)
        {

            throw new NotImplementedException("Implementation in progress");

            //if (cmd == null) throw new ArgumentNullException("SqlCommand");

            //var textParser = new ObjectNameParser(cmd.CommandText);

            //// Hack to check for schema name in the spName
            //string schemaName = "dbo";
            //string spName = textParser.UnquotedString;
            //int firstDot = spName.IndexOf('.');
            //if (firstDot > 0)
            //{
            //    schemaName = cmd.CommandText.Substring(0, firstDot);
            //    spName = spName.Substring(firstDot + 1);
            //}

            //var alreadyOpened = connection.State == ConnectionState.Open;

            //if (!alreadyOpened)
            //    connection.Open();

            //try
            //{
            //    var dmParameters = GetProcedureParameters(connection, connection.Database, spName);
                

            //}
            //finally
            //{
            //    if (!alreadyOpened)
            //        connection.Close();
            //}

            //if (!includeReturnValueParameter && cmd.Parameters.Count > 0)
            //    cmd.Parameters.RemoveAt(0);

            //MySqlParameter[] discoveredParameters = new MySqlParameter[cmd.Parameters.Count];

            //cmd.Parameters.CopyTo(discoveredParameters, 0);

            //// Init the parameters with a DBNull value
            //foreach (MySqlParameter discoveredParameter in discoveredParameters)
            //    discoveredParameter.Value = DBNull.Value;

            //return discoveredParameters;

        }


        /// <summary>
        /// Return schema information about parameters for procedures and functions
        /// </summary>
        internal static SyncTable GetProcedureParameters(this MySqlConnection connection, string schema, string procName)
        {
            // we want to avoid using IS if  we can as it is painfully slow
            var parametersTable = CreateParametersTable();

            var cmd = connection.CreateCommand();

            string showCreateSql = $"SHOW CREATE PROCEDURE `{schema}`.`{procName}`";
            cmd.CommandText = showCreateSql;
            try
            {

                using MySqlDataReader reader = cmd.ExecuteReader();
                reader.Read();
                string body = reader.GetString(2);
                string sqlMode = reader.GetString(1);
                reader.Close();
                ParseProcedureBody(parametersTable, body, sqlMode, schema, procName);
            }
            catch (Exception)
            {
                throw new InvalidOperationException($"Unable to retrieve parameters on PROCEDURE {procName}");
            }

            return parametersTable;
        }


        private static void InitParameterRow(SyncRow parameter, string schema, string procName)
        {
            parameter["SPECIFIC_CATALOG"] = null;
            parameter["SPECIFIC_SCHEMA"] = schema;
            parameter["SPECIFIC_NAME"] = procName;
            parameter["PARAMETER_MODE"] = "IN";
            parameter["ORDINAL_POSITION"] = 0;
        }

        private static void ParseProcedureBody(SyncTable parametersTable, string body, string sqlMode, string schema, string procName)
        {
            var modes = new List<string>(new string[3] { "IN", "OUT", "INOUT" });

            int pos = 1;
            var tokenizer = new MySqlTokenizer(body)
            {
                AnsiQuotes = sqlMode.IndexOf("ANSI_QUOTES") != -1,
                BackslashEscapes = sqlMode.IndexOf("NO_BACKSLASH_ESCAPES") == -1,
                ReturnComments = false
            };
            string token = tokenizer.NextToken();

            // this block will scan for the opening paren while also determining
            // if this routine is a function.  If so, then we need to add a
            // parameter row for the return parameter since it is ordinal position
            // 0 and should appear first.
            while (token != "(")
            {
                if (String.Compare(token, "FUNCTION", StringComparison.OrdinalIgnoreCase) == 0)
                {
                    var newRow = parametersTable.NewRow();
                    InitParameterRow(newRow, schema, procName);
                    parametersTable.Rows.Add(newRow);
                }
                token = tokenizer.NextToken();
            }
            token = tokenizer.NextToken();  // now move to the next token past the (

            while (token != ")")
            {
                var parmRow = parametersTable.NewRow();
                InitParameterRow(parmRow, schema, procName);
                parmRow["ORDINAL_POSITION"] = pos++;

                // handle mode and name for the parameter
                string mode = token.ToUpperInvariant();
                if (!tokenizer.Quoted && modes.Contains(mode))
                {
                    parmRow["PARAMETER_MODE"] = mode;
                    token = tokenizer.NextToken();
                }
                if (tokenizer.Quoted)
                    token = token.Substring(1, token.Length - 2);
                parmRow["PARAMETER_NAME"] = token;

                // now parse data type
                token = ParseDataType(parmRow, tokenizer);
                if (token == ",")
                    token = tokenizer.NextToken();

                    parametersTable.Rows.Add(parmRow);
            }

            // now parse out the return parameter if there is one.
            token = tokenizer.NextToken().ToUpperInvariant();

            if (String.Compare(token, "RETURNS", StringComparison.OrdinalIgnoreCase) == 0)
            {
                var parameterRow = parametersTable.Rows[0];
                parameterRow["PARAMETER_NAME"] = "RETURN_VALUE";
                ParseDataType(parameterRow, tokenizer);
            }
        }

        private static SyncTable CreateParametersTable()
        {
            var dt = new SyncTable("Procedure Parameters");
            dt.Columns.Add("SPECIFIC_CATALOG", typeof(string));
            dt.Columns.Add("SPECIFIC_SCHEMA", typeof(string));
            dt.Columns.Add("SPECIFIC_NAME", typeof(string));
            dt.Columns.Add("ORDINAL_POSITION", typeof(Int32));
            dt.Columns.Add("PARAMETER_MODE", typeof(string));
            dt.Columns.Add("PARAMETER_NAME", typeof(string));
            dt.Columns.Add("DATA_TYPE", typeof(string));
            dt.Columns.Add("CHARACTER_MAXIMUM_LENGTH", typeof(Int32));
            dt.Columns.Add("CHARACTER_OCTET_LENGTH", typeof(Int32));
            dt.Columns.Add("NUMERIC_PRECISION", typeof(byte));
            dt.Columns.Add("NUMERIC_SCALE", typeof(Int32));
            dt.Columns.Add("CHARACTER_SET_NAME", typeof(string));
            dt.Columns.Add("COLLATION_NAME", typeof(string));
            dt.Columns.Add("DTD_IDENTIFIER", typeof(string));
            dt.Columns.Add("ROUTINE_TYPE", typeof(string));
            return dt;
        }

        private static string ParseDataType(SyncRow row, MySqlTokenizer tokenizer)
        {
            StringBuilder dtd = new StringBuilder(tokenizer.NextToken().ToUpperInvariant());

            row["DATA_TYPE"] = dtd.ToString();
            string type = row["DATA_TYPE"].ToString();

            string token = tokenizer.NextToken();
            if (token == "(")
            {
                token = tokenizer.ReadParenthesis();
                dtd.AppendFormat(CultureInfo.InvariantCulture, "{0}", token);

                if (type != "ENUM" && type != "SET")
                    ParseDataTypeSize(row, token);
                token = tokenizer.NextToken();
            }
            else
                dtd.Append(GetDataTypeDefaults(type, row));

            while (token != ")" &&
                   token != "," &&
                   String.Compare(token, "begin", StringComparison.OrdinalIgnoreCase) != 0 &&
                   String.Compare(token, "return", StringComparison.OrdinalIgnoreCase) != 0)
            {
                if (String.Compare(token, "CHARACTER", StringComparison.OrdinalIgnoreCase) == 0 ||
                    String.Compare(token, "BINARY", StringComparison.OrdinalIgnoreCase) == 0)
                { }  // we don't need to do anything with this
                else if (String.Compare(token, "SET", StringComparison.OrdinalIgnoreCase) == 0 ||
                         String.Compare(token, "CHARSET", StringComparison.OrdinalIgnoreCase) == 0)
                    row["CHARACTER_SET_NAME"] = tokenizer.NextToken();
                else if (String.Compare(token, "ASCII", StringComparison.OrdinalIgnoreCase) == 0)
                    row["CHARACTER_SET_NAME"] = "latin1";
                else if (String.Compare(token, "UNICODE", StringComparison.OrdinalIgnoreCase) == 0)
                    row["CHARACTER_SET_NAME"] = "ucs2";
                else if (String.Compare(token, "COLLATE", StringComparison.OrdinalIgnoreCase) == 0)
                    row["COLLATION_NAME"] = tokenizer.NextToken();
                else
                    dtd.AppendFormat(CultureInfo.InvariantCulture, " {0}", token);
                token = tokenizer.NextToken();
            }

            if (dtd.Length > 0)
                row["DTD_IDENTIFIER"] = dtd.ToString();

            // now default the collation if one wasn't given
            //if (string.IsNullOrEmpty((string)row["COLLATION_NAME"]) &&
            //    !string.IsNullOrEmpty((string)row["CHARACTER_SET_NAME"]))
            //    row["COLLATION_NAME"] = CharSetMap.GetDefaultCollation(
            //        row["CHARACTER_SET_NAME"].ToString(), connection);

            // now set the octet length
            //if (row["CHARACTER_MAXIMUM_LENGTH"] != null)
            //{
            //    if (row["CHARACTER_SET_NAME"] == null)
            //        row["CHARACTER_SET_NAME"] = "";
            //    row["CHARACTER_OCTET_LENGTH"] =
            //        CharSetMap.GetMaxLength((string)row["CHARACTER_SET_NAME"], connection) *
            //        (int)row["CHARACTER_MAXIMUM_LENGTH"];
            //}

            return token;
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
            var metadata = new MySqlDbMetadata();

            string format = "({0},{1})";
            object precision = row["NUMERIC_PRECISION"];

            if (IsNumericType(type) && string.IsNullOrEmpty((string)row["NUMERIC_PRECISION"]))
            {
                row["NUMERIC_PRECISION"] = 10;
                row["NUMERIC_SCALE"] = 0;

                if (type.ToLowerInvariant() == "numeric" || type.ToLowerInvariant() == "decimal" ||
                    type.ToLowerInvariant() == "dec" || type.ToLowerInvariant() == "real")
                    format = "({0})";

                return String.Format(format, row["NUMERIC_PRECISION"],
                    row["NUMERIC_SCALE"]);
            }
            return String.Empty;
        }

        private static void ParseDataTypeSize(SyncRow row, string size)
        {
            var metadata = new MySqlDbMetadata();
            size = size.Trim('(', ')');
            string[] parts = size.Split(',');

            if (!IsNumericType(row["DATA_TYPE"].ToString()))
            {
                row["CHARACTER_MAXIMUM_LENGTH"] = Int32.Parse(parts[0]);
                // will set octet length in a minute
            }
            else
            {
                row["NUMERIC_PRECISION"] = Int32.Parse(parts[0]);
                if (parts.Length == 2)
                    row["NUMERIC_SCALE"] = Int32.Parse(parts[1]);
            }
        }

    }
}
