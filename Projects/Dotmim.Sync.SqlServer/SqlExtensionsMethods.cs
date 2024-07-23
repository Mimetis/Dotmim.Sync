using Dotmim.Sync.Builders;
using Microsoft.Data.SqlClient;
using System;
using System.Data;

namespace Dotmim.Sync.SqlServer
{
    public static class SqlExtensionsMethods
    {

        internal static SqlParameter[] DeriveParameters(this SqlConnection connection, SqlCommand cmd, bool includeReturnValueParameter = false, SqlTransaction transaction = null)
        {
            Guard.ThrowIfNull(cmd);

            var textParser = ParserName.Parse(cmd.CommandText);

            string schemaName = textParser.SchemaName;
            string spName = textParser.ObjectName;

            var alreadyOpened = connection.State == ConnectionState.Open;

            if (!alreadyOpened)
                connection.Open();

            try
            {
                using var getParamsCommand = new SqlCommand("sp_procedure_params_rowset", connection);
                getParamsCommand.CommandType = CommandType.StoredProcedure;
                getParamsCommand.Transaction = transaction;

                var p = new SqlParameter("@procedure_name", SqlDbType.NVarChar) { Value = spName };
                getParamsCommand.Parameters.Add(p);
                p = new SqlParameter("@procedure_schema", SqlDbType.NVarChar) { Value = schemaName };
                getParamsCommand.Parameters.Add(p);

                using var sdr = getParamsCommand.ExecuteReader();

                // Do we have any rows?
                if (sdr.HasRows)
                {
                    // Read the parameter information
                    int paramNameCol = sdr.GetOrdinal("PARAMETER_NAME");
                    int paramSizeCol = sdr.GetOrdinal("CHARACTER_MAXIMUM_LENGTH");
                    int paramTypeCol = sdr.GetOrdinal("TYPE_NAME");
                    int paramNullCol = sdr.GetOrdinal("IS_NULLABLE");
                    int paramPrecCol = sdr.GetOrdinal("NUMERIC_PRECISION");
                    int paramDirCol = sdr.GetOrdinal("PARAMETER_TYPE");
                    int paramScaleCol = sdr.GetOrdinal("NUMERIC_SCALE");

                    // Loop through and read the rows
                    while (sdr.Read())
                    {
                        string name = sdr.GetString(paramNameCol);
                        string datatype = sdr.GetString(paramTypeCol);

                        // Is this xml?
                        // ADO.NET 1.1 does not support XML, replace with text
                        // if (0 == String.Compare("xml", datatype, true))
                        //    datatype = "Text";
                        if (string.Equals("table", datatype, StringComparison.OrdinalIgnoreCase))
                            datatype = "Structured";

                        // TODO : Should we raise an error here ??
                        if (!Enum.TryParse(datatype, true, out SqlDbType type))
                            type = SqlDbType.Variant;

                        bool nullable = sdr.GetBoolean(paramNullCol);
                        var param = new SqlParameter(name, type);

                        // Determine parameter direction
                        int dir = sdr.GetInt16(paramDirCol);
                        switch (dir)
                        {
                            case 1:
                                param.Direction = ParameterDirection.Input;
                                break;
                            case 2:
                                param.Direction = ParameterDirection.Output;
                                break;
                            case 3:
                                param.Direction = ParameterDirection.InputOutput;
                                break;
                            case 4:
                                param.Direction = ParameterDirection.ReturnValue;
                                break;
                        }

                        param.IsNullable = nullable;
                        if (!sdr.IsDBNull(paramPrecCol))
                            param.Precision = (byte)sdr.GetInt16(paramPrecCol);
                        if (!sdr.IsDBNull(paramSizeCol))
                            param.Size = sdr.GetInt32(paramSizeCol);
                        if (!sdr.IsDBNull(paramScaleCol))
                            param.Scale = (byte)sdr.GetInt16(paramScaleCol);

                        cmd.Parameters.Add(param);
                    }
                }
            }
            finally
            {
                if (!alreadyOpened)
                    connection.Close();
            }

            if (!includeReturnValueParameter && cmd.Parameters.Count > 0)
                cmd.Parameters.RemoveAt(0);

            SqlParameter[] discoveredParameters = new SqlParameter[cmd.Parameters.Count];

            cmd.Parameters.CopyTo(discoveredParameters, 0);

            // Init the parameters with a DBNull value
            foreach (var discoveredParameter in discoveredParameters)
                discoveredParameter.Value = DBNull.Value;

            return discoveredParameters;
        }

        internal static SqlParameter Clone(this SqlParameter param)
        {
            var p = new SqlParameter
            {
                DbType = param.DbType,
                Direction = param.Direction,
                IsNullable = param.IsNullable,
                ParameterName = param.ParameterName,
                Precision = param.Precision,
                Scale = param.Scale,
                Size = param.Size,
                SourceColumn = param.SourceColumn,
                SqlDbType = param.SqlDbType,
                SqlValue = param.SqlValue,
                TypeName = param.TypeName,
                Value = param.Value,
            };

            return p;
        }
    }
}