using Dotmim.Sync.Builders;

using System;
using System.Collections.Generic;
using System.Data;
using Microsoft.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;

namespace Dotmim.Sync.SqlServer
{
    public static class SqlExtensionsMethods
    {

        internal static async Task<SqlParameter[]> DeriveParametersAsync(this SqlConnection connection, SqlCommand cmd, bool includeReturnValueParameter = false, SqlTransaction transaction = null)
        {
            if (cmd == null) throw new ArgumentNullException("SqlCommand");

            var textParser = ParserName.Parse(cmd.CommandText);

            string schemaName = textParser.SchemaName;
            string spName = textParser.ObjectName;

            var alreadyOpened = connection.State == ConnectionState.Open;

            if (!alreadyOpened)
                await connection.OpenAsync().ConfigureAwait(false);

            try
            {
                var getParamsCommand = new SqlCommand("sp_procedure_params_rowset", connection);
                getParamsCommand.CommandType = CommandType.StoredProcedure;
                    getParamsCommand.Transaction = transaction;

                var p = new SqlParameter("@procedure_name", SqlDbType.NVarChar);
                p.Value = spName;
                getParamsCommand.Parameters.Add(p);
                p = new SqlParameter("@procedure_schema", SqlDbType.NVarChar);
                p.Value = schemaName;
                getParamsCommand.Parameters.Add(p);

                using (var sdr = await getParamsCommand.ExecuteReaderAsync().ConfigureAwait(false))
                {
                    // Do we have any rows?
                    if (sdr.HasRows)
                    {
                        // Read the parameter information
                        int ParamNameCol = sdr.GetOrdinal("PARAMETER_NAME");
                        int ParamSizeCol = sdr.GetOrdinal("CHARACTER_MAXIMUM_LENGTH");
                        int ParamTypeCol = sdr.GetOrdinal("TYPE_NAME");
                        int ParamNullCol = sdr.GetOrdinal("IS_NULLABLE");
                        int ParamPrecCol = sdr.GetOrdinal("NUMERIC_PRECISION");
                        int ParamDirCol = sdr.GetOrdinal("PARAMETER_TYPE");
                        int ParamScaleCol = sdr.GetOrdinal("NUMERIC_SCALE");

                        // Loop through and read the rows
                        while (sdr.Read())
                        {
                            string name = sdr.GetString(ParamNameCol);
                            string datatype = sdr.GetString(ParamTypeCol);

                            // Is this xml?
                            // ADO.NET 1.1 does not support XML, replace with text
                            //if (0 == String.Compare("xml", datatype, true))
                            //    datatype = "Text";

                            if (0 == String.Compare("table", datatype, true))
                                datatype = "Structured";

                            // TODO : Should we raise an error here ??
                            if (!Enum.TryParse(datatype, true, out SqlDbType type))
                                type = SqlDbType.Variant;

                            bool Nullable = sdr.GetBoolean(ParamNullCol);
                            var param = new SqlParameter(name, type);

                            // Determine parameter direction
                            int dir = sdr.GetInt16(ParamDirCol);
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
                            param.IsNullable = Nullable;
                            if (!sdr.IsDBNull(ParamPrecCol))
                                param.Precision = (Byte)sdr.GetInt16(ParamPrecCol);
                            if (!sdr.IsDBNull(ParamSizeCol))
                                param.Size = sdr.GetInt32(ParamSizeCol);
                            if (!sdr.IsDBNull(ParamScaleCol))
                                param.Scale = (Byte)sdr.GetInt16(ParamScaleCol);

                            cmd.Parameters.Add(param);
                        }
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
                Value = param.Value
            };

            return p;
        }

    }
}
