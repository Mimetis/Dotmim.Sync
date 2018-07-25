using Dotmim.Sync.Builders;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.OracleClient;
using System.Text;

namespace Dotmim.Sync.Oracle
{
    public static class OracleExtensionsMethods
    {
        internal static OracleParameter[] DeriveParameters(this OracleConnection connection, OracleCommand cmd, bool includeReturnValueParameter = false, OracleTransaction transaction = null)
        {
            if (cmd == null) throw new ArgumentNullException("OracleCommand");

            var textParser = new ObjectNameParser(cmd.CommandText);

            string schemaName = textParser.SchemaName;
            string spName = textParser.ObjectName;

            var alreadyOpened = connection.State == ConnectionState.Open;

            if (!alreadyOpened)
                connection.Open();

            try
            {
                StringBuilder sb = new StringBuilder();
                sb.AppendLine("SELECT args.ARGUMENT_NAME as PARAMETER_NAME,");
                sb.AppendLine("args.DATA_LENGTH as CHARACTER_MAXIMUM_LENGTH,");
                sb.AppendLine("args.DATA_TYPE as TYPE_NAME,");
                sb.AppendLine("1 as IS_NULLABLE,"); 
                sb.AppendLine("args.DATA_PRECISION as NUMERIC_PRECISION,");
                sb.AppendLine("args.IN_OUT as PARAMETER_TYPE,");
                sb.AppendLine("0 as NUMERIC_SCALE");
                sb.AppendLine("FROM user_objects obj");
                sb.AppendLine("INNER JOIN user_arguments args");
                sb.AppendLine("ON obj.object_id = args.object_id");
                sb.AppendLine("WHERE obj.OBJECT_TYPE IN('PROCEDURE') and UPPER(obj.object_name) = upper(:procedure)");
                sb.AppendLine("ORDER BY args.POSITION ASC");

                OracleCommand getParamsCommand = new OracleCommand(sb.ToString(), connection);
                getParamsCommand.CommandType = CommandType.Text;
                if (transaction != null)
                    getParamsCommand.Transaction = transaction;

                OracleParameter sqlParameter = new OracleParameter() {
                    ParameterName = "procedure",
                    Value = spName
                };
                getParamsCommand.Parameters.Add(sqlParameter);

                OracleDataReader sdr = getParamsCommand.ExecuteReader();

                // Do we have any rows?
                if (sdr.HasRows)
                {
                    using (sdr)
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
                            if (!Enum.TryParse(datatype, true, out OracleType type))
                                if (datatype.ToUpper().Equals("NVARCHAR2"))
                                    type = OracleType.NVarChar;
                                else if (datatype.ToUpper().Equals("REF CURSOR"))
                                    type = OracleType.Cursor;
                                else
                                    throw new Exception($"Erreur with type {datatype} ..");
                                

                            int Nullable = sdr.GetInt32(ParamNullCol);
                            OracleParameter param = new OracleParameter(name, type);

                            // Determine parameter direction
                            string dir = sdr.GetString(ParamDirCol);
                            switch (dir)
                            {
                                case "IN":
                                    param.Direction = ParameterDirection.Input;
                                    break;
                                case "OUT":
                                    param.Direction = ParameterDirection.Output;
                                    break;
                                case "IN_OUT":
                                    param.Direction = ParameterDirection.InputOutput;
                                    break;
                            }

                            param.IsNullable = Nullable == 1;

                            if (!sdr.IsDBNull(ParamPrecCol))
                                param.Precision = (Byte)sdr.GetInt16(ParamPrecCol);
                            if (!sdr.IsDBNull(ParamSizeCol))
                                param.Size = sdr.GetInt32(ParamSizeCol);
                            if (!sdr.IsDBNull(ParamScaleCol))
                                param.Scale = (byte)sdr.GetInt32(ParamScaleCol);

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

            OracleParameter[] discoveredParameters = new OracleParameter[cmd.Parameters.Count];

            cmd.Parameters.CopyTo(discoveredParameters, 0);

            // Init the parameters with a DBNull value
            foreach (OracleParameter discoveredParameter in discoveredParameters)
                discoveredParameter.Value = DBNull.Value;

            return discoveredParameters;
        }

        internal static OracleParameter Clone(this OracleParameter param)
        {
            OracleParameter p = new OracleParameter();
           // p.DbType = param.DbType;
            p.Direction = param.Direction;
            p.IsNullable = param.IsNullable;
            p.ParameterName = param.ParameterName;
            p.Precision = param.Precision;
            p.Scale = param.Scale;
            p.Size = param.Size;
            p.SourceColumn = param.SourceColumn;
            p.OracleType = param.OracleType;
            p.Value = param.Value;
            return p;

        }
    }
}
