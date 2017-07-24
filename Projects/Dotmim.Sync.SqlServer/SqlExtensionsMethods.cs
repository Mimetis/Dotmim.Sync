using Dotmim.Sync.Core;
using Dotmim.Sync.Core.Common;
using Dotmim.Sync.Data;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Text;

namespace Dotmim.Sync.SqlServer
{
    public static class SqlExtensionsMethods
    {

        internal static SqlParameter[] DeriveParameters(this SqlConnection connection, SqlCommand cmd, bool includeReturnValueParameter = false, SqlTransaction transaction = null)
        {
            if (cmd == null) throw new ArgumentNullException("SqlCommand");

            var textParser = new ObjectNameParser(cmd.CommandText);

            // Hack to check for schema name in the spName
            string schemaName = "dbo";
            string spName = textParser.UnquotedString;
            int firstDot = spName.IndexOf('.');
            if (firstDot > 0)
            {
                schemaName = cmd.CommandText.Substring(0, firstDot);
                spName = spName.Substring(firstDot + 1);
            }

            var alreadyOpened = connection.State == ConnectionState.Open;

            if (!alreadyOpened)
                connection.Open();

            try
            {
                SqlCommand getParamsCommand = new SqlCommand("sp_procedure_params_rowset", connection);
                getParamsCommand.CommandType = CommandType.StoredProcedure;
                if (transaction != null)
                    getParamsCommand.Transaction = transaction;

                SqlParameter p = new SqlParameter("@procedure_name", SqlDbType.NVarChar);
                p.Value = spName;
                getParamsCommand.Parameters.Add(p);
                p = new SqlParameter("@procedure_schema", SqlDbType.NVarChar);
                p.Value = schemaName;
                getParamsCommand.Parameters.Add(p);
                SqlDataReader sdr = getParamsCommand.ExecuteReader();

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
                            if (0 == String.Compare("xml", datatype, true))
                                datatype = "Text";

                            if (0 == String.Compare("table", datatype, true))
                                datatype = "Structured";

                            SqlDbType type = SqlDbType.Int;

                            if (!Enum.TryParse<SqlDbType>(datatype, true, out type))
                                type = SqlDbType.Variant;

                            bool Nullable = sdr.GetBoolean(ParamNullCol);
                            SqlParameter param = new SqlParameter(name, type);
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

            // WORKAROUND begin
            foreach (SqlParameter sqlParam in discoveredParameters)
            {
                if ((sqlParam.SqlDbType == SqlDbType.VarChar) &&
                    (sqlParam.Size == Int32.MaxValue))
                {
                    sqlParam.SqlDbType = SqlDbType.Text;
                }
            }
            // WORKAROUND end

            // Init the parameters with a DBNull value
            foreach (SqlParameter discoveredParameter in discoveredParameters)
            {
                discoveredParameter.Value = DBNull.Value;
            }
            return discoveredParameters;
        }

        internal static string GetSqlTypePrecisionString(this DmColumn column)
        {
            string sizeString = string.Empty;
            switch (column.DbType)
            {
                case DbType.AnsiString:
                case DbType.AnsiStringFixedLength:
                case DbType.Binary:
                case DbType.String:
                case DbType.StringFixedLength:
                    if (column.MaxLength > 0)
                        sizeString = $"({column.MaxLength})";
                    else
                        sizeString = $"(MAX)";
                    break;
                case DbType.Decimal:
                case DbType.Double:
                case DbType.Single:
                case DbType.VarNumeric:
                    if (!column.PrecisionSpecified || !column.ScaleSpecified)
                        break;

                    sizeString = $"({ column.Precision}, {column.Scale})";
                    break;
            }

            return sizeString;
        }

       internal static (byte length, byte scale) GetSqlTypePrecision(this DmColumn column)
        {
            string sizeString = string.Empty;
            switch (column.DbType)
            {
                case DbType.Decimal:
                case DbType.Double:
                case DbType.Single:
                case DbType.VarNumeric:
                    if (!column.PrecisionSpecified || !column.ScaleSpecified)
                        break;

                    return (column.Precision, column.Scale);
            }

            return (0, 0);
        }
        internal static string GetSqlDbTypeString(this DmColumn column)
        {

            string sqlType = string.Empty;
            switch (column.DbType)
            {
                case DbType.AnsiString:
                case DbType.AnsiStringFixedLength:
                    sqlType = "varchar";
                    break;
                case DbType.Binary:
                    sqlType = "varbinary";
                    break;
                case DbType.Boolean:
                    sqlType = "bit";
                    break;
                case DbType.Byte:
                    sqlType = "tinyint";
                    break;
                case DbType.Currency:
                    sqlType = "money";
                    break;
                case DbType.Date:
                    sqlType = "date";
                    break;
                case DbType.DateTime:
                    sqlType = "datetime";
                    break;
                case DbType.DateTime2:
                    sqlType = "datetime2";
                    break;
                case DbType.DateTimeOffset:
                    sqlType = "datetimeoffset";
                    break;
                case DbType.Decimal:
                case DbType.Double:
                case DbType.Single:
                    sqlType = "decimal";
                    break;
                case DbType.Guid:
                    sqlType = "uniqueidentifier";
                    break;
                case DbType.Int16:
                    sqlType = "smallint";
                    break;
                case DbType.Int32:
                case DbType.UInt16:
                    sqlType = "int";
                    break;
                case DbType.Int64:
                case DbType.UInt32:
                case DbType.UInt64:
                    sqlType = "bigint";
                    break;
                case DbType.SByte:
                    sqlType = "smallint";
                    break;
                case DbType.String:
                case DbType.StringFixedLength:
                case DbType.Xml:
                    sqlType = "nvarchar";
                    break;
                case DbType.Time:
                    sqlType = "time";
                    break;
                case DbType.VarNumeric:
                    sqlType = "numeric";
                    break;
            }

            if (string.IsNullOrEmpty(sqlType))
                throw new Exception($"sqltype not valid for the column {column.ColumnName}");

            return sqlType;
        }

        internal static SqlDbType GetSqlDbType(this DmColumn column)
        {

            SqlDbType sqlType = SqlDbType.Variant;
            switch (column.DbType)
            {
                case DbType.AnsiString:
                case DbType.AnsiStringFixedLength:
                    sqlType = SqlDbType.VarChar;
                    break;
                case DbType.Binary:
                    sqlType = SqlDbType.VarBinary;
                    break;
                case DbType.Boolean:
                    sqlType = SqlDbType.Bit;
                    break;
                case DbType.Byte:
                    sqlType = SqlDbType.TinyInt;
                    break;
                case DbType.Currency:
                    sqlType = SqlDbType.Money;
                    break;
                case DbType.Date:
                    sqlType = SqlDbType.Date;
                    break;
                case DbType.DateTime:
                    sqlType = SqlDbType.DateTime;
                    break;
                case DbType.DateTime2:
                    sqlType = SqlDbType.DateTime2;
                    break;
                case DbType.DateTimeOffset:
                    sqlType = SqlDbType.DateTimeOffset;
                    break;
                case DbType.Decimal:
                case DbType.Double:
                case DbType.Single:
                    sqlType = SqlDbType.Decimal;
                    break;
                case DbType.Guid:
                    sqlType = SqlDbType.UniqueIdentifier;
                    break;
                case DbType.Int16:
                    sqlType = SqlDbType.SmallInt;
                    break;
                case DbType.Int32:
                case DbType.UInt16:
                    sqlType = SqlDbType.Int;
                    break;
                case DbType.Int64:
                case DbType.UInt32:
                case DbType.UInt64:
                    sqlType = SqlDbType.BigInt;
                    break;
                case DbType.SByte:
                    sqlType = SqlDbType.SmallInt;
                    break;
                case DbType.String:
                case DbType.StringFixedLength:
                case DbType.Xml:
                    sqlType = SqlDbType.NVarChar;
                    break;
                case DbType.Time:
                    sqlType = SqlDbType.Time;
                    break;
                case DbType.VarNumeric:
                    sqlType = SqlDbType.Decimal;
                    break;
            }

            return sqlType;
        }


        internal static SqlParameter Clone(this SqlParameter param)
        {
            SqlParameter p = new SqlParameter();
            p.DbType = param.DbType;
            p.Direction = param.Direction;
            p.IsNullable = param.IsNullable;
            p.ParameterName = param.ParameterName;
            p.Precision = param.Precision;
            p.Scale = param.Scale;
            p.Size = param.Size;
            p.SourceColumn = param.SourceColumn;
            p.SqlDbType = param.SqlDbType;
            p.SqlValue = param.SqlValue;
            p.TypeName = param.TypeName;
            p.Value = param.Value;

            return p;

        }

        internal static SqlParameter GetSqlParameter(this DmColumn column)
        {
            SqlParameter sqlParameter = new SqlParameter();
            sqlParameter.ParameterName = $"@{column.ColumnName}";
            sqlParameter.SqlDbType = GetSqlDbType(column);
            sqlParameter.IsNullable = column.AllowDBNull;

            (byte precision, byte scale) = GetSqlTypePrecision(column);

            if (sqlParameter.SqlDbType == SqlDbType.Decimal && precision > 0)
            {
                sqlParameter.Precision = precision;

                if (scale > 0)
                    sqlParameter.Scale = scale;
            }
            else if (column.MaxLength > 0)
            {
                sqlParameter.Size = column.MaxLength;
            }
            else
            {
                sqlParameter.Size = -1;
            }

            return sqlParameter;
        }



    }
}
