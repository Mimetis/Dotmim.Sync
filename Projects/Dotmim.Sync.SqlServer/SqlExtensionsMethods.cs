using Dotmim.Sync.Core;
using Dotmim.Sync.Core.Common;
using Dotmim.Sync.Data;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Text;
using System.Linq;

namespace Dotmim.Sync.SqlServer
{
    public static class SqlExtensionsMethods
    {
        static Dictionary<string, Type> DataTypeHashtable;
        static Dictionary<string, SqlDbType> SqlDbTypeHashtable;

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
                            //if (0 == String.Compare("xml", datatype, true))
                            //    datatype = "Text";

                            if (0 == String.Compare("table", datatype, true))
                                datatype = "Structured";

                            // TODO : Should we raise an error here ??
                            if (!Enum.TryParse(datatype, true, out SqlDbType type))
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

            //// WORKAROUND begin
            //foreach (SqlParameter sqlParam in discoveredParameters)
            //{
            //    if ((sqlParam.SqlDbType == SqlDbType.VarChar) &&
            //        (sqlParam.Size == Int32.MaxValue))
            //    {
            //        sqlParam.SqlDbType = SqlDbType.Text;
            //    }
            //}
            //// WORKAROUND end

            // Init the parameters with a DBNull value
            foreach (SqlParameter discoveredParameter in discoveredParameters)
            {
                discoveredParameter.Value = DBNull.Value;
            }
            return discoveredParameters;
        }

        internal static string GetSqlTypePrecisionString(this DmColumn column)
        {
            if (!String.IsNullOrEmpty(column.OrginalDbType))
            {
                SqlDbType? sqlDbType = column.OrginalDbType.ToSqlDbType();
                if (sqlDbType.HasValue)
                {
                    switch (sqlDbType)
                    {
                        case SqlDbType.Binary:
                        case SqlDbType.Char:
                        case SqlDbType.NChar:
                        case SqlDbType.NVarChar:
                        case SqlDbType.VarBinary:
                        case SqlDbType.VarChar:
                            if (column.MaxLength > 0)
                                return $"({column.MaxLength})";
                            else
                                return "(MAX)";
                        case SqlDbType.Decimal:
                            if (!column.PrecisionSpecified || !column.ScaleSpecified)
                                break;
                            return $"({ column.Precision}, {column.Scale})";
                        default:
                            return string.Empty;
                    }

                }
            }



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
            if (!String.IsNullOrEmpty(column.OrginalDbType))
            {
                SqlDbType? sqlDbType = column.OrginalDbType.ToSqlDbType();
                if (sqlDbType.HasValue)
                    return column.OrginalDbType;
            }

            string sqlType = string.Empty;
            switch (column.DbType)
            {
                case DbType.AnsiString:
                case DbType.AnsiStringFixedLength:
                    sqlType = "VarChar";
                    break;
                case DbType.Binary:
                    sqlType = "VarBinary";
                    break;
                case DbType.Boolean:
                    sqlType = "Bit";
                    break;
                case DbType.Byte:
                    sqlType = "TinyInt";
                    break;
                case DbType.Currency:
                    sqlType = "Money";
                    break;
                case DbType.Date:
                    sqlType = "Date";
                    break;
                case DbType.DateTime:
                    sqlType = "DateTime";
                    break;
                case DbType.DateTime2:
                    sqlType = "DateTime2";
                    break;
                case DbType.DateTimeOffset:
                    sqlType = "DateTimeOffset";
                    break;
                case DbType.Decimal:
                case DbType.Double:
                case DbType.Single:
                case DbType.VarNumeric:
                    sqlType = "Decimal";
                    break;
                case DbType.Guid:
                    sqlType = "UniqueIdentifier";
                    break;
                case DbType.Int16:
                    sqlType = "SmallInt";
                    break;
                case DbType.Int32:
                case DbType.UInt16:
                    sqlType = "Int";
                    break;
                case DbType.Int64:
                case DbType.UInt32:
                case DbType.UInt64:
                    sqlType = "BigInt";
                    break;
                case DbType.SByte:
                    sqlType = "SmallInt";
                    break;
                case DbType.String:
                case DbType.StringFixedLength:
                case DbType.Xml:
                    sqlType = "NVarChar";
                    break;
                case DbType.Time:
                    sqlType = "Time";
                    break;
            }

            if (string.IsNullOrEmpty(sqlType))
                throw new Exception($"sqltype not valid for the column {column.ColumnName}");

            return sqlType;
        }

        /// <summary>
        /// Get the original SqlDbType.
        /// If it's come from something else than Sql Server, try a simple conversion
        /// </summary>
        internal static SqlDbType GetSqlDbType(this DmColumn column)
        {
            // Try to set the real db type if it's present as string in the column.OriginalDbType property
            if (!String.IsNullOrEmpty(column.OrginalDbType))
            {
                SqlDbType? sqlDbType = column.OrginalDbType.ToSqlDbType();
                if (sqlDbType.HasValue)
                    return sqlDbType.Value;
            }

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
                case DbType.VarNumeric:
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

        /// <summary>
        /// Returns the corresponding SqlDbType. Because it could be lower case, we should handle it
        /// </summary>
        public static SqlDbType? ToSqlDbType(this string str)
        {
            // Handling lowercase with a dictionary
            if (SqlDbTypeHashtable == null)
            {
                SqlDbTypeHashtable = new Dictionary<string, SqlDbType>();
                var names = Enum.GetNames(typeof(SqlDbType)).ToList();
                names.ForEach(n => SqlDbTypeHashtable.Add(n.ToLowerInvariant(), (SqlDbType)Enum.Parse(typeof(SqlDbType), n)));

                // exception for numeric, sql_variant
                SqlDbTypeHashtable.Add("numeric", SqlDbType.Decimal);
                SqlDbTypeHashtable.Add("sql_variant", SqlDbType.Variant);
                SqlDbTypeHashtable.Remove("variant");

                //removing ntext, text, image since it won't be used in further sql version
                SqlDbTypeHashtable.Remove("text");
                SqlDbTypeHashtable.Remove("ntext");
                SqlDbTypeHashtable.Remove("image");

                // invalid for SqlMetadata
                //SqlDbTypeHashtable.Remove("binary");
            }

            var strLowerCase = str.ToLowerInvariant();

            if (SqlDbTypeHashtable.ContainsKey(strLowerCase))
                return SqlDbTypeHashtable[strLowerCase];

            return null;
        }

        public static Type ToManagedType(this SqlDbType sqlDbType)
        {
            switch (sqlDbType)
            {
                case SqlDbType.BigInt:
                    return Type.GetType("System.Int64");
                case SqlDbType.Binary:
                    return Type.GetType("System.Byte[]");
                case SqlDbType.Bit:
                    return Type.GetType("System.Boolean");
                case SqlDbType.Char:
                    return Type.GetType("System.String");
                case SqlDbType.Date:
                    return Type.GetType("System.DateTime");
                case SqlDbType.DateTime:
                    return Type.GetType("System.DateTime");
                case SqlDbType.DateTime2:
                    return Type.GetType("System.DateTime");
                case SqlDbType.DateTimeOffset:
                    return Type.GetType("System.DateTimeOffset");
                case SqlDbType.Decimal:
                    return Type.GetType("System.Decimal");
                case SqlDbType.Float:
                    return Type.GetType("System.Double");
                //case SqlDbType.Image:
                //    return Type.GetType("System.Byte[]");
                case SqlDbType.Int:
                    return Type.GetType("System.Int32");
                case SqlDbType.Money:
                    return Type.GetType("System.Decimal");
                case SqlDbType.NChar:
                    return Type.GetType("System.String");
                //case SqlDbType.NText:
                //    return Type.GetType("System.String");
                case SqlDbType.NVarChar:
                    return Type.GetType("System.String");
                case SqlDbType.Real:
                    return Type.GetType("System.Single");
                case SqlDbType.SmallDateTime:
                    return Type.GetType("System.DateTime");
                case SqlDbType.SmallInt:
                    return Type.GetType("System.Int16");
                case SqlDbType.SmallMoney:
                    return Type.GetType("System.Decimal");
                case SqlDbType.Structured:
                    return Type.GetType("System.Byte[]");
                //case SqlDbType.Text:
                //    return Type.GetType("System.String");
                case SqlDbType.Time:
                    return Type.GetType("System.TimeSpan");
                case SqlDbType.Timestamp:
                    return Type.GetType("System.Byte[]");
                case SqlDbType.TinyInt:
                    return Type.GetType("System.Byte");
                case SqlDbType.Udt:
                    return Type.GetType("System.Byte[]");
                case SqlDbType.UniqueIdentifier:
                    return Type.GetType("System.Guid");
                case SqlDbType.VarBinary:
                    return Type.GetType("System.Byte[]");
                case SqlDbType.VarChar:
                    return Type.GetType("System.String");
                case SqlDbType.Variant:
                    return Type.GetType("System.Object");
                case SqlDbType.Xml:
                    return Type.GetType("System.String");
                default:
                    return Type.GetType("System.String");
            }
        }

        /// <summary>
        /// TODO : Obsolete ?
        /// </summary>
        static void Init()
        {
            DataTypeHashtable = new Dictionary<string, Type>
            {
                { "bigint", Type.GetType("System.Int64") },
                { "binary", Type.GetType("System.Byte[]") },
                { "bit", Type.GetType("System.Boolean") },
                { "char", Type.GetType("System.String") },
                { "datetime", Type.GetType("System.DateTime") },
                { "decimal", Type.GetType("System.Decimal") },
                { "float", Type.GetType("System.Double") },
                { "image", Type.GetType("System.Byte[]") },
                { "int", Type.GetType("System.Int32") },
                { "money", Type.GetType("System.Decimal") },
                { "nchar", Type.GetType("System.String") },
                { "numeric", Type.GetType("System.Decimal") },
                { "ntext", Type.GetType("System.String") },
                { "nvarchar", Type.GetType("System.String") },
                { "real", Type.GetType("System.Single") },
                { "uniqueidentifier", Type.GetType("System.Guid") },
                { "smalldatetime", Type.GetType("System.DateTime") },
                { "smallint", Type.GetType("System.Int16") },
                { "smallmoney", Type.GetType("System.Decimal") },
                { "text", Type.GetType("System.String") },
                { "timestamp", Type.GetType("System.Byte[]") },
                { "tinyint", Type.GetType("System.Byte") },
                { "varbinary", Type.GetType("System.Byte[]") },
                { "varchar", Type.GetType("System.String") },
                { "variant", Type.GetType("System.Object") },
                { "xml", Type.GetType("System.String") },
                { "udt", Type.GetType("System.Byte[]") },
                { "structured", Type.GetType("System.Byte[]") },
                { "date", Type.GetType("System.DateTime") },
                { "time", Type.GetType("System.TimeSpan") },
                { "datetime2", Type.GetType("System.DateTime") },
                { "datetimeoffset", Type.GetType("System.DateTimeOffset") }
            };
        }

    }
}
