using Dotmim.Sync.Data;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace Dotmim.Sync.Manager
{
    public class DbConverterSqlServer : DbConverter
    {
        public override bool IsValid(string type)
        {
            switch (type)
            {
                case "uniqueidentifier":
                case "binary":
                case "sql_variant":
                case "image":
                case "varbinary":
                case "date":
                case "datetime":
                case "datetime2":
                case "smalldatetime":
                case "numeric":
                case "float":
                case "decimal":
                case "real":
                case "smallmoney":
                case "money":
                case "int":
                case "smallint":
                case "bigint":
                case "tinyint":
                case "bit":
                case "timestamp":
                case "datetimeoffset":
                case "time":
                case "char":
                case "nchar":
                case "text":
                case "ntext":
                case "varchar":
                case "nvarchar":
                case "xml":
                    return true;
            }
            return false;
        }



        /// <summary>
        /// Convert a MySql column to Sql Server
        /// Using https://docs.microsoft.com/en-us/sql/ssma/mysql/project-settings-type-mapping-mysqltosql?view=sql-server-2017 
        /// </summary>
        private static DmColumn GetSqlColumnFromMySql(DmColumn mySqlColumn)
        {
            // get the mysql type (int, text, longtext, decimal and so on ..)
            var oType = mySqlColumn.OriginalTypeName.ToLowerInvariant();

            if (oType == "bigint")
                return MySql_BigInt_To_SqlServer(mySqlColumn);

            if (oType == "binary")
                return MySql_Binary_To_SqlServer(mySqlColumn);

            if (oType == "bit")
                return MySql_Bit_To_SqlServer(mySqlColumn);

            if (oType == "blob" || oType == "mediumblob" || oType == "longblob")
                return MySql_Blob_To_SqlServer(mySqlColumn);

            if (oType == "varbinary" )
                return MySql_VarBinary_To_SqlServer(mySqlColumn);

            if (oType == "tinyblob")
                return MySql_TinyBlob_To_SqlServer(mySqlColumn);

            // be carefule, nchar is for national char !! (not unicode)
            if (oType == "char" || oType == "nchar")
                return MySql_Char_To_SqlServer(mySqlColumn);

            if (oType == "date")
                return MySql_Date_To_SqlServer(mySqlColumn);

            if (oType == "datetime")
                return MySql_Datetime_To_SqlServer(mySqlColumn);

            if (oType == "decimal")
                return MySql_Decimal_To_SqlServer(mySqlColumn);

            if (oType == "double" || oType == "float")
                return MySql_Double_To_SqlServer(mySqlColumn);

            if (oType == "int" || oType == "integer" || oType == "mediumint")
                return MySql_Int_To_SqlServer(mySqlColumn);

            if (oType == "longtext" || oType == "mediumtext")
                return MySql_Text_To_SqlServer(mySqlColumn);

            // be careful nvarchar is for national varchar (non unicode; like sql server)
            if (oType == "varchar" || oType == "nvarchar" || oType == "text")
                return MySql_Varchar_To_SqlServer(mySqlColumn);

            if (oType == "tinytext")
                return MySql_TinyText_To_SqlServer(mySqlColumn);

            if (oType == "timestamp")
                return MySql_TimeStamp_To_SqlServer(mySqlColumn);

            if (oType == "tinyint")
                return MySql_Tinyint_To_SqlServer(mySqlColumn);

            if (oType == "year")
                return MySql_Year_To_SqlServer(mySqlColumn);

            // default ?
            throw new NotSupportedException($"this type {oType} is not supported to convert from MySql to Sql Server...");

        }


        private static DmColumn MySql_Year_To_SqlServer(DmColumn mySqlColumn)
        {
            var sqlColumn = mySqlColumn.Clone();

            sqlColumn.MaxLength = -1;
            sqlColumn.Precision = 0;
            sqlColumn.Scale = 0;
            sqlColumn.DbType = DbType.Int16;
            sqlColumn.OriginalDbType = "SqlDbType.SmallInt";
            sqlColumn.OriginalTypeName = "smallint";
            return sqlColumn;
        }

        /// <summary>
        /// Converts to smallint
        /// </summary>
        private static DmColumn MySql_Tinyint_To_SqlServer(DmColumn mySqlColumn)
        {
            var sqlColumn = mySqlColumn.Clone();

            sqlColumn.MaxLength = -1;
            sqlColumn.Precision = 0;
            sqlColumn.Scale = 0;
            sqlColumn.DbType = DbType.Int16;
            sqlColumn.OriginalDbType = "SqlDbType.SmallInt";
            sqlColumn.OriginalTypeName = "smallint";
            return sqlColumn;
        }

        private static DmColumn MySql_TimeStamp_To_SqlServer(DmColumn mySqlColumn)
        {
            var sqlColumn = mySqlColumn.Clone();

            sqlColumn.MaxLength = 8;
            sqlColumn.Precision = 0;
            sqlColumn.Scale = 0;
            sqlColumn.DbType = DbType.UInt64;
            sqlColumn.OriginalDbType = "SqlDbType.BigInt";
            sqlColumn.OriginalTypeName = "bigint";
            return sqlColumn;
        }

        private static DmColumn MySql_TinyText_To_SqlServer(DmColumn mySqlColumn)
        {
            var sqlColumn = mySqlColumn.Clone();


            // if length > 8000, will be a NVachar(max)
            sqlColumn.MaxLength = -1;
            sqlColumn.Precision = 0;
            sqlColumn.Scale = 0;
            sqlColumn.DbType = DbType.String;
            sqlColumn.OriginalDbType = "SqlDbType.NVarchar";
            sqlColumn.OriginalTypeName = "nvarchar";
            return sqlColumn;
        }

        /// <summary>
        /// Convert to NVarchar (max if len > 4000)
        /// </summary>
        private static DmColumn MySql_Varchar_To_SqlServer(DmColumn mySqlColumn)
        {
            var sqlColumn = mySqlColumn.Clone();


            var maxlen = mySqlColumn.MaxLength;

            // check length.
            // Exception on 0 and go to NVachar(max) if > 4000
            if (maxlen == 0)
                maxlen = 1;
            else if (maxlen > 4000)
                maxlen = -1;

            sqlColumn.MaxLength = maxlen;
            sqlColumn.Precision = 0;
            sqlColumn.Scale = 0;
            sqlColumn.DbType = DbType.String;
            sqlColumn.OriginalDbType = "SqlDbType.NVarchar";
            sqlColumn.OriginalTypeName = "nvarchar";
            return sqlColumn;
        }

        private static DmColumn MySql_Text_To_SqlServer(DmColumn mySqlColumn)
        {
            var sqlColumn = mySqlColumn.Clone();

            sqlColumn.MaxLength = -1;
            sqlColumn.Precision = 0;
            sqlColumn.Scale = 0;
            sqlColumn.DbType = DbType.String;
            sqlColumn.OriginalDbType = "SqlDbType.NVarchar";
            sqlColumn.OriginalTypeName = "nvarchar";
            return sqlColumn;
        }

        /// <summary>
        /// Converts to int if [signed] else to bigint if [unsigned] && type is "int"
        /// </summary>
        private static DmColumn MySql_Int_To_SqlServer(DmColumn mySqlColumn)
        {
            var sqlColumn = mySqlColumn.Clone();

            sqlColumn.Scale = 0;
            sqlColumn.Precision = 10;

            // if it's an int (not mediumint) check if unsigned
            if (mySqlColumn.IsUnsigned && mySqlColumn.OriginalTypeName.ToLowerInvariant() != "mediumint")
            {
                sqlColumn.MaxLength = 8;
                sqlColumn.DbType = DbType.Int64;
                sqlColumn.OriginalDbType = "SqlDbType.BigInt";
                sqlColumn.OriginalTypeName = "bigint";
            }
            else
            {
                sqlColumn.MaxLength = 4;
                sqlColumn.DbType = DbType.Int32;
                sqlColumn.OriginalDbType = "SqlDbType.Int";
                sqlColumn.OriginalTypeName = "int";

            }

            return sqlColumn;
        }

        /// <summary>
        /// if scale is sup to 0 then return numeric otherwise, return a float
        /// </summary>
        private static DmColumn MySql_Double_To_SqlServer(DmColumn mySqlColumn)
        {

            var sqlColumn = mySqlColumn.Clone();

            sqlColumn.MaxLength = -1;

            if (mySqlColumn.Scale > 0)
            {
                sqlColumn.DbType = DbType.VarNumeric;
                sqlColumn.OriginalDbType = "SqlDbType.Numeric";
                sqlColumn.OriginalTypeName = "numeric";
            }
            else
            {
                sqlColumn.DbType = DbType.Double;
                // if we convert from double, the float will be 53 or if we convert from float it will be 24
                sqlColumn.Precision = mySqlColumn.OriginalTypeName.ToLowerInvariant() == "double" ? (byte)53 : (byte)24;
                sqlColumn.Scale = 0;
                sqlColumn.OriginalDbType = "SqlDbType.Float";
                sqlColumn.OriginalTypeName = "float";
            }
            return sqlColumn;
        }

        private static DmColumn MySql_Decimal_To_SqlServer(DmColumn mySqlColumn)
        {
            var sqlColumn = mySqlColumn.Clone();

            sqlColumn.MaxLength = -1;
            sqlColumn.Precision = Math.Min((byte)65, mySqlColumn.Precision);
            sqlColumn.Scale = Math.Min((byte)30, mySqlColumn.Scale);
            sqlColumn.DbType = DbType.Decimal;
            sqlColumn.OriginalDbType = "SqlDbType.Decimal";
            sqlColumn.OriginalTypeName = "decimal";
            return sqlColumn;
        }

        /// <summary>
        /// Convert datetime to sql datetime2 (https://docs.microsoft.com/en-us/sql/t-sql/data-types/datetime-transact-sql?view=sql-server-2017)
        /// </summary>
        private static DmColumn MySql_Datetime_To_SqlServer(DmColumn mySqlColumn)
        {
            var sqlColumn = mySqlColumn.Clone();

            sqlColumn.MaxLength = 8;
            sqlColumn.Precision = 27;
            sqlColumn.Scale = 7;
            sqlColumn.DbType = DbType.DateTime2;
            sqlColumn.OriginalDbType = "SqlDbType.DateTime2";
            sqlColumn.OriginalTypeName = "datetime2";
            return sqlColumn;
        }

        private static DmColumn MySql_Date_To_SqlServer(DmColumn mySqlColumn)
        {
            var sqlColumn = mySqlColumn.Clone();

            sqlColumn.MaxLength = 1;
            sqlColumn.DbType = DbType.Date;
            sqlColumn.OriginalDbType = "SqlDbType.Date";
            sqlColumn.OriginalTypeName = "date";
            return sqlColumn;

        }

        /// <summary>
        /// Converts to unicode Sql server char (nchar)
        /// </summary>
        private static DmColumn MySql_Char_To_SqlServer(DmColumn mySqlColumn)
        {
            var sqlColumn = mySqlColumn.Clone();


            // a char(0) is possible in mysql
            if (mySqlColumn.MaxLength == 0)
                sqlColumn.MaxLength = 1;
            else
                sqlColumn.MaxLength = mySqlColumn.MaxLength >= 1 ? mySqlColumn.MaxLength : -1;

            // set if it's a fixed size string
            sqlColumn.DbType = sqlColumn.MaxLength > 0 ? DbType.StringFixedLength : DbType.String;

            sqlColumn.OriginalDbType = "SqlDbType.NChar";
            sqlColumn.OriginalTypeName = "nchar";
            return sqlColumn;
        }

        /// <summary>
        /// Converts to Varbinary(255)
        /// </summary>
        private static DmColumn MySql_TinyBlob_To_SqlServer(DmColumn mySqlColumn)
        {
            var sqlColumn = mySqlColumn.Clone();

            sqlColumn.DbType = DbType.Binary;
            sqlColumn.maxLength = 255;
            sqlColumn.OriginalDbType = "SqlDbType.VarBinary";
            sqlColumn.OriginalTypeName = "varbinary";
            return sqlColumn;
        }

        /// <summary>
        /// Converts to Varbinary(Max)
        /// </summary>
        private static DmColumn MySql_Blob_To_SqlServer(DmColumn mySqlColumn)
        {
            var sqlColumn = mySqlColumn.Clone();

            sqlColumn.DbType = DbType.Binary;
            sqlColumn.maxLength = -1;
            sqlColumn.OriginalDbType = "SqlDbType.VarBinary";
            sqlColumn.OriginalTypeName = "varbinary";
            return sqlColumn;
        }

        /// <summary>
        /// Converts to Varbinary(N)
        /// </summary>
        private static DmColumn MySql_VarBinary_To_SqlServer(DmColumn mySqlColumn)
        {
            var maxlen = mySqlColumn.MaxLength;

            // check length.
            // Exception on 0 and go to Varbinary(max) if > 8000
            if (maxlen == 0)
                maxlen = 1;
            else if (maxlen > 8000)
                maxlen = -1;

            var sqlColumn = mySqlColumn.Clone();

            sqlColumn.DbType = DbType.Binary;
            sqlColumn.MaxLength = maxlen;
            sqlColumn.OriginalDbType = "SqlDbType.VarBinary";
            sqlColumn.OriginalTypeName = "varbinary";
            return sqlColumn;
        }

        /// <summary>
        /// Binary is pretty the same. Just be careful a minimum Maxlength (1) is set
        /// </summary>
        private static DmColumn MySql_Binary_To_SqlServer(DmColumn mySqlColumn)
        {
            var sqlColumn = mySqlColumn.Clone();

            sqlColumn.DbType = DbType.Binary;
            sqlColumn.maxLength = Math.Min(mySqlColumn.MaxLength, 1);
            sqlColumn.OriginalDbType = "SqlDbType.Binary";
            sqlColumn.OriginalTypeName = "binary";
            return sqlColumn;

        }

        /// <summary>
        /// Bigint is the same. Just the max length (octet length) from Sql Server could be useful
        /// Even if the MySql Bigint is unsigned, we get bigint
        /// </summary>
        private static DmColumn MySql_BigInt_To_SqlServer(DmColumn mySqlColumn)
        {
            var sqlColumn = mySqlColumn.Clone();

            sqlColumn.MaxLength = 8;
            sqlColumn.DbType = DbType.Int64;
            sqlColumn.OriginalDbType = "SqlDbType.BigInt";
            sqlColumn.OriginalTypeName = "bigint";
            sqlColumn.IsUnsigned = false;
            return sqlColumn;

        }

        /// <summary>
        /// bit type in my sql could have a precision, specifying how much bits I can store
        ///  for example : bit(4) can store a value from (mysql syntax) b'0000' to b'1111' (so 0 to 15)
        ///  so if bit precision inf or eq to 1 so we have a sql server bit, otherwise, we have a binary data
        /// </summary>
        private static DmColumn MySql_Bit_To_SqlServer(DmColumn mySqlColumn)
        {
            var sqlColumn = mySqlColumn.Clone();


            var precision = mySqlColumn.Precision;

            if (precision <= 1)
            {
                sqlColumn.DataType = typeof(Boolean);
                sqlColumn.Precision = 1;
                sqlColumn.MaxLength = 1;
                sqlColumn.DbType = DbType.Boolean;
                sqlColumn.OriginalDbType = "SqlDbType.Bit";
                sqlColumn.OriginalTypeName = "bit";
                return sqlColumn;
            }

            // getting the ceiling value to get the binary length we need
            var binaryLength = Convert.ToInt32(Math.Ceiling(precision / 8d));

            sqlColumn.DataType = typeof(Byte[]);
            sqlColumn.Precision = 0;
            sqlColumn.MaxLength = binaryLength;
            sqlColumn.DbType = DbType.Binary;
            sqlColumn.OriginalDbType = "SqlDbType.Binary";
            sqlColumn.OriginalTypeName = "binary";
            return sqlColumn;
        }

        public override bool CanConvertFrom(DbConverterType type)
        {
            switch (type)
            {
                case DbConverterType.MySql:
                case DbConverterType.SqlServer:
                    return true;
                case DbConverterType.Sqlite:
                default:
                    return false;
            }
        }

        public override DmColumn ConvertFrom(DbConverterType type, DmColumn dmColumn)
        {
            switch (type)
            {
                case DbConverterType.SqlServer:
                    return dmColumn;
                case DbConverterType.MySql:
                    return GetSqlColumnFromMySql(dmColumn);
                case DbConverterType.Sqlite:
                    throw new NotImplementedException("Can't get a Sql Server column from a Sqlite column");
            }

            return null;
        }

     
    }
}
