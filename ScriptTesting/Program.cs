using Dotmim.Sync.PostgreSql;
using NpgsqlTypes;
using System;

namespace ScriptTesting
{
    internal class Program
    {
        static void Main(string[] args)
        {
            NpgsqlDbType npgsqlDbType = GetNpgsqlDbTypeForArray("integer");


            Console.WriteLine(npgsqlDbType);
            Console.ReadLine();
        }
        public static NpgsqlDbType GetNpgsqlDbTypeForArray(string postgresqlDataType)
        {
            // Normalize the input data type to lower case
            postgresqlDataType = postgresqlDataType.ToLowerInvariant();

            switch (postgresqlDataType)
            {
                case "boolean":
                    return NpgsqlDbType.Array | NpgsqlDbType.Boolean;
                case "smallint":
                    return NpgsqlDbType.Array | NpgsqlDbType.Smallint;
                case "integer":
                    return NpgsqlDbType.Array | NpgsqlDbType.Integer;
                case "bigint":
                    return NpgsqlDbType.Array | NpgsqlDbType.Bigint;
                case "numeric":
                    return NpgsqlDbType.Array | NpgsqlDbType.Numeric;
                case "real":
                    return NpgsqlDbType.Array | NpgsqlDbType.Real;
                case "double precision":
                    return NpgsqlDbType.Array | NpgsqlDbType.Double;
                case "char":
                    return NpgsqlDbType.Array | NpgsqlDbType.Char;
                case "varchar":
                    return NpgsqlDbType.Array | NpgsqlDbType.Varchar;
                case "text":
                    return NpgsqlDbType.Array | NpgsqlDbType.Text;
                case "timestamp":
                    return NpgsqlDbType.Array | NpgsqlDbType.Timestamp;
                case "date":
                    return NpgsqlDbType.Array | NpgsqlDbType.Date;
                case "time":
                    return NpgsqlDbType.Array | NpgsqlDbType.Time;
                case "interval":
                    return NpgsqlDbType.Array | NpgsqlDbType.Interval;
                case "bytea":
                    return NpgsqlDbType.Array | NpgsqlDbType.Bytea;
                default:
                    throw new ArgumentException($"Unrecognized PostgreSQL data type: {postgresqlDataType}");
            }
        }

    }
}
