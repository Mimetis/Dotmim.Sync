using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;

namespace IdentityColumnsServer
{
    public class SeedingServices : ISeedingServices
    {

        private const int MAX_CLIENTS_COUNT = 50;
        private const int FIRST_CLIENT_SEED = 10;

        public async Task<List<Seeding>> GetSeedingsAsync(Guid scopeId, DbConnection connection)
        {
            var lstSeeding = new List<Seeding>();

            using (var command = connection.CreateCommand())
            {
                command.Connection = connection;
                command.CommandText = "Select * from scope_info_seeding where ClientScopeId = @scopeId";
                var p = command.CreateParameter();
                p.ParameterName = "@scopeId";
                p.Value = scopeId;
                command.Parameters.Add(p);

                connection.Open();
                using var dr = await command.ExecuteReaderAsync();
                while (dr.Read())
                {
                    Seeding seeding = new Seeding();
                    seeding.ClientScopeId = (Guid)dr["ClientScopeId"];
                    seeding.SchemaName = dr["SchemaName"] as string;
                    seeding.TableName = dr["TableName"] as string;
                    seeding.Step = (int)dr["Step"];
                    seeding.Seed = (int)dr["Seed"];

                    lstSeeding.Add(seeding);
                }
                dr.Close();
                connection.Close();
            }


            var customerSeeding = lstSeeding.FirstOrDefault(s => s.TableName == "Customer" && s.SchemaName == "dbo");

            if (customerSeeding == null)
            {
                customerSeeding = await EnsureSeedingAsync(scopeId, "Customer", "dbo", connection);
                lstSeeding.Add(customerSeeding);
            }

            var customerAddressSeeding = lstSeeding.FirstOrDefault(s => s.TableName == "CustomerAddress" && s.SchemaName == "dbo");

            if (customerAddressSeeding == null)
            {
                customerAddressSeeding = await EnsureSeedingAsync(scopeId, "CustomerAddress", "dbo", connection);
                lstSeeding.Add(customerAddressSeeding);
            }


            var addressSeeding = lstSeeding.FirstOrDefault(s => s.TableName == "Address" && s.SchemaName == "dbo");

            if (addressSeeding == null)
            {
                addressSeeding = await EnsureSeedingAsync(scopeId, "Address", "dbo", connection);
                lstSeeding.Add(addressSeeding);
            }


            return lstSeeding;


        }


        private async Task<Seeding> EnsureSeedingAsync(Guid scopeId, string tableName, string schemaName, DbConnection connection)
        {
            int seed;

            // Customer
            var seeding = new Seeding
            {
                ClientScopeId = scopeId,
                SchemaName = schemaName,
                TableName = tableName,
                Step = MAX_CLIENTS_COUNT
            };

            using (var command = connection.CreateCommand())
            {
                command.Connection = connection;
                command.CommandText = "Select Max(Seed) from scope_info_seeding where TableName=@TableName and SchemaName=@SchemaName";
                var p = command.CreateParameter();
                p.ParameterName = "@TableName";
                p.Value = tableName;
                command.Parameters.Add(p);

                p = command.CreateParameter();
                p.ParameterName = "@SchemaName";
                p.Value = schemaName;
                command.Parameters.Add(p);

                connection.Open();
                var seedResponse = await command.ExecuteScalarAsync();
                seed = seedResponse == DBNull.Value ? 0 : Convert.ToInt32(seedResponse);
                connection.Close();
            }

            // first client for this table will start its seeding to 10
            // otherwise increment seed
            seeding.Seed = seed <= 0 ? FIRST_CLIENT_SEED : seed++;

            await SaveSeedingAsync(seeding, connection);

            return seeding;
        }


        private async Task SaveSeedingAsync(Seeding seeding, DbConnection connection)
        {
            using var command = connection.CreateCommand();

            command.Connection = connection;
            command.CommandText = "Insert Into scope_info_seeding(ClientScopeId, TableName, SchemaName, Seed, Step) Values(@ClientScopeId, @TableName, @SchemaName, @Seed, @Step)";

            var p = command.CreateParameter();
            p.ParameterName = "@ClientScopeId";
            p.DbType = DbType.Guid;
            p.Value = seeding.ClientScopeId;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@TableName";
            p.DbType = DbType.String;
            p.Value = seeding.TableName;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@SchemaName";
            p.DbType = DbType.String;
            p.Value = seeding.SchemaName;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@Seed";
            p.DbType = DbType.Int32;
            p.Value = seeding.Seed;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@Step";
            p.DbType = DbType.Int32;
            p.Value = seeding.Step;
            command.Parameters.Add(p);

            connection.Open();
            await command.ExecuteNonQueryAsync().ConfigureAwait(false);
            connection.Close();

        }

    }
}
