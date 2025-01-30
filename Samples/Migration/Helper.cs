using Dotmim.Sync;
using Microsoft.Data.SqlClient;
using System;
using System.Data;
using System.Data.Common;
using System.Threading.Tasks;

namespace Migration
{
    public static class Helper
    {
        public static async Task AddNewColumnToAddressAsync(DbConnection c)
        {
            using (var command = c.CreateCommand())
            {
                command.CommandText = "ALTER TABLE Address ADD CreatedDate datetime NULL;";
                c.Open();
                await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                c.Close();
            }
        }

        public static async Task<int> InsertOneAddressWithNewColumnAsync(SqlConnection c)
        {
            using var command = c.CreateCommand();
            command.CommandText = @"INSERT INTO [Address] 
                                    ([AddressLine1] ,[City],[StateProvince],[CountryRegion],[PostalCode], [CreatedDate])
                                    VALUES 
                                    (@AddressLine1 ,@City, @StateProvince, @CountryRegion, @PostalCode, @CreatedDate);
                                    Select SCOPE_IDENTITY() as AddressID";

            command.Parameters.AddWithValue("@AddressLine1", "1 barber avenue");
            command.Parameters.AddWithValue("@City", "Munitan");
            command.Parameters.AddWithValue("@StateProvince", string.Empty);
            command.Parameters.AddWithValue("@CountryRegion", string.Empty);
            command.Parameters.AddWithValue("@PostalCode", "0001");
            command.Parameters.AddWithValue("@CreatedDate", DateTime.Now);

            await c.OpenAsync().ConfigureAwait(false);
            var addressId = await command.ExecuteScalarAsync().ConfigureAwait(false);
            c.Close();

            return Convert.ToInt32(addressId);
        }

        public static async Task<SyncRow> GetLastAddressRowAsync(DbConnection c, int addressId)
        {
            using var command = c.CreateCommand();
            command.CommandText = @"Select * From [Address] Where [AddressID]= @AddressId";

            var p = command.CreateParameter();
            p.ParameterName = "@AddressId";
            p.DbType = DbType.Int32;
            p.Value = addressId;
            command.Parameters.Add(p);

            var syncTable = new SyncTable("Address");

            await c.OpenAsync().ConfigureAwait(false);
            using (var reader = await command.ExecuteReaderAsync().ConfigureAwait(false))
                syncTable.Load(reader);
            c.Close();

            return syncTable.Rows[0];
        }
    }
}