using Dotmim.Sync;
using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Text;
using System.Threading.Tasks;

namespace Migration
{
    public class Helper
    {
        public static async Task AddNewColumnToAddressAsync(DbConnection c)
        {
            using (var command = c.CreateCommand())
            {
                command.CommandText = "ALTER TABLE Address ADD CreatedDate datetime NULL;";
                c.Open();
                await command.ExecuteNonQueryAsync();
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
            command.Parameters.AddWithValue("@StateProvince", "");
            command.Parameters.AddWithValue("@CountryRegion", "");
            command.Parameters.AddWithValue("@PostalCode", "0001");
            command.Parameters.AddWithValue("@CreatedDate", DateTime.Now);

            await c.OpenAsync();
            var addressId = await command.ExecuteScalarAsync();
            c.Close();

            return Convert.ToInt32(addressId);
        }


        public static async Task<SyncRow> GetLastAddressRowAsync(DbConnection c, int addressId)
        {
            using var command = c.CreateCommand();
            command.CommandText = @"Select * From [Address] Where [AddressID]= @AddressId";

            var p = command.CreateParameter();
            p.ParameterName = "@AddressId";
            p.DbType= DbType.Int32;
            p.Value = addressId;
            command.Parameters.Add(p);

            var syncTable = new SyncTable("Address");

            await c.OpenAsync();
            using (var reader = await command.ExecuteReaderAsync().ConfigureAwait(false))
                syncTable.Load(reader);
            c.Close();

            return syncTable.Rows[0];
        }
    }
}
