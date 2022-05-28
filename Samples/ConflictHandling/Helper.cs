using Dotmim.Sync;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Text;
using System.Threading.Tasks;

namespace Conflict
{
    public class Helper
    {

        public static async Task<int> InsertOneProductCategoryAsync(DbConnection c, string updatedName)
        {
            using (var command = c.CreateCommand())
            {
                command.CommandText = "Insert Into ProductCategory (Name) Values (@Name); SELECT SCOPE_IDENTITY();";
                var p = command.CreateParameter();
                p.DbType = DbType.String;
                p.Value = updatedName;
                p.ParameterName = "@Name";
                command.Parameters.Add(p);

                await c.OpenAsync();
                var id = await command.ExecuteScalarAsync();
                c.Close();

                return Convert.ToInt32(id);
            }
        }

        public static async Task InsertOneConflictCustomerAsync(DbConnection c, int customerId, string firstName, string lastName)
        {
            using (var command = c.CreateCommand())
            {
                command.CommandText =
                    "SET IDENTITY_INSERT Customer ON; " +
                    "Insert Into Customer (CustomerID, FirstName, LastName) Values (@customerId, @firstName, @lastName); " +
                    "SET IDENTITY_INSERT Customer OFF; ";

                var p = command.CreateParameter();
                p.DbType = DbType.Int32;
                p.Value = customerId;
                p.ParameterName = "@customerId";
                command.Parameters.Add(p);

                p = command.CreateParameter();
                p.DbType = DbType.String;
                p.Value = firstName;
                p.ParameterName = "@firstName";
                command.Parameters.Add(p);

                p = command.CreateParameter();
                p.DbType = DbType.String;
                p.Value = lastName;
                p.ParameterName = "@lastName";
                command.Parameters.Add(p);

                await c.OpenAsync();
                var id = await command.ExecuteScalarAsync();
                c.Close();
            }
        }
        public static async Task InsertNConflictsCustomerAsync(DbConnection c, int rowsCount, int customerId, string firstName, string lastName)
        {
            using var command = c.CreateCommand();

            var strBuilder = new StringBuilder();
            strBuilder.AppendLine("SET IDENTITY_INSERT Customer ON; ");

            for (var i = 0; i < rowsCount; i++)
                strBuilder.AppendLine($"Insert Into Customer (CustomerID, FirstName, LastName) Values ({customerId++}, '{firstName}', '{lastName}'); ");

            strBuilder.AppendLine("SET IDENTITY_INSERT Customer OFF; ");

            command.CommandText = strBuilder.ToString();

            await c.OpenAsync();
            await command.ExecuteScalarAsync();
            c.Close();
        }

        public static async Task<SyncRow> GetCustomerAsync(DbConnection c, int customerId)
        {
            using var command = c.CreateCommand();
            command.CommandText = "Select CustomerID, FirstName, LastName from Customer Where CustomerID = @CustomerID";
            var p = command.CreateParameter();
            p.ParameterName = "@CustomerID";
            p.DbType = DbType.Int32;
            p.Value = customerId;
            command.Parameters.Add(p);

            var syncTable = new SyncTable("Customer");

            await c.OpenAsync();
            using (var reader = await command.ExecuteReaderAsync().ConfigureAwait(false))
                syncTable.Load(reader);
            c.Close();

            return syncTable.Rows[0];
        }

    }
}
