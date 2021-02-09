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
        public static async Task InsertNConflictsCustomerAsync(DbConnection c, int n, int customerId, string firstName, string lastName)
        {
            using var command = c.CreateCommand();

            var strBuilder = new StringBuilder();
            strBuilder.AppendLine("SET IDENTITY_INSERT Customer ON; ");

            for (var i = 0; i < n; i++)
                strBuilder.AppendLine($"Insert Into Customer (CustomerID, FirstName, LastName) Values ({customerId++}, '{firstName}', '{lastName}'); ");

            strBuilder.AppendLine("SET IDENTITY_INSERT Customer OFF; ");

            command.CommandText = strBuilder.ToString();

            await c.OpenAsync();
            await command.ExecuteScalarAsync();
            c.Close();
        }
    }
}
