using System;
using System.Data;
using System.Data.Common;
using System.Threading.Tasks;

namespace Orchestrators
{
    public static class Helper
    {
        public static async Task InsertOneProductCategoryAsync(DbConnection c, string updatedName)
        {
            using (var command = c.CreateCommand())
            {
                command.CommandText = "Insert Into ProductCategory (ProductCategoryID, Name) Values (@ProductCategoryID, @Name)";

                var p = command.CreateParameter();
                p.DbType = DbType.Guid;
                p.Value = Guid.NewGuid();
                p.ParameterName = "@ProductCategoryID";
                command.Parameters.Add(p);

                p = command.CreateParameter();
                p.DbType = DbType.String;
                p.Value = updatedName;
                p.ParameterName = "@Name";
                command.Parameters.Add(p);

                await c.OpenAsync().ConfigureAwait(false);
                var id = await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                c.Close();
            }
        }

        public static async Task<int> InsertOneCustomerAsync(DbConnection c, string firstName, string lastName)
        {
            using (var command = c.CreateCommand())
            {
                command.CommandText = "Insert Into Customer (FirstName, LastName) Values (@firstName, @lastName); SELECT SCOPE_IDENTITY();";
                var p = command.CreateParameter();
                p.DbType = DbType.String;
                p.Value = firstName;
                p.ParameterName = "@firstName";
                command.Parameters.Add(p);

                p = command.CreateParameter();
                p.DbType = DbType.String;
                p.Value = lastName;
                p.ParameterName = "@lastName";
                command.Parameters.Add(p);

                await c.OpenAsync().ConfigureAwait(false);
                var id = await command.ExecuteScalarAsync().ConfigureAwait(false);
                c.Close();

                return Convert.ToInt32(id);
            }
        }
    }
}