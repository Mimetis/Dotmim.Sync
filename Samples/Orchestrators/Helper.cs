using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Text;
using System.Threading.Tasks;

namespace Orchestrators
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

                await c.OpenAsync();
                var id = await command.ExecuteScalarAsync();
                c.Close();

                return Convert.ToInt32(id);
            }
        }
        public static async Task UpdateOneProductCategoryAsync(DbConnection c, int productCategoryId, string updatedName)
        {
            using (var command = c.CreateCommand())
            {
                command.CommandText = "Update ProductCategory Set Name = @Name Where ProductCategoryId = @Id";
                var p = command.CreateParameter();
                p.DbType = DbType.String;
                p.Value = updatedName;
                p.ParameterName = "@Name";
                command.Parameters.Add(p);

                p = command.CreateParameter();
                p.DbType = DbType.Int32;
                p.Value = productCategoryId;
                p.ParameterName = "@Id";
                command.Parameters.Add(p);

                await c.OpenAsync();
                await command.ExecuteNonQueryAsync();
                c.Close();
            }
        }

    }
}
