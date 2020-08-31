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
                command.CommandText = "ALTER TABLE dbo.Address ADD CreatedDate datetime NULL;";
                c.Open();
                await command.ExecuteNonQueryAsync();
                c.Close();
            }
        }


    }
}
