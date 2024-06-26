﻿using Dotmim.Sync.Sqlite;
using Dotmim.Sync.Tests.Models;
using Microsoft.Data.SqlClient;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.SampleConsole
{
    public static class DBHelper
    {
        private static IConfiguration configuration;

        static DBHelper()
        {
            configuration = new ConfigurationBuilder()
              .AddJsonFile("appsettings.json", false, true)
              .AddJsonFile("appsettings.local.json", true, true)
              .Build();

        }

        public static string GetRandomName(string pref = default)
        {
            var str1 = Path.GetRandomFileName().Replace(".", "").ToLowerInvariant();
            return $"{pref}{str1}";
        }


        public static string GetConnectionString(string connectionStringName) =>
            configuration.GetSection("ConnectionStrings")[connectionStringName];

        public static string GetDatabaseConnectionString(string dbName) =>
            string.Format(configuration.GetSection("ConnectionStrings")["SqlConnection"], dbName);

        public static string GetAzureDatabaseConnectionString(string dbName) =>
            string.Format(configuration.GetSection("ConnectionStrings")["AzureSqlConnection"], dbName);

        public static string GetMySqlDatabaseConnectionString(string dbName) =>
            string.Format(configuration.GetSection("ConnectionStrings")["MySqlConnection"], dbName);

        public static string GetMariadbDatabaseConnectionString(string dbName) =>
            string.Format(configuration.GetSection("ConnectionStrings")["MariadbConnection"], dbName);


        public static string GetNpgsqlDatabaseConnectionString(string dbName) =>
            string.Format(configuration.GetSection("ConnectionStrings")["NpgsqlConnection"], dbName);



        /// <summary>
        /// create a server database with datas and an empty client database
        /// </summary>
        /// <returns></returns>
        public static async Task EnsureDatabasesAsync(string databaseName, bool useSeeding = true)
        {
            // Create server database with items
            using var dbServer = new AdventureWorksContext(GetDatabaseConnectionString(databaseName), useSeeding);
            await dbServer.Database.EnsureDeletedAsync();
            await dbServer.Database.EnsureCreatedAsync();
        }

        public static async Task DeleteDatabaseAsync(string dbName)
        {
            var masterConnection = new SqlConnection(GetDatabaseConnectionString("master"));
            await masterConnection.OpenAsync();
            var cmdDb = new SqlCommand(GetDeleteDatabaseScript(dbName), masterConnection);
            await cmdDb.ExecuteNonQueryAsync();
            masterConnection.Close();
        }



        public static async Task CreateDatabaseAsync(string dbName, bool recreateDb = true)
        {
            var masterConnection = new SqlConnection(GetDatabaseConnectionString("master"));
            await masterConnection.OpenAsync();
            var cmdDb = new SqlCommand(GetCreationDBScript(dbName, recreateDb), masterConnection);
            await cmdDb.ExecuteNonQueryAsync();
            masterConnection.Close();
        }

        private static string GetDeleteDatabaseScript(string dbName) =>
                  $@"if (exists (Select * from sys.databases where name = '{dbName}'))
            begin
	            alter database [{dbName}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE
	            drop database {dbName}
            end";

        private static string GetCreationDBScript(string dbName, bool recreateDb = true)
        {
            if (recreateDb)
                return $@"if (exists (Select * from sys.databases where name = '{dbName}'))
                    begin
	                    alter database [{dbName}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE
	                    drop database {dbName}
                    end
                    Create database {dbName}";
            else
                return $@"if not (exists (Select * from sys.databases where name = '{dbName}')) 
                          Create database {dbName}";

        }

        public static async Task ExecuteSqliteScriptAsync(string connectionString, string commandText)
        {
            using var connection = new SqliteConnection(connectionString);
            connection.Open();

            using var cmdDb = new SqliteCommand(commandText, connection);
            await cmdDb.ExecuteNonQueryAsync();

            connection.Close();
        }

        public static async Task ExecuteScriptAsync(string dbName, string script)
        {
            using var connection = new SqlConnection(GetDatabaseConnectionString(dbName));
            connection.Open();

            //split the script on "GO" commands
            string[] splitter = new string[] { "\r\nGO\r\n" };
            string[] commandTexts = script.Split(splitter, StringSplitOptions.RemoveEmptyEntries);

            foreach (string commandText in commandTexts)
            {
                using var cmdDb = new SqlCommand(commandText, connection);
                await cmdDb.ExecuteNonQueryAsync();
            }
            connection.Close();
        }


        internal static async Task<Guid> AddProductCategoryRowAsync(
            CoreProvider provider, Guid? parentProductCategoryId = default, string name = default)
        {
            string commandText = $"Insert into ProductCategory (ProductCategoryId, ParentProductCategoryID, Name, ModifiedDate, rowguid) " +
                                 $"Values (@ProductCategoryId, @ParentProductCategoryID, @Name, @ModifiedDate, @rowguid)";

            var connection = provider.CreateConnection();

            connection.Open();

            var pId = Guid.NewGuid();

            var command = connection.CreateCommand();
            command.CommandText = commandText;
            command.Connection = connection;

            var p = command.CreateParameter();
            p.DbType = DbType.Guid;
            p.ParameterName = "@ProductCategoryId";
            p.Value = pId;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.DbType = DbType.Guid;
            p.ParameterName = "@ParentProductCategoryID";
            p.Value = parentProductCategoryId.HasValue  ?  parentProductCategoryId : DBNull.Value ;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.DbType = DbType.String;
            p.ParameterName = "@Name";
            p.Value = string.IsNullOrEmpty(name) ? Path.GetRandomFileName().Replace(".", "").ToLowerInvariant() + ' ' + Path.GetRandomFileName().Replace(".", "").ToLowerInvariant() : name;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.DbType = DbType.Guid;
            p.ParameterName = "@rowguid";
            p.Value = Guid.NewGuid();
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.DbType = DbType.DateTime;
            p.ParameterName = "@ModifiedDate";
            p.Value = DateTime.UtcNow;
            command.Parameters.Add(p);

            await command.ExecuteNonQueryAsync();

            connection.Close();

            return pId;
        }

        internal static async Task DeleteProductCategoryRowAsync(CoreProvider provider, Guid? productId = default, string name = default)
        {
            string commandText = $"Delete From ProductCategory Where " +
                                 $"(ProductCategoryId = @ProductCategoryId And @ProductCategoryId is not null) OR " +
                                 $"(Name = @Name And @Name is not null)";

            var connection = provider.CreateConnection();

            connection.Open();

            var command = connection.CreateCommand();
            command.CommandText = commandText;
            command.Connection = connection;

            var p = command.CreateParameter();
            p.DbType = DbType.Guid;
            p.ParameterName = "@ProductCategoryId";
            p.Value = productId.HasValue ? productId.Value : DBNull.Value;
            command.Parameters.Add(p);


            p = command.CreateParameter();
            p.DbType = DbType.String;
            p.ParameterName = "@Name";
            p.Value = string.IsNullOrEmpty(name) ? DBNull.Value : name;
            command.Parameters.Add(p);

            await command.ExecuteNonQueryAsync();

            connection.Close();
        }

        private static async Task AddProductRowAsync(CoreProvider provider)
        {

            string commandText = "Insert into Product (Name, ProductNumber, StandardCost, ListPrice, SellStartDate) Values (@Name, @ProductNumber, @StandardCost, @ListPrice, @SellStartDate)";
            var connection = provider.CreateConnection();

            connection.Open();

            var command = connection.CreateCommand();
            command.CommandText = commandText;
            command.Connection = connection;

            var p = command.CreateParameter();
            p.DbType = DbType.String;
            p.ParameterName = "@Name";
            p.Value = Path.GetRandomFileName().Replace(".", "").ToLowerInvariant();
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.DbType = DbType.String;
            p.ParameterName = "@ProductNumber";
            p.Value = Path.GetRandomFileName().Replace(".", "").ToLowerInvariant().Substring(0, 6).ToUpperInvariant();
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.DbType = DbType.Double;
            p.ParameterName = "@StandardCost";
            p.Value = 100;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.DbType = DbType.Double;
            p.ParameterName = "@ListPrice";
            p.Value = 100;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.DbType = DbType.DateTime;
            p.ParameterName = "@SellStartDate";
            p.Value = DateTime.UtcNow;
            command.Parameters.Add(p);

            await command.ExecuteNonQueryAsync();

            connection.Close();

        }
        private static async Task AddProductCategoryRowWithOneMoreColumnAsync(CoreProvider provider)
        {

            string commandText = "Insert into ProductCategory (ProductCategoryId, Name, ModifiedDate, CreatedDate, rowguid) Values (@ProductCategoryId, @Name, @ModifiedDate, @CreatedDate, @rowguid)";
            var connection = provider.CreateConnection();

            connection.Open();

            var command = connection.CreateCommand();
            command.CommandText = commandText;
            command.Connection = connection;

            var p = command.CreateParameter();
            p.DbType = DbType.String;
            p.ParameterName = "@Name";
            p.Value = Path.GetRandomFileName().Replace(".", "").ToLowerInvariant();
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.DbType = DbType.Guid;
            p.ParameterName = "@ProductCategoryId";
            p.Value = Guid.NewGuid();
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.DbType = DbType.DateTime;
            p.ParameterName = "@ModifiedDate";
            p.Value = DateTime.UtcNow;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.DbType = DbType.DateTime;
            p.ParameterName = "@CreatedDate";
            p.Value = DateTime.UtcNow;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.DbType = DbType.Guid;
            p.ParameterName = "@rowguid";
            p.Value = Guid.NewGuid();
            command.Parameters.Add(p);

            await command.ExecuteNonQueryAsync();

            connection.Close();

        }


        private static async Task AddColumnsToProductCategoryAsync(CoreProvider provider)
        {
            var commandText = @"ALTER TABLE dbo.ProductCategory ADD CreatedDate datetime NULL;";

            var connection = provider.CreateConnection();

            connection.Open();

            var command = connection.CreateCommand();
            command.CommandText = commandText;
            command.Connection = connection;

            await command.ExecuteNonQueryAsync();

            connection.Close();
        }
        private static async Task UpdateAllProductCategoryAsync(CoreProvider provider, string addedString)
        {
            string commandText = "Update ProductCategory Set Name = Name + @addedString";
            var connection = provider.CreateConnection();

            connection.Open();

            var command = connection.CreateCommand();
            command.CommandText = commandText;
            command.Connection = connection;

            var p = command.CreateParameter();
            p.DbType = DbType.String;
            p.ParameterName = "@addedString";
            p.Value = addedString;
            command.Parameters.Add(p);

            await command.ExecuteNonQueryAsync();

            connection.Close();
        }

    }
}