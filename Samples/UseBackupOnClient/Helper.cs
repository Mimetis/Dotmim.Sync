using Microsoft.Data.SqlClient;
using System.Data;
using System.Diagnostics;

namespace UseBackupOnClient
{
    public static class Helper
    {

        /// <summary>
        /// Backup a SQL Server database.
        /// </summary>
        public static void BackupDatabase(string dbName, string connectionString)
        {
            if (!Directory.Exists(Path.Combine(Path.GetTempPath(), "Backup")))
                Directory.CreateDirectory(Path.Combine(Path.GetTempPath(), "Backup"));

            var localDatabasePath = Path.Combine(Path.GetTempPath(), "Backup", $"{dbName}.bak");

            var formatMediaName = $"DatabaseToolkitBackup_{dbName}";

            using var connection = new SqlConnection(connectionString);

            var sql = @$"BACKUP DATABASE [{dbName}] TO DISK = N'{localDatabasePath}' WITH NAME = '{formatMediaName}'";

            connection.Open();

            using (var command = new SqlCommand(sql, connection))
            {
                command.CommandType = CommandType.Text;
                command.ExecuteNonQuery();
            }

            connection.Close();
        }

        /// <summary>
        /// Restore a sql backup file.
        /// </summary>
        public static void RestoreSqlDatabase(string dbName, string masterConnectionString)
        {
            var localDatabasePath = Path.Combine(Path.GetTempPath(), "Backup", $"{dbName}.bak");

            var script = $@"
                if (exists (select * from sys.databases where name = '{dbName}'))
                begin                
                    ALTER DATABASE [{dbName}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE
                End

                RESTORE DATABASE [{dbName}] FROM  DISK = N'{localDatabasePath}' WITH  RESTRICTED_USER, NOUNLOAD, REPLACE

                ALTER DATABASE [{dbName}] SET MULTI_USER";

            using var connection = new SqlConnection(masterConnectionString);
            connection.Open();

            using var cmdDb = new SqlCommand(script, connection);

            cmdDb.ExecuteNonQuery();

            connection.Close();
        }

        /// <summary>
        /// Restore a sql backup file from a server to client.
        /// </summary>
        public static void RestoreSqlDatabase(string fromBackupName, string toDestinationDatabaseName, string masterConnectionString)
        {
            var dataName = Path.GetFileNameWithoutExtension(toDestinationDatabaseName) + ".mdf";
            var logName = Path.GetFileNameWithoutExtension(toDestinationDatabaseName) + ".ldf";

            var localDatabaseBackupPath = Path.Combine(Path.GetTempPath(), "Backup", $"{fromBackupName}.bak");

            var script = $@"
                if (exists (select * from sys.databases where name = '{toDestinationDatabaseName}'))
                    begin                
                        ALTER DATABASE [{toDestinationDatabaseName}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE
                    End
                else
                    begin
                        CREATE DATABASE [{toDestinationDatabaseName}]
                    end

                -- the backup contains the full path to the database files
                -- in order to be able to restore them on different developer machines
                -- we retrieve the default data path from the server
                -- and use it in RESTORE with the MOVE option
                declare @databaseFolder as nvarchar(256);
                set @databaseFolder = Convert(nvarchar(256), (SELECT ServerProperty(N'InstanceDefaultDataPath') AS default_file));

                declare @dataFile as nvarchar(256);
                declare @logFile as nvarchar(256);
                set @dataFile =@databaseFolder + '{dataName}';
                set @logFile =@databaseFolder + '{logName}';

                RESTORE DATABASE [{toDestinationDatabaseName}] FROM  DISK = N'{localDatabaseBackupPath}' WITH  RESTRICTED_USER, REPLACE,
                    MOVE '{fromBackupName}' TO @dataFile,
                    MOVE '{fromBackupName}_log' TO @logFile;
                ALTER DATABASE [{toDestinationDatabaseName}] SET MULTI_USER";

            using var connection = new SqlConnection(masterConnectionString);
            connection.Open();

            using (var cmdDb = new SqlCommand(script, connection))
                cmdDb.ExecuteNonQuery();

            connection.Close();
        }

        public static void DropSqlDatabase(string dbName, string masterConnectionString)
        {
            using var masterConnection = new SqlConnection(masterConnectionString);

            var script = $@"if (exists (Select * from sys.databases where name = '{dbName}'))
            begin
	            alter database [{dbName}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	            drop database {dbName};
            end";
            try
            {
                masterConnection.Open();

                using var cmdDb = new SqlCommand(script, masterConnection);
                cmdDb.ExecuteNonQuery();

                masterConnection.Close();
            }
            catch (Exception ex)
            {
                Debug.WriteLine(ex.Message);
                throw;
            }
        }

        internal static async Task<Guid> AddProductCategoryRowAsync(string sqlConnectionString, string name = default)
        {

            var clothingGuid = new Guid("10A7C342-CA82-48D4-8A38-46A2EB089B74");
            var newId = Guid.NewGuid();

            string commandText = $"Insert into ProductCategory (ProductCategoryId, ParentProductCategoryID, Name, ModifiedDate, rowguid) " +
                                 $"Values (@ProductCategoryId, @ParentProductCategoryID, @Name, @ModifiedDate, @rowguid)";

            var connection = new SqlConnection(sqlConnectionString);

            connection.Open();

            var command = connection.CreateCommand();
            command.CommandText = commandText;
            command.Connection = connection;

            var p = command.CreateParameter();
            p.DbType = DbType.Guid;
            p.ParameterName = "@ProductCategoryId";
            p.Value = newId;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.DbType = DbType.Guid;
            p.ParameterName = "@ParentProductCategoryID";
            p.Value = clothingGuid;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.DbType = DbType.String;
            p.ParameterName = "@Name";
            p.Value = string.IsNullOrEmpty(name) ? Path.GetRandomFileName().Replace(".", string.Empty).ToLowerInvariant() + ' ' + Path.GetRandomFileName().Replace(".", string.Empty).ToLowerInvariant() : name;
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

            await command.ExecuteNonQueryAsync().ConfigureAwait(false);

            connection.Close();

            return newId;
        }

        public static string GetRandomName(string pref = default)
        {
            var str1 = Path.GetRandomFileName().Replace(".", string.Empty).ToLowerInvariant();
            return $"{pref}{str1}";
        }
    }
}