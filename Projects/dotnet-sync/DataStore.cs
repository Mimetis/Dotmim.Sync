using System;
using System.Collections.Generic;
using System.Data;
using Microsoft.Data.Sqlite;
using System.Diagnostics;
using System.IO;
using System.Text;

namespace Dotmim.Sync.Tools
{
    public class DataStore
    {
        public string ConnectionString { get; }
        internal const string DEFAULT_PROJECT_NAME = "dotmim_sync_p0";
        internal const string DEFAULT_DATABASE_NAME = "dotmim_sync.db";
        internal const string PROJECT_TABLE = "ds_project";
        internal const string TABLE_TABLE = "ds_table";
        internal const string CONF_TABLE = "ds_conf";
        internal const int OUTPUT_COLUMN_WIDTH = 32;
        private SqliteConnection connection;
        //private SqliteTransaction transaction;

        private static DataStore dataStore;

        /// <summary>
        /// Gets the current datastore instance
        /// </summary>
        public static DataStore Current => dataStore ?? (dataStore = new DataStore());

        private DataStore()
        {
            var s = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
            var dmsyncfolder = Path.Combine(s, ".dmsync");

            if (!Directory.Exists(dmsyncfolder))
                Directory.CreateDirectory(dmsyncfolder);

            var builder = new SqliteConnectionStringBuilder { DataSource = Path.Combine(dmsyncfolder, DEFAULT_DATABASE_NAME) };

            // prefer to store guid in text
            //builder.Add("BinaryGUID", false);

            this.ConnectionString = builder.ConnectionString;
            this.connection = new SqliteConnection(this.ConnectionString);
        }

        internal void EnsureDatabase()
        {
            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    connection.Open();

                var existCommand = BuildExistTableCommand(PROJECT_TABLE);
                existCommand.Connection = connection;

                var tableExist = (long)existCommand.ExecuteScalar() != 0L;

                if (tableExist)
                    return;

                using (var t = connection.BeginTransaction())
                {
                    var command = BuildTableProjectCommand();
                    command.Connection = connection;
                    command.Transaction = t;
                    command.ExecuteNonQuery();

                    command = BuildTableTableCommand();
                    command.Connection = connection;
                    command.Transaction = t;
                    command.ExecuteNonQuery();

                    t.Commit();
                }

            }
            finally
            {
                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();
            }
            return;
        }

        internal void DeleteTable(string projectName, string tableName)
        {
            bool alreadyOpened = connection.State == ConnectionState.Open;
            try
            {
                if (!alreadyOpened)
                    connection.Open();

                var command = connection.CreateCommand();

                //if (transaction != null)
                //    command.Transaction = transaction;

                command.CommandText = $"Delete from {TABLE_TABLE} where project_name=@projectName and name=@tableName";

                var p = command.CreateParameter();
                p.ParameterName = "@projectName";
                p.Value = projectName;
                p.DbType = DbType.String;
                command.Parameters.Add(p);

                p = command.CreateParameter();
                p.ParameterName = "@tableName";
                p.Value = tableName;
                p.DbType = DbType.String;
                command.Parameters.Add(p);

                command.ExecuteNonQuery();
            }
            finally
            {
                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();
            }
        }

        internal void SaveTable(string projectName, Table tbl)
        {
            bool alreadyOpened = connection.State == ConnectionState.Open;
            try
            {
                if (!alreadyOpened)
                    connection.Open();

                var command = connection.CreateCommand();

                //if (transaction != null)
                //    command.Transaction = transaction;

                command.CommandText = $"Select count(*) from {TABLE_TABLE} where project_name=@projectName and name=@tableName";

                var p = command.CreateParameter();
                p.ParameterName = "@projectName";
                p.Value = projectName;
                p.DbType = DbType.String;
                command.Parameters.Add(p);

                p = command.CreateParameter();
                p.ParameterName = "@tableName";
                p.Value = tbl.Name;
                p.DbType = DbType.String;
                command.Parameters.Add(p);

                var tableExist = (long)command.ExecuteScalar() > 0;

                command.CommandText = $"Select count(*) from {TABLE_TABLE} where project_name=@projectName";

                p = command.CreateParameter();
                p.ParameterName = "@projectName";
                p.Value = projectName;
                p.DbType = DbType.String;
                command.Parameters.Add(p);

                p = command.CreateParameter();
                p.ParameterName = "@tableName";
                p.Value = tbl.Name;
                p.DbType = DbType.String;
                command.Parameters.Add(p);

                var orderNumberMax = (long)command.ExecuteScalar() ;


                string commandTableText = string.Empty;
                if (tableExist)
                    commandTableText = $@"Update {TABLE_TABLE} 
                                             Set schema=@schema, direction=@direction, order_number=@order_number
                                             Where project_name=@projectName and name=@tableName";
                else
                    commandTableText = $@"Insert into {TABLE_TABLE} 
                                             (name, project_name, schema, direction, order_number)
                                             Values
                                             (@tableName, @projectName, @schema, @direction, @order_number)";

                command = connection.CreateCommand();
                command.CommandText = commandTableText;

                p = command.CreateParameter();
                p.ParameterName = "@projectName";
                p.Value = projectName;
                p.DbType = DbType.String;
                command.Parameters.Add(p);

                p = command.CreateParameter();
                p.ParameterName = "@tableName";
                p.Value = tbl.Name;
                p.DbType = DbType.String;
                command.Parameters.Add(p);

                p = command.CreateParameter();
                p.ParameterName = "@schema";
                p.Value = tbl.Schema;
                p.DbType = DbType.String;
                command.Parameters.Add(p);

                p = command.CreateParameter();
                p.ParameterName = "@order_number";
                p.Value = tbl.Order > 0 ? tbl.Order : orderNumberMax;
                p.DbType = DbType.Int64;
                command.Parameters.Add(p);

                string direction = null;
                if (tbl.Direction == Enumerations.SyncDirection.Bidirectional)
                    direction = "bidirectional";
                if (tbl.Direction == Enumerations.SyncDirection.UploadOnly)
                    direction = "uploadonly";
                if (tbl.Direction == Enumerations.SyncDirection.DownloadOnly)
                    direction = "downloadonly";

                p = command.CreateParameter();
                p.ParameterName = "@direction";
                p.Value = direction;
                p.DbType = DbType.String;
                command.Parameters.Add(p);

                command.ExecuteNonQuery();
            }
            finally
            {
                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();
            }
        }

        internal List<Project> LoadAllProjects()
        {
            List<Project> projects = new List<Project>();

            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    connection.Open();

                var command = connection.CreateCommand();

                command.CommandText = $"Select * from {PROJECT_TABLE}";

                using (var reader = command.ExecuteReader())
                {
                    if (!reader.HasRows)
                        return projects;

                    while (reader.Read())
                    {
                        Project project = FillProject(reader, connection);
                        projects.Add(project);
                    }

                }
            }
            finally
            {
                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();
            }

            return projects;
        }

        internal Project LoadProject(string name)
        {
            Project project = null;

            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    connection.Open();

                var command = connection.CreateCommand();

                command.CommandText = $"Select * from {PROJECT_TABLE} where name = @name";
                var p = command.CreateParameter();
                p.ParameterName = "@name";
                p.Value = name;
                p.DbType = DbType.String;
                command.Parameters.Add(p);

                using (var reader = command.ExecuteReader())
                {
                    if (!reader.HasRows)
                        return null;

                    if (!reader.Read())
                        return null;

                    project = FillProject(reader, connection);
                }
            }
            finally
            {
                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();
            }

            return project;
        }


        private Project FillProject(SqliteDataReader reader, SqliteConnection connection)
        {
            var projectName = reader["name"] as string;

            // Create a default project
            var project = Project.CreateProject(projectName);

            String serverProvider = reader["server_provider"] != DBNull.Value ? reader["server_provider"] as string : null;
            String serverProviderCs = reader["server_provider_cs"] != DBNull.Value ? reader["server_provider_cs"] as string : null;
            String clientProvider = reader["client_provider"] != DBNull.Value ? reader["client_provider"] as string : null;
            String clientProviderCs = reader["client_provider_cs"] != DBNull.Value ? reader["client_provider_cs"] as string : null;

            if (!string.IsNullOrEmpty(serverProvider))
            {
                if (serverProvider == "sqlserver")
                    project.ServerProvider.ProviderType = ProviderType.SqlServer;
                if (serverProvider == "sqlite")
                    project.ServerProvider.ProviderType = ProviderType.Sqlite;
                if (serverProvider == "web")
                    project.ServerProvider.ProviderType = ProviderType.Web;

                if (!String.IsNullOrEmpty(serverProviderCs))
                    project.ServerProvider.ConnectionString = serverProviderCs;

                project.ServerProvider.SyncType = SyncType.Server;
            }
            if (!string.IsNullOrEmpty(clientProvider))
            {
                if (clientProvider == "sqlserver")
                    project.ClientProvider.ProviderType = ProviderType.SqlServer;
                if (clientProvider == "sqlite")
                    project.ClientProvider.ProviderType = ProviderType.Sqlite;

                if (!String.IsNullOrEmpty(clientProviderCs))
                    project.ClientProvider.ConnectionString = clientProviderCs;

                project.ClientProvider.SyncType = SyncType.Client;
            }

            String conflict = reader["conflict"] != DBNull.Value ? reader["conflict"] as string : null;
            Int64 batchSize = reader["batch_size"] != DBNull.Value ? (Int64)reader["batch_size"] : 0L;
            String batchDirectory = reader["batch_directory"] != DBNull.Value ? reader["batch_directory"] as string : null;
            String format = reader["format"] != DBNull.Value ? reader["format"] as string : null;
            Int64 bulkOperations = reader["bulk_operations"] != DBNull.Value ? (Int64)reader["bulk_operations"] : 0L;

            if (!string.IsNullOrEmpty(batchDirectory))
                project.Configuration.BatchDirectory = batchDirectory;

            if (conflict == "serverwins")
                project.Configuration.ConflictResolutionPolicy = Enumerations.ConflictResolutionPolicy.ServerWins;

            if (conflict == "clientwins")
                project.Configuration.ConflictResolutionPolicy = Enumerations.ConflictResolutionPolicy.ClientWins;

            if (format == "json")
                project.Configuration.SerializationFormat = Enumerations.SerializationFormat.Json;

            if (format == "bin")
                project.Configuration.SerializationFormat = Enumerations.SerializationFormat.Binary;

            if (batchSize > 0)
                project.Configuration.DownloadBatchSizeInKB = batchSize;

            if (bulkOperations >= 0)
                project.Configuration.UseBulkOperations = bulkOperations == 1;


            var command = connection.CreateCommand();

            command.CommandText = $"Select * from {TABLE_TABLE} where project_name = @name";
            var p = command.CreateParameter();
            p.ParameterName = "@name";
            p.Value = project.Name;
            p.DbType = DbType.String;
            command.Parameters.Add(p);

            using (var readerTable = command.ExecuteReader())
            {
                while (readerTable.Read())
                {
                    Table table = new Table
                    {
                        Name = readerTable["name"] as string,
                        Order = Convert.ToInt32((long)readerTable["order_number"]),
                        Schema = readerTable["schema"] != DBNull.Value ? readerTable["schema"] as string : null
                    };

                    String direction = readerTable["direction"] != DBNull.Value ? readerTable["direction"] as string : null;
                    if (direction == "bidirectional" || string.IsNullOrEmpty(direction))
                        table.Direction = Enumerations.SyncDirection.Bidirectional;
                    if (direction == "uploadonly")
                        table.Direction = Enumerations.SyncDirection.UploadOnly;
                    if (direction == "downloadonly")
                        table.Direction = Enumerations.SyncDirection.DownloadOnly;

                    project.Tables.Add(table);
                }

            }

            return project;
        }

        internal void DeleteProject(Project project)
        {
            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    connection.Open();

                var command = connection.CreateCommand();

                command.CommandText = $"Delete from {TABLE_TABLE} where project_name = @name";
                var p = command.CreateParameter();
                p.ParameterName = "@name";
                p.Value = project.Name;
                p.DbType = DbType.String;
                command.Parameters.Add(p);

                command.ExecuteNonQuery();

                command = connection.CreateCommand();

                command.CommandText = $"Delete from {PROJECT_TABLE} where name = @name";
                p = command.CreateParameter();
                p.ParameterName = "@name";
                p.Value = project.Name;
                p.DbType = DbType.String;
                command.Parameters.Add(p);

                command.ExecuteNonQuery();
            }
            finally
            {
                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();
            }
        }

        internal bool SaveProject(Project project)
        {
            bool isNew = false;

            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    connection.Open();

                var command = connection.CreateCommand();

                command.CommandText = $"Select count(*) from {PROJECT_TABLE} where name = @name";
                var p = command.CreateParameter();
                p.ParameterName = "@name";
                p.Value = project.Name;
                p.DbType = DbType.String;
                command.Parameters.Add(p);

                var exist = (long)command.ExecuteScalar() > 0;

                // set if it's a new project
                isNew = !exist;

                string commandText = string.Empty;
                if (exist)
                    commandText = $@"Update {PROJECT_TABLE} 
                                             Set server_provider=@server_provider, server_provider_cs=@server_provider_cs, 
                                                 client_provider=@client_provider, client_provider_cs=@client_provider_cs,
                                                 conflict=@conflict, batch_size=@batch_size, batch_directory=@batch_directory,
                                                 format=@format, bulk_operations=@bulk_operations
                                             Where name=@name";
                else
                    commandText = $@"Insert into {PROJECT_TABLE} 
                                             (name, server_provider, server_provider_cs, client_provider, client_provider_cs,
                                              conflict, batch_size, batch_directory, format, bulk_operations)
                                             Values
                                             (@name, @server_provider, @server_provider_cs, @client_provider, @client_provider_cs,
                                              @conflict, @batch_size, @batch_directory, @format, @bulk_operations)";
                command = connection.CreateCommand();

                command.CommandText = commandText;

                p = command.CreateParameter();
                p.ParameterName = "@name";
                p.Value = project.Name;
                p.DbType = DbType.String;
                command.Parameters.Add(p);

                p = command.CreateParameter();
                p.ParameterName = "@server_provider";
                p.Value = project.ServerProvider.ProviderType.ToString().ToLowerInvariant();
                p.DbType = DbType.String;
                command.Parameters.Add(p);

                p = command.CreateParameter();
                p.ParameterName = "@server_provider_cs";
                p.Value = project.ServerProvider.ConnectionString;
                p.DbType = DbType.String;
                command.Parameters.Add(p);

                p = command.CreateParameter();
                p.ParameterName = "@client_provider";
                p.Value = project.ClientProvider.ProviderType.ToString().ToLowerInvariant();
                p.DbType = DbType.String;
                command.Parameters.Add(p);

                p = command.CreateParameter();
                p.ParameterName = "@client_provider_cs";
                p.Value = project.ClientProvider.ConnectionString;
                p.DbType = DbType.String;
                command.Parameters.Add(p);

                p = command.CreateParameter();
                p.ParameterName = "@conflict";
                p.Value = project.Configuration.ConflictResolutionPolicy == Enumerations.ConflictResolutionPolicy.ServerWins ? "severwins" : "clientwins";
                p.DbType = DbType.String;
                command.Parameters.Add(p);

                p = command.CreateParameter();
                p.ParameterName = "@batch_directory";
                p.Value = string.IsNullOrEmpty(project.Configuration.BatchDirectory) ? DBNull.Value : (Object)project.Configuration.BatchDirectory;
                p.DbType = DbType.String;
                command.Parameters.Add(p);

                p = command.CreateParameter();
                p.ParameterName = "@batch_size";
                p.Value = project.Configuration.DownloadBatchSizeInKB <= 0 ? DBNull.Value : (Object)project.Configuration.DownloadBatchSizeInKB;
                p.DbType = DbType.Int64;
                command.Parameters.Add(p);

                p = command.CreateParameter();
                p.ParameterName = "@format";
                p.Value = project.Configuration.SerializationFormat == Enumerations.SerializationFormat.Json ? "json" : "bin";
                p.DbType = DbType.String;
                command.Parameters.Add(p);

                p = command.CreateParameter();
                p.ParameterName = "@bulk_operations";
                p.Value = project.Configuration.UseBulkOperations ? 1 : 0;
                p.DbType = DbType.Int64;
                command.Parameters.Add(p);

                command.ExecuteNonQuery();
            }
            finally
            {
                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();
            }
            return isNew;
        }



        private SqliteCommand BuildExistTableCommand(string tableName)
        {
            var dbCommand = new SqliteCommand();
            dbCommand.CommandText = "select count(*) from sqlite_master where name = @tableName AND type='table'";

            SqliteParameter SqliteParameter = new SqliteParameter()
            {
                ParameterName = "@tableName",
                Value = tableName
            };
            dbCommand.Parameters.Add(SqliteParameter);

            return dbCommand;
        }

        private SqliteCommand BuildTableProjectCommand()
        {
            StringBuilder stringBuilder = new StringBuilder($"CREATE TABLE IF NOT EXISTS {PROJECT_TABLE} (");
            stringBuilder.AppendLine($"name text NOT NULL,");
            stringBuilder.AppendLine($"server_provider text NULL,");
            stringBuilder.AppendLine($"server_provider_cs text NULL,");
            stringBuilder.AppendLine($"client_provider text NULL,");
            stringBuilder.AppendLine($"client_provider_cs text NULL,");
            stringBuilder.AppendLine($"conflict text NOT NULL,");
            stringBuilder.AppendLine($"batch_size integer NULL,");
            stringBuilder.AppendLine($"batch_directory text NULL,");
            stringBuilder.AppendLine($"format text NOT NULL,");
            stringBuilder.AppendLine($"bulk_operations integer NOT NULL,");
            stringBuilder.AppendLine($"PRIMARY KEY (name))");

            return new SqliteCommand(stringBuilder.ToString());
        }

        private SqliteCommand BuildTableTableCommand()
        {
            StringBuilder stringBuilder = new StringBuilder($"CREATE TABLE IF NOT EXISTS {TABLE_TABLE} (");
            stringBuilder.AppendLine($"name text NOT NULL,");
            stringBuilder.AppendLine($"project_name text NOT NULL,");
            stringBuilder.AppendLine($"schema text NULL,");
            stringBuilder.AppendLine($"direction text NULL,");
            stringBuilder.AppendLine($"order_number integer NOT NULL,");
            stringBuilder.AppendLine($"PRIMARY KEY (project_name, name),");
            stringBuilder.AppendLine($"FOREIGN KEY (project_name) REFERENCES {PROJECT_TABLE} (name))");

            return new SqliteCommand(stringBuilder.ToString());

        }

    }
}
