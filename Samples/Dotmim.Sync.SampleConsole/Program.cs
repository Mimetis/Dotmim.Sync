﻿using Dotmim.Sync;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.SampleConsole;
using Dotmim.Sync.Sqlite;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Web.Client;
using Dotmim.Sync.Web.Server;
using Microsoft.AspNetCore.Http;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Data;
using System.Data.Common;
using Microsoft.Data.SqlClient;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Serilog;
using Serilog.Events;
using Npgsql;
using Microsoft.Extensions.Configuration;
using NpgsqlTypes;
using Newtonsoft.Json;
using System.Collections.Generic;
using Dotmim.Sync.Postgres;
using Dotmim.Sync.Postgres.Builders;
using Dotmim.Sync.MySql;
using MySql.Data.MySqlClient;
using System.Linq;
using System.Transactions;
using System.Threading;

internal class Program
{
    public static string serverDbName = "AdventureWorks";
    public static string serverProductCategoryDbName = "AdventureWorksProductCategory";
    public static string clientDbName = "Client";
    public static string[] allTables = new string[] {"ProductDescription", "ProductCategory",
                                                    "ProductModel", "Product",
                                                    "Address", "Customer", "CustomerAddress",
                                                    "SalesOrderHeader", "SalesOrderDetail" };

    public static string[] oneTable = new string[] { "ProductCategory" };
    private static async Task Main(string[] args)
    {

        await SynchronizeHeavyTableAsync();

    }

    private static async Task SynchronizeHeavyTableAsync()
    {
        // Create 2 Sql Sync providers
        var serverProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString("HeavyTables"));
        var clientProvider = new SqliteSyncProvider("heavyTwo.db");

        var setup = new SyncSetup(new string[] { "Customer" });

        var options = new SyncOptions();
        options.BatchSize = 4000;

        // Creating an agent that will handle all the process
        var agent = new SyncAgent(clientProvider, serverProvider, options, setup);

        var remoteProgress = new SynchronousProgress<ProgressArgs>(s =>
        {
            Console.ForegroundColor = ConsoleColor.Blue;
            Console.WriteLine($"{s.Context.SyncStage}:\t{s.Message}");
            Console.ResetColor();
        });
        agent.AddRemoteProgress(remoteProgress);

        // Using the Progress pattern to handle progession during the synchronization
        var progress = new SynchronousProgress<ProgressArgs>(s =>
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"{s.Context.SyncStage}:\t{s.Message}");
            Console.ResetColor();
        });

        do
        {
            // Console.Clear();
            Console.WriteLine("Sync Start");
            try
            {
                var r = await agent.SynchronizeAsync(progress);
                // Write results
                Console.WriteLine(r);

            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }


            //Console.WriteLine("Sync Ended. Press a key to start again, or Escapte to end");
        } while (Console.ReadKey().Key != ConsoleKey.Escape);

        Console.WriteLine("End");
    }
    private static async Task TestRemovingAColumnWithInterceptorAsync()
    {
        // Create 2 Sql Sync providers
        var serverProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(serverDbName));
        var clientProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(clientDbName));

        var setup = new SyncSetup(new string[] { "Address", "Customer", "CustomerAddress", "SalesOrderHeader", "SalesOrderDetail" });

        var options = new SyncOptions();
        // Creating an agent that will handle all the process
        var agent = new SyncAgent(clientProvider, serverProvider, options, allTables);

        agent.RemoteOrchestrator.OnDatabaseProvisioning(args =>
        {
            var schema = args.Schema;

            foreach (var table in schema.Tables)
            {
                var columnsToRemove = new string[] { "ModifiedDate", "rowguid" };
                foreach (var columnName in columnsToRemove)
                {
                    var column = table.Columns[columnName];

                    if (column != null)
                        table.Columns.Remove(column);
                }
            }

        });

        // Using the Progress pattern to handle progession during the synchronization
        var progress = new SynchronousProgress<ProgressArgs>(s =>
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"{s.Context.SyncStage}:\t{s.Message}");
            Console.ResetColor();
        });

        do
        {
            // Console.Clear();
            Console.WriteLine("Sync Start");
            try
            {
                var s1 = await agent.SynchronizeAsync();

                // Write results
                Console.WriteLine(s1);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }


            //Console.WriteLine("Sync Ended. Press a key to start again, or Escapte to end");
        } while (Console.ReadKey().Key != ConsoleKey.Escape);

        Console.WriteLine("End");
    }

    private static async Task TestMySqlToSqliteSyncAsync()
    {
        // Create Sqlite database and table
        CreateSqliteDatabaseAndTable("Scenario01", "Customer");

        // Create MySql database and table
        CreateMySqlDatabase("Scenario01");
        CreateMySqlTable("Scenario01", "Customer");

        // Add one mysql record
        AddMySqlRecord("Scenario01", "Customer");

        // Add one record in sqlite
        AddSqliteRecord("Scenario01", "Customer");

        // sync with a client database with rows, before sync is enabled, and who needs to be sent to server anyway
        await TestSyncAsync("Scenario01", "Scenario01", "Customer", false);

        // Add two records in sqlite
        AddSqliteRecord("Scenario01", "Customer");
        AddSqliteRecord("Scenario01", "Customer");

        // Add two records in mysql
        AddMySqlRecord("Scenario01", "Customer");
        AddMySqlRecord("Scenario01", "Customer");

        await TestSyncAsync("Scenario01", "Scenario01", "Customer", true);
    }

    private static void CreateSqliteDatabaseAndTable(string fileName, string tableName)
    {

        // Delete sqlite database
        string filePath = null;
        try
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();

            filePath = Path.Combine(Directory.GetCurrentDirectory(), $"{fileName}.db");

            if (File.Exists(filePath))
                File.Delete(filePath);

        }
        catch (Exception)
        {
        }



        var builder = new SqliteConnectionStringBuilder();
        builder.DataSource = $"{fileName}.db";

        if (!File.Exists(fileName))
        {
            string sql = $"CREATE TABLE [{tableName}] (" +
                        " [ID] text NOT NULL UNIQUE" +
                        ",[F1] integer NULL" +
                        ",[F2] text NOT NULL COLLATE NOCASE" +
                        ",[F3] text NULL COLLATE NOCASE" +
                        ", PRIMARY KEY([ID]))";

            using (SqliteConnection dBase = new SqliteConnection(builder.ConnectionString))
            {
                try
                {
                    dBase.Open();
                    SqliteCommand cmd = new SqliteCommand(sql, dBase);
                    cmd.ExecuteNonQuery();
                    dBase.Close();
                    Console.WriteLine("Database created: " + fileName);
                }
                catch (SqliteException ex)
                {
                    Console.WriteLine(ex.Message);
                }
            }
        }

    }
    private static void CreateMySqlDatabase(string databaseName)
    {

        var sysBuilder = new MySqlConnectionStringBuilder();

        sysBuilder.Database = "sys";
        sysBuilder.Port = 3307;
        sysBuilder.UserID = "root";
        sysBuilder.Password = "Password12!";

        string sqlDB = $"DROP DATABASE IF EXISTS `{databaseName}`; CREATE DATABASE `{databaseName}`;";

        using (var mySqlConnection = new MySqlConnection(sysBuilder.ConnectionString))
        {
            try
            {
                var cmd = new MySqlCommand(sqlDB, mySqlConnection);

                mySqlConnection.Open();
                cmd.ExecuteNonQuery();
                mySqlConnection.Close();

                Console.WriteLine("Database created: " + databaseName);
            }
            catch (SqliteException ex)
            {
                Console.WriteLine(ex.Message);
            }
        }
    }
    private static void CreateMySqlTable(string databaseName, string tableName)
    {

        var builder = new MySqlConnectionStringBuilder();

        builder.Database = databaseName;
        builder.Port = 3307;
        builder.UserID = "root";
        builder.Password = "Password12!";


        string sql = $"CREATE TABLE `{tableName}` (" +
            " `ID` char(36) NOT NULL " +
            ",`F1` int NULL" +
            ",`F2` longtext NOT NULL" +
            ",`F3` longtext NULL" +
            ", PRIMARY KEY(`ID`))";

        using (var mySqlConnection = new MySqlConnection(builder.ConnectionString))
        {
            try
            {
                var cmd = new MySqlCommand(sql, mySqlConnection);

                mySqlConnection.Open();
                cmd.ExecuteNonQuery();
                mySqlConnection.Close();

                Console.WriteLine("Table created.");
            }
            catch (SqliteException ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

    }

    private static void AddMySqlRecord(string databaseName, string tableName)
    {

        var builder = new MySqlConnectionStringBuilder();

        builder.Database = databaseName;
        builder.Port = 3307;
        builder.UserID = "root";
        builder.Password = "Password12!";

        string sql = $"INSERT INTO `{tableName}` (ID,F1,F2,F3) VALUES (@ID, 2, '2st2tem', '1111111')";

        using (MySqlConnection dBase = new MySqlConnection(builder.ConnectionString))
        {
            try
            {
                MySqlCommand cmd = new MySqlCommand(sql, dBase);
                var parameter = new MySqlParameter("@ID", Guid.NewGuid());
                cmd.Parameters.Add(parameter);

                dBase.Open();
                cmd.ExecuteNonQuery();
                dBase.Close();
                Console.WriteLine("Record added into table " + tableName);
            }
            catch (SqliteException ex)
            {
                Console.WriteLine(ex.Message);
            }
        }
    }
    private static void AddSqliteRecord(string fileName, string tableName)
    {

        var builder = new SqliteConnectionStringBuilder();
        builder.DataSource = $"{fileName}.db";

        string sql = $"INSERT INTO [{tableName}] (ID,F1,F2,F3) VALUES ( @ID, 1, '1stItem', '1111111')";

        using (SqliteConnection dBase = new SqliteConnection(builder.ConnectionString))
        {
            try
            {
                SqliteCommand cmd = new SqliteCommand(sql, dBase);
                var parameter = new SqliteParameter("@ID", Guid.NewGuid());
                cmd.Parameters.Add(parameter);


                dBase.Open();
                cmd.ExecuteNonQuery();
                dBase.Close();
                Console.WriteLine("Record added into table " + tableName);
            }
            catch (SqliteException ex)
            {
                Console.WriteLine(ex.Message);
            }
        }
    }


    private static async Task TestSyncAsync(string mySqlDatabaseName, string sqliteFileName, string tableName, bool uploadOnly = false)
    {
        var builder = new MySqlConnectionStringBuilder();

        builder.Database = mySqlDatabaseName;
        builder.Port = 3307;
        builder.UserID = "root";
        builder.Password = "Password12!";

        var serverProvider = new MySqlSyncProvider(builder.ConnectionString);

        var sqlitebuilder = new SqliteConnectionStringBuilder();
        sqlitebuilder.DataSource = $"{sqliteFileName}.db";

        var clientProvider = new SqliteSyncProvider(sqlitebuilder.ConnectionString);

        var setup = new SyncSetup(new string[] { tableName });
        setup.Tables[tableName].SyncDirection = uploadOnly ? SyncDirection.UploadOnly : SyncDirection.Bidirectional;

        var options = new SyncOptions();

        // Creating an agent that will handle all the process
        var agent = new SyncAgent(clientProvider, serverProvider, options, setup);

        // Using the Progress pattern to handle progession during the synchronization
        var progress = new SynchronousProgress<ProgressArgs>(s =>
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"{s.Context.SyncStage}:\t{s.Message}");
            Console.ResetColor();
        });

        do
        {
            // Console.Clear();
            Console.WriteLine("Sync Start");
            try
            {

                var s = await agent.SynchronizeAsync();
                Console.WriteLine(s);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }


            //Console.WriteLine("Sync Ended. Press a key to start again, or Escapte to end");
        } while (Console.ReadKey().Key != ConsoleKey.Escape);

        Console.WriteLine("End");
    }

    private static async Task TestClientHasRowsToSyncAnywayAsync(string mySqlDatabaseName, string sqliteFileName, string tableName)
    {
        var builder = new MySqlConnectionStringBuilder();

        builder.Database = mySqlDatabaseName;
        builder.Port = 3307;
        builder.UserID = "root";
        builder.Password = "Password12!";

        var serverProvider = new MySqlSyncProvider(builder.ConnectionString);

        var sqlitebuilder = new SqliteConnectionStringBuilder();
        sqlitebuilder.DataSource = $"{sqliteFileName}.db";

        var clientProvider = new SqliteSyncProvider(sqlitebuilder.ConnectionString);

        var setup = new SyncSetup(new string[] { tableName });

        var options = new SyncOptions { ConflictResolutionPolicy = ConflictResolutionPolicy.ClientWins };

        // Creating an agent that will handle all the process
        var agent = new SyncAgent(clientProvider, serverProvider, options, setup);

        // Using the Progress pattern to handle progession during the synchronization
        var progress = new SynchronousProgress<ProgressArgs>(s =>
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"{s.Context.SyncStage}:\t{s.Message}");
            Console.ResetColor();
        });

        do
        {
            // Console.Clear();
            Console.WriteLine("Sync Start");
            try
            {

                //var s = await agent.SynchronizeAsync();
                //Console.WriteLine(s);

                var schema = await agent.RemoteOrchestrator.GetSchemaAsync();

                await agent.LocalOrchestrator.UpdateUntrackedRowsAsync(schema);

                var s2 = await agent.SynchronizeAsync();
                Console.WriteLine(s2);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }


            //Console.WriteLine("Sync Ended. Press a key to start again, or Escapte to end");
        } while (Console.ReadKey().Key != ConsoleKey.Escape);

        Console.WriteLine("End");
    }


    private static async Task TestInsertJsonArrayPostgreSqlAsync()
    {
        var connection = new NpgsqlConnection("Host=localhost;Database=Client;User ID=postgres;Password=azerty31*;");

        var lstscopes = new List<scope>
        {
            new scope { sync_scope_id = Guid.NewGuid(), sync_scope_name = "jsonscope1" },
            new scope { sync_scope_id = Guid.NewGuid(), sync_scope_name = "jsonscope2" },
            new scope { sync_scope_id = Guid.NewGuid(), sync_scope_name = "jsonscope3" },
            new scope { sync_scope_id = Guid.NewGuid(), sync_scope_name = "jsonscope4" },
            new scope { sync_scope_id = Guid.NewGuid(), sync_scope_name = "jsonscope5" },
            new scope { sync_scope_id = Guid.NewGuid(), sync_scope_name = "jsonscope6" }
        };


        var p = new NpgsqlParameter
        {
            ParameterName = "scopes",
            NpgsqlDbType = NpgsqlDbType.Json,
            NpgsqlValue = JsonConvert.SerializeObject(lstscopes)
        };

        try
        {
            using (var command = new NpgsqlCommand("fn_upsert_scope_info_json", connection))
            {
                command.Parameters.Add(p);
                command.CommandType = CommandType.StoredProcedure;

                await connection.OpenAsync().ConfigureAwait(false);

                await command.ExecuteNonQueryAsync();



                connection.Close();

            }
        }
        catch (Exception)
        {

            throw;
        }

    }



    private static async Task TestNpgsqlAsync()
    {
        var commandText = $"SELECT * " +
               " FROM information_schema.tables " +
               " WHERE table_type = 'BASE TABLE' " +
               " AND table_schema != 'pg_catalog' AND table_schema != 'information_schema' " +
               " AND table_name=@tableName AND table_schema=@schemaName " +
               " ORDER BY table_schema, table_name " +
               " LIMIT 1";

        var schemaNameString = "public";
        var tableNameString = "actor";

        var connection = new NpgsqlConnection(DBHelper.GetNpgsqlDatabaseConnectionString(clientDbName));

        using (var command = new NpgsqlCommand(commandText, connection))
        {
            command.Parameters.AddWithValue("@tableName", tableNameString);
            command.Parameters.AddWithValue("@schemaName", schemaNameString);

            await connection.OpenAsync().ConfigureAwait(false);

            using (var reader = await command.ExecuteReaderAsync().ConfigureAwait(false))
            {
                while (reader.Read())
                {
                    Console.WriteLine(reader["table_schema"].ToString() + "." + reader["table_name"].ToString());
                }
            }

            connection.Close();

        }
    }


    private static async Task LongTransactionBeforeRuningSyncAsync()
    {
        // Create 2 Sql Sync providers
        var serverProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(serverDbName));
        var clientProvider = new SqliteSyncProvider("longrunning.sqlite");

        // Create standard Setup and Options
        var setup = new SyncSetup(new string[] { "Address", "Customer", "CustomerAddress" });
        var options = new SyncOptions();

        // Creating an agent that will handle all the process
        var agent = new SyncAgent(clientProvider, serverProvider, options, setup);

        // First Sync
        // ----------------------
        Console.WriteLine("FIRST INITIALIZE SYNC");
        Console.WriteLine("--------------------");
        var s1 = await agent.SynchronizeAsync();
        Console.WriteLine(s1);

        // Second Sync
        // ----------------------
        Console.WriteLine();
        Console.WriteLine("SECOND PARALLEL SYNC");
        Console.WriteLine("--------------------");

        agent.LocalOrchestrator.OnDatabaseChangesSelecting(dcsa =>
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"[{DateTime.Now:ss.ms}] - Last Sync TS start (T0) : " + dcsa.ChangesRequest.LastTimestamp.ToString().Substring(dcsa.ChangesRequest.LastTimestamp.ToString().Length - 4, 4));
            Console.ResetColor();
            //await InsertIntoSqliteWithCurrentTransactionAsync(dcsa.Connection, dcsa.Transaction);
        });

        agent.LocalOrchestrator.OnDatabaseChangesSelected(dcsa =>
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"[{DateTime.Now:ss.ms}] - Last Sync TS for next run (T10) : " + dcsa.Timestamp.ToString().Substring(dcsa.Timestamp.ToString().Length - 4, 4));
            Console.ResetColor();
        });


        // Using the Progress pattern to handle progession during the synchronization
        var progress = new SynchronousProgress<ProgressArgs>(s =>
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"[{DateTime.Now:ss.ms}] - {s.Context.SyncStage}:\t{s.Message}");
            Console.ResetColor();
        });

        var t1 = LongInsertIntoSqliteAsync(clientProvider.CreateConnection());
        var t2 = agent.SynchronizeAsync(progress);

        await Task.WhenAll(t1, t2);
        Console.WriteLine(t2.Result);

        Console.WriteLine();
        Console.WriteLine("THIRD FINAL SYNC");
        Console.WriteLine("--------------------");

        agent.LocalOrchestrator.OnDatabaseChangesSelecting(null);
        agent.LocalOrchestrator.OnDatabaseChangesSelected(null);
        // Third Sync
        // ----------------------
        // First Sync
        var s3 = await agent.SynchronizeAsync();
        Console.WriteLine(s3);

    }



    private static async Task LongInsertIntoSqliteAsync(DbConnection connection)
    {
        var addressline1 = Path.GetRandomFileName().Replace(".", "").ToLowerInvariant();

        var command = connection.CreateCommand();
        command.CommandText =
            $"Insert into Address (AddressLine1, City, StateProvince, CountryRegion, PostalCode, rowguid, ModifiedDate) Values ('{addressline1}', 'Toulouse', 'Haute Garonne', 'Occitanie', '31000', '{Guid.NewGuid()}', '2020-02-02');" +
            $"Select timestamp from Address_tracking where AddressID = (Select AddressID from Address where AddressLine1 = '{addressline1}')";

        connection.Open();

        using (var transaction = connection.BeginTransaction())
        {
            command.Transaction = transaction;
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"[{DateTime.Now:ss.ms}] - Inserting row ");
            var ts = await command.ExecuteScalarAsync();
            Console.WriteLine($"[{DateTime.Now:ss.ms}] - SQLite Timestamp inserted (T9) : " + ts.ToString().Substring(ts.ToString().Length - 4, 4));

            Console.WriteLine($"[{DateTime.Now:ss.ms}] - Waiting 10 sec before commit");
            await Task.Delay(10000);

            transaction.Commit();
            Console.WriteLine($"[{DateTime.Now:ss.ms}] - Transaction commit");
            Console.ResetColor();

        }
            connection.Close();
    }

    private static async Task InsertIntoSqliteWithCurrentTransactionAsync(DbConnection connection, DbTransaction transaction)
    {


        try
        {
            var addressline1 = Path.GetRandomFileName().Replace(".", "").ToLowerInvariant();

            var command = connection.CreateCommand();
            command.CommandText =
                $"Insert into Address (AddressLine1, City, StateProvince, CountryRegion, PostalCode, rowguid, ModifiedDate) Values ('{addressline1}', 'Toulouse', 'Haute Garonne', 'Occitanie', '31000', '{Guid.NewGuid()}', '2020-02-02');" +
                $"Select timestamp from Address_tracking where AddressID = (Select AddressID from Address where AddressLine1 = '{addressline1}')";

            command.Transaction = transaction;

            //using (var transaction = connection.BeginTransaction())
            //{
            //command.Transaction = transaction;
            Console.WriteLine("Inserting row ");

            var ts = await command.ExecuteScalarAsync();

            //await Task.Delay(10000);

            //transaction.Commit();
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine("SQLite Timestamp inserted: " + ts.ToString().Substring(ts.ToString().Length - 4, 4));
            Console.ResetColor();

            //}
            //connection.Close();
        }
        catch (Exception)
        {

            throw;
    }

    }


    private static async Task SynchronizeThenDeprovisionThenProvisionAsync()
    {
        // Create 2 Sql Sync providers
        var serverProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(serverDbName));
        var clientProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(clientDbName));

        // Create standard Setup and Options
        var setup = new SyncSetup(new string[] { "Address", "Customer", "CustomerAddress" });
        var options = new SyncOptions();

        // Creating an agent that will handle all the process
        var agent = new SyncAgent(clientProvider, serverProvider, options, setup);

        // Using the Progress pattern to handle progession during the synchronization
        var progress = new SynchronousProgress<ProgressArgs>(s => Console.WriteLine($"{s.Context.SyncStage}:\t{s.Message}"));

        // First sync to have a starting point
        var s1 = await agent.SynchronizeAsync(progress);

        Console.WriteLine(s1);

        // -----------------------------------------------------------------
        // Migrating a table by adding a new column
        // -----------------------------------------------------------------

        // Adding a new column called CreatedDate to Address table, on the server, and on the client.
        await AddNewColumnToAddressAsync(serverProvider.CreateConnection());
        await AddNewColumnToAddressAsync(clientProvider.CreateConnection());

        // -----------------------------------------------------------------
        // Server side
        // -----------------------------------------------------------------

        // Creating a setup regarding only the table Address
        var setupAddress = new SyncSetup(new string[] { "Address" });

        // Create a server orchestrator used to Deprovision and Provision only table Address
        var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options, setupAddress);

        // Unprovision the Address triggers / stored proc. 
        // We can conserve the Address tracking table, since we just add a column, 
        // that is not a primary key used in the tracking table
        // That way, we are preserving historical data
        await remoteOrchestrator.DeprovisionAsync(SyncProvision.StoredProcedures | SyncProvision.Triggers);

        // Provision the Address triggers / stored proc again, 
        // This provision method will fetch the address schema from the database, 
        // so it will contains all the columns, including the new Address column added
        await remoteOrchestrator.ProvisionAsync(SyncProvision.StoredProcedures | SyncProvision.Triggers);

        // Now we need the full setup to get the full schema.
        // Setup includes [Address] [Customer] and [CustomerAddress]
        remoteOrchestrator.Setup = setup;
        var newSchema = await remoteOrchestrator.GetSchemaAsync();

        // Now we need to save this new schema to the serverscope table
        // get the server scope again
        var serverScope = await remoteOrchestrator.GetServerScopeAsync();

        // affect good values
        serverScope.Setup = setup;
        serverScope.Schema = newSchema;

        // save it
        await remoteOrchestrator.WriteServerScopeAsync(serverScope);

        // -----------------------------------------------------------------
        // Client side
        // -----------------------------------------------------------------

        // Now go for local orchestrator
        var localOrchestrator = new LocalOrchestrator(clientProvider, options, setupAddress);

        // Unprovision the Address triggers / stored proc. We can conserve tracking table, since we just add a column, that is not a primary key used in the tracking table
        // In this case, we will 
        await localOrchestrator.DeprovisionAsync(SyncProvision.StoredProcedures | SyncProvision.Triggers);

        // Provision the Address triggers / stored proc again, 
        // This provision method will fetch the address schema from the database, so it will contains all the columns, including the new one added
        await localOrchestrator.ProvisionAsync(SyncProvision.StoredProcedures | SyncProvision.Triggers);

        // Now we need to save this to clientscope
        // get the server scope again
        var clientScope = await localOrchestrator.GetClientScopeAsync();

        // At this point, if you need the schema and you are not able to create a RemoteOrchestrator,
        // You can create a WebClientOrchestrator and get the schema as well
        // var proxyClientProvider = new WebClientOrchestrator("https://localhost:44369/api/Sync");
        // var newSchema = proxyClientProvider.GetSchemaAsync();

        // affect good values
        clientScope.Setup = setup;
        clientScope.Schema = newSchema;

        // save it
        await localOrchestrator.WriteClientScopeAsync(clientScope);





        // Now test a new sync, everything should work as expected.
        do
        {
            // Console.Clear();
            Console.WriteLine("Sync Start");
            try
            {
                var s2 = await agent.SynchronizeAsync();

                // Write results
                Console.WriteLine(s2);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }

        } while (Console.ReadKey().Key != ConsoleKey.Escape);

        Console.WriteLine("End");

    }

    private static void TestSqliteDoubleStatement()
    {
        var clientProvider = new SqliteSyncProvider(@"C:\PROJECTS\DOTMIM.SYNC\Tests\Dotmim.Sync.Tests\bin\Debug\netcoreapp2.0\st_r55jmmolvwg.db");
        var clientConnection = new SqliteConnection(clientProvider.ConnectionString);

        var commandText = "Update ProductCategory Set Name=@Name Where ProductCategoryId=@Id; " +
                          "Select * from ProductCategory Where ProductCategoryId=@Id;";

        using (DbCommand command = clientConnection.CreateCommand())
        {
            command.Connection = clientConnection;
            command.CommandText = commandText;
            var p = command.CreateParameter();
            p.ParameterName = "@Id";
            p.DbType = DbType.String;
            p.Value = "FTNLBJ";
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@Name";
            p.DbType = DbType.String;
            p.Value = "Awesome Bike";
            command.Parameters.Add(p);

            clientConnection.Open();

            using (var reader = command.ExecuteReader())
            {
                while (reader.Read())
                {
                    var name = reader["Name"];
                    Console.WriteLine(name);
                }
            }

            clientConnection.Close();
        }

    }
    private static async Task TestDeleteWithoutBulkAsync()
    {
        var cs = DBHelper.GetDatabaseConnectionString(serverProductCategoryDbName);
        var cc = DBHelper.GetDatabaseConnectionString(clientDbName);

        // Create 2 Sql Sync providers
        var serverProvider = new SqlSyncProvider(cs);
        var serverConnection = new SqlConnection(cs);

        //var clientProvider = new SqlSyncProvider(cc);
        //var clientConnection = new SqlConnection(cc);

        var clientProvider = new SqliteSyncProvider("advworks2.db");
        var clientConnection = new SqliteConnection(clientProvider.ConnectionString);

        // Creating an agent that will handle all the process
        var agent = new SyncAgent(clientProvider, serverProvider, new string[] { "ProductCategory" });

        // Using the Progress pattern to handle progession during the synchronization
        var progress = new SynchronousProgress<ProgressArgs>(s =>
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"{s.Context.SyncStage}:\t{s.Message}");
            Console.ResetColor();
        });

        var remoteProgress = new SynchronousProgress<ProgressArgs>(s =>
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"{s.Context.SyncStage}:\t{s.Message}");
            Console.ResetColor();
        });
        // agent.AddRemoteProgress(remoteProgress);

        agent.Options.BatchDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDiretory(), "sync");
        agent.Options.BatchSize = 0;
        agent.Options.UseBulkOperations = false;
        agent.Options.DisableConstraintsOnApplyChanges = false;
        agent.Options.ConflictResolutionPolicy = ConflictResolutionPolicy.ClientWins;

        Console.WriteLine("Sync Start");
        var s1 = await agent.SynchronizeAsync(progress);
        Console.WriteLine(s1);

        Console.WriteLine("Insert product category on Server");
        var id = InsertOneProductCategoryId(serverConnection, "GLASSES");
        Console.WriteLine("Update Done.");

        Console.WriteLine("Update product category on Server");
        UpdateOneProductCategoryId(serverConnection, id, "OVERGLASSES");
        Console.WriteLine("Update Done.");

        Console.WriteLine("Sync Start");
        s1 = await agent.SynchronizeAsync(progress);
        Console.WriteLine(s1);

        Console.WriteLine("End");
    }

    private static async Task AddNewColumnToAddressAsync(DbConnection c)
    {
        using (var command = c.CreateCommand())
        {
            command.CommandText = "ALTER TABLE dbo.Address ADD CreatedDate datetime NULL;";
            c.Open();
            await command.ExecuteNonQueryAsync();
            c.Close();
        }
    }
    private static int InsertOneProductCategoryId(IDbConnection c, string updatedName)
    {
        using (var command = c.CreateCommand())
        {
            command.CommandText = "Insert Into ProductCategory (Name) Values (@Name); SELECT SCOPE_IDENTITY();";
            var p = command.CreateParameter();
            p.DbType = DbType.String;
            p.Value = updatedName;
            p.ParameterName = "@Name";
            command.Parameters.Add(p);

            c.Open();
            var id = command.ExecuteScalar();
            c.Close();

            return Convert.ToInt32(id);
        }
    }
    private static void UpdateOneProductCategoryId(IDbConnection c, int productCategoryId, string updatedName)
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

            c.Open();
            command.ExecuteNonQuery();
            c.Close();
        }
    }
    private static void DeleteOneLine(DbConnection c, int productCategoryId)
    {
        using (var command = c.CreateCommand())
        {
            command.CommandText = "Delete from ProductCategory Where ProductCategoryId = @Id";

            var p = command.CreateParameter();
            p.DbType = DbType.Int32;
            p.Value = productCategoryId;
            p.ParameterName = "@Id";
            command.Parameters.Add(p);

            c.Open();
            command.ExecuteNonQuery();
            c.Close();
        }
    }

    private static async Task CreateSnapshotAsync()
    {
        // Create 2 Sql Sync providers
        var serverProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(serverDbName));

        // specific Setup with only 2 tables, and one filtered
        var setup = new SyncSetup(allTables);

        // Using the Progress pattern to handle progession during the synchronization
        var progress = new SynchronousProgress<ProgressArgs>(s =>
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"{s.Context.SyncStage}:\t{s.Message}");
            Console.ResetColor();
        });

        var remoteProgress = new SynchronousProgress<ProgressArgs>(s =>
        {
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine($"{s.Context.SyncStage}:\t{s.Message}");
            Console.ResetColor();
        });

        // snapshot directory
        var directory = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), "Snapshots");

        var options = new SyncOptions
        {
            SnapshotsDirectory = directory,
            BatchSize = 2000
        };

        var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options, setup);

        await remoteOrchestrator.CreateSnapshotAsync(null, default, remoteProgress);
        // client provider
        var clientProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(clientDbName));

        // Insert a value after snapshot created
        using (var c = serverProvider.CreateConnection())
        {
            var command = c.CreateCommand();
            command.CommandText = "INSERT INTO [dbo].[ProductCategory] ([Name]) VALUES ('Bikes revolution');";
            c.Open();
            command.ExecuteNonQuery();
            c.Close();
        }

        var syncOptions = new SyncOptions { SnapshotsDirectory = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), "Snapshots") };

        // Creating an agent that will handle all the process
        var agent = new SyncAgent(clientProvider, serverProvider, syncOptions, setup);



        //// Launch the sync process
        //if (!agent.Parameters.Contains("City"))
        //    agent.Parameters.Add("City", "Toronto");

        //if (!agent.Parameters.Contains("postal"))
        //    agent.Parameters.Add("postal", "NULL");



        do
        {
            Console.Clear();
            Console.WriteLine("Sync Start");
            try
            {
                var s1 = await agent.SynchronizeAsync(SyncType.Reinitialize, progress);
                Console.WriteLine(s1);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
        } while (Console.ReadKey().Key != ConsoleKey.Escape);

        Console.WriteLine("End");
    }

    private static async Task SynchronizeMultiScopesAsync()
    {
        // Create 2 Sql Sync providers
        var serverProvider = new SqlSyncChangeTrackingProvider(DBHelper.GetDatabaseConnectionString(serverDbName));
        var clientProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(clientDbName));

        // Create 2 tables list (one for each scope)
        string[] productScopeTables = new string[] { "ProductCategory", "ProductModel", "Product" };
        string[] customersScopeTables = new string[] { "Address", "Customer", "CustomerAddress", "SalesOrderHeader", "SalesOrderDetail" };

        // Create 2 sync setup with named scope 
        var setupProducts = new SyncSetup(productScopeTables);
        var setupCustomers = new SyncSetup(customersScopeTables);

        var syncOptions = new SyncOptions();

        // Create 2 agents, one for each scope
        var agentProducts = new SyncAgent(clientProvider, serverProvider, syncOptions, setupProducts, "productScope");
        var agentCustomers = new SyncAgent(clientProvider, serverProvider, syncOptions, setupCustomers, "customerScope");

        // Using the Progress pattern to handle progession during the synchronization
        // We can use the same progress for each agent
        var progress = new SynchronousProgress<ProgressArgs>(s =>
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"{s.Context.SyncStage}:\t{s.Message}");
            Console.ResetColor();
        });

        var remoteProgress = new SynchronousProgress<ProgressArgs>(s =>
        {
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine($"{s.Context.SyncStage}:\t{s.Message}");
            Console.ResetColor();
        });

        // Spying what's going on the server side
        agentProducts.AddRemoteProgress(remoteProgress);
        agentCustomers.AddRemoteProgress(remoteProgress);


        do
        {
            Console.Clear();
            Console.WriteLine("Sync Start");
            try
            {
                Console.WriteLine("Hit 1 for sync Products. Hit 2 for sync customers and sales");
                var k = Console.ReadKey().Key;

                if (k == ConsoleKey.D1)
                {
                    Console.WriteLine("Sync Products:");
                    var s1 = await agentProducts.SynchronizeAsync(progress);
                    Console.WriteLine(s1);
                }
                else
                {
                    Console.WriteLine("Sync Customers and Sales:");
                    var s1 = await agentCustomers.SynchronizeAsync(progress);
                    Console.WriteLine(s1);
                }

            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
        } while (Console.ReadKey().Key != ConsoleKey.Escape);

        Console.WriteLine("End");
    }


    private static async Task TestAvbAsync()
    {
        var serverProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString("TestAvg"));
        var clientProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(clientDbName));

        var setup = new SyncSetup(new String[] { "avb.avb", "avb.avbg" });

        // filter on table avb.avbg
        var avbGFilter = new SetupFilter("avbg", "avb");

        // add parameter from table avb.avb Column avb.avb.UserId
        avbGFilter.AddParameter("UserId", "avb", "avb");

        // join to table avb.avbg
        avbGFilter.AddJoin(Join.Left, "avb.avb").On("avb.avb", "Id", "avb.avbg", "avbId");
        avbGFilter.AddWhere("UserId", "avb", "UserId", "avb");

        setup.Filters.Add(avbGFilter);

        // Creating an agent that will handle all the process
        var agent = new SyncAgent(clientProvider, serverProvider, new SyncOptions(), setup);

        // Using the Progress pattern to handle progession during the synchronization
        var progress = new SynchronousProgress<ProgressArgs>(s =>
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"{s.Context.SyncStage}:\t{s.Message}");
            Console.ResetColor();
        });

        Guid userId = new Guid("a7ca90ef-f6b1-4a19-9e31-01a215abbb95");

        agent.Parameters.Add("UserId", userId);

        var s1 = await agent.SynchronizeAsync(progress);

        Console.WriteLine(s1);
    }

    private static async Task SynchronizeWithFiltersAsync()
    {
        // Create 2 Sql Sync providers
        var serverProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(serverDbName));
        var clientProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(clientDbName));
        //var clientProvider = new SqliteSyncProvider("clientX.db");

        var setup = new SyncSetup(new string[] { "Address", "Customer", "CustomerAddress", "SalesOrderHeader", "SalesOrderDetail" });

        setup.Filters.Add("Customer", "CompanyName");

        var addressCustomerFilter = new SetupFilter("CustomerAddress");
        addressCustomerFilter.AddParameter("CompanyName", "Customer");
        addressCustomerFilter.AddJoin(Join.Left, "Customer").On("CustomerAddress", "CustomerId", "Customer", "CustomerId");
        addressCustomerFilter.AddWhere("CompanyName", "Customer", "CompanyName");
        setup.Filters.Add(addressCustomerFilter);

        var addressFilter = new SetupFilter("Address");
        addressFilter.AddParameter("CompanyName", "Customer");
        addressFilter.AddJoin(Join.Left, "CustomerAddress").On("CustomerAddress", "AddressId", "Address", "AddressId");
        addressFilter.AddJoin(Join.Left, "Customer").On("CustomerAddress", "CustomerId", "Customer", "CustomerId");
        addressFilter.AddWhere("CompanyName", "Customer", "CompanyName");
        setup.Filters.Add(addressFilter);

        var orderHeaderFilter = new SetupFilter("SalesOrderHeader");
        orderHeaderFilter.AddParameter("CompanyName", "Customer");
        orderHeaderFilter.AddJoin(Join.Left, "CustomerAddress").On("CustomerAddress", "CustomerId", "SalesOrderHeader", "CustomerId");
        orderHeaderFilter.AddJoin(Join.Left, "Customer").On("CustomerAddress", "CustomerId", "Customer", "CustomerId");
        orderHeaderFilter.AddWhere("CompanyName", "Customer", "CompanyName");
        setup.Filters.Add(orderHeaderFilter);

        var orderDetailsFilter = new SetupFilter("SalesOrderDetail");
        orderDetailsFilter.AddParameter("CompanyName", "Customer");
        orderDetailsFilter.AddJoin(Join.Left, "SalesOrderHeader").On("SalesOrderDetail", "SalesOrderID", "SalesOrderHeader", "SalesOrderID");
        orderDetailsFilter.AddJoin(Join.Left, "CustomerAddress").On("CustomerAddress", "CustomerId", "SalesOrderHeader", "CustomerId");
        orderDetailsFilter.AddJoin(Join.Left, "Customer").On("CustomerAddress", "CustomerId", "Customer", "CustomerId");
        orderDetailsFilter.AddWhere("CompanyName", "Customer", "CompanyName");
        setup.Filters.Add(orderDetailsFilter);

        // Add pref suf
        //setup.StoredProceduresPrefix = "s";
        //setup.StoredProceduresSuffix = "";
        //setup.TrackingTablesPrefix = "t";
        //setup.TrackingTablesSuffix = "";

        var options = new SyncOptions();

        // Creating an agent that will handle all the process
        var agent = new SyncAgent(clientProvider, serverProvider, options, setup);

        // Using the Progress pattern to handle progession during the synchronization
        var progress = new SynchronousProgress<ProgressArgs>(s =>
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"{s.Context.SyncStage}:\t{s.Message}");
            Console.ResetColor();
        });

        do
        {
            // Console.Clear();
            Console.WriteLine("Sync Start");
            try
            {
                // Launch the sync process
                if (!agent.Parameters.Contains("CompanyName"))
                    agent.Parameters.Add("CompanyName", "Professional Sales and Service");

                var s1 = await agent.SynchronizeAsync(SyncType.Reinitialize);

                // Write results
                Console.WriteLine(s1);

            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }


            //Console.WriteLine("Sync Ended. Press a key to start again, or Escapte to end");
        } while (Console.ReadKey().Key != ConsoleKey.Escape);

        Console.WriteLine("End");
    }

    private static async Task SynchronizeWithLoggerAsync()
    {
        // Create 2 Sql Sync providers
        var serverProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(serverDbName));
        var clientProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(clientDbName));
        //var clientProvider = new SqliteSyncProvider("clientX.db");

        var setup = new SyncSetup(new string[] { "Address", "Customer", "CustomerAddress", "SalesOrderHeader", "SalesOrderDetail" });
        //var setup = new SyncSetup(new string[] { "Customer" });
        //var setup = new SyncSetup(new[] { "Customer" });
        //setup.Tables["Customer"].Columns.AddRange(new[] { "CustomerID", "FirstName", "LastName" });


        var options = new SyncOptions();
        options.BatchSize = 500;


        //Log.Logger = new LoggerConfiguration()
        //    .Enrich.FromLogContext()
        //    .MinimumLevel.Verbose()
        //    .MinimumLevel.Override("Microsoft", LogEventLevel.Warning)
        //    .WriteTo.Console()
        //    .CreateLogger();

        // 1) create a console logger
        //var loggerFactory = LoggerFactory.Create(builder => { builder.AddConsole().SetMinimumLevel(LogLevel.Trace); });
        //var logger = loggerFactory.CreateLogger("Dotmim.Sync");
        //options.Logger = logger;


        // 1) create a serilog logger
        //var loggerFactory = LoggerFactory.Create(builder => { builder.AddSerilog().SetMinimumLevel(LogLevel.Trace); });
        //var logger = loggerFactory.CreateLogger("SyncAgent");
        //options.Logger = logger;

        //3) Using Serilog with Seq
        //var serilogLogger = new LoggerConfiguration()
        //    .Enrich.FromLogContext()
        //    .MinimumLevel.Debug()
        //    .MinimumLevel.Override("Microsoft", LogEventLevel.Warning)
        //    .WriteTo.Seq("http://localhost:5341")
        //    .WriteTo.Console()
        //    .CreateLogger();


        //var actLogging = new Action<SyncLoggerOptions>(slo =>
        //{
        //    slo.AddConsole();
        //    slo.SetMinimumLevel(LogLevel.Information);
        //});

        ////var loggerFactory = LoggerFactory.Create(builder => builder.AddSerilog().AddConsole().SetMinimumLevel(LogLevel.Information));
        //var loggerFactory = LoggerFactory.Create(builder => builder.AddSerilog(serilogLogger));


        // loggerFactory.AddSerilog(serilogLogger);

        //options.Logger = loggerFactory.CreateLogger("dms");

        // 2nd option to add serilog
        //var loggerFactorySerilog = new SerilogLoggerFactory();
        //var logger = loggerFactorySerilog.CreateLogger<SyncAgent>();
        //options.Logger = logger;

        //options.Logger = new SyncLogger().AddConsole().AddDebug().SetMinimumLevel(LogLevel.Trace);

        //var snapshotDirectory = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), "Snapshots");
        //options.BatchSize = 500;
        //options.SnapshotsDirectory = snapshotDirectory;
        //var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options, setup);
        //remoteOrchestrator.CreateSnapshotAsync().GetAwaiter().GetResult();


        // Creating an agent that will handle all the process
        var agent = new SyncAgent(clientProvider, serverProvider, options, setup);

        // Using the Progress pattern to handle progession during the synchronization
        //var progress = new SynchronousProgress<ProgressArgs>(s =>
        //{
        //    Console.ForegroundColor = ConsoleColor.Green;
        //    Console.WriteLine($"{s.Context.SyncStage}:\t{s.Message}");
        //    Console.ResetColor();
        //});

        //var remoteProgress = new SynchronousProgress<ProgressArgs>(s =>
        //{
        //    Console.ForegroundColor = ConsoleColor.Yellow;
        //    Console.WriteLine($"{s.Context.SyncStage}:\t{s.Message}");
        //    Console.ResetColor();
        //});


        //agent.AddRemoteProgress(remoteProgress);

        //agent.Options.BatchDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDiretory(), "sync");
        // agent.Options.BatchSize = 1000;
        agent.Options.CleanMetadatas = true;
        agent.Options.UseBulkOperations = true;
        agent.Options.DisableConstraintsOnApplyChanges = true;
        //agent.Options.ConflictResolutionPolicy = ConflictResolutionPolicy.ClientWins;
        //agent.Options.UseVerboseErrors = false;


        do
        {
            // Console.Clear();
            Console.WriteLine("Sync Start");
            try
            {
                // Launch the sync process
                //if (!agent.Parameters.Contains("CompanyName"))
                //    agent.Parameters.Add("CompanyName", "Professional Sales and Service");

                var s1 = await agent.SynchronizeAsync(SyncType.Reinitialize);

                // Get the server scope
                var serverScope = await agent.RemoteOrchestrator.GetServerScopeAsync();


                await agent.RemoteOrchestrator.DeprovisionAsync(SyncProvision.StoredProcedures
                                                                     | SyncProvision.Triggers | SyncProvision.TrackingTable);

                // Affect good values
                serverScope.Setup = null;
                serverScope.Schema = null;

                // save the server scope
                await agent.RemoteOrchestrator.WriteServerScopeAsync(serverScope);

                // Write results
                Console.WriteLine(s1);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }


            //Console.WriteLine("Sync Ended. Press a key to start again, or Escapte to end");
        } while (Console.ReadKey().Key != ConsoleKey.Escape);

        Console.WriteLine("End");
    }

    private static async Task SynchronizeAsync()
    {
        // Create 2 Sql Sync providers
        var serverProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(serverDbName));
        var clientProvider = new SqliteSyncProvider("longrunning.db");
        var setup = new SyncSetup(new string[] { "Address", "Customer", "CustomerAddress", "SalesOrderHeader", "SalesOrderDetail" });
        //var setup = new SyncSetup(new string[] { "Customer" });
        //var setup = new SyncSetup(new[] { "Customer" });
        //setup.Tables["Customer"].Columns.AddRange(new[] { "CustomerID", "FirstName", "LastName" });


        var options = new SyncOptions();
        // Creating an agent that will handle all the process
        var agent = new SyncAgent(clientProvider, serverProvider, options, allTables);

        // Using the Progress pattern to handle progession during the synchronization
        var progress = new SynchronousProgress<ProgressArgs>(s =>
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"{s.Context.SyncStage}:\t{s.Message}");
            Console.ResetColor();
        });

        do
        {
            // Console.Clear();
            Console.WriteLine("Sync Start");
            try
            {
                var r = await agent.SynchronizeAsync(progress);
                // Write results
                Console.WriteLine(r);

                //using (var sqliteConnection = new SqliteConnection(clientProvider.ConnectionString))
                //{
                //    using (var command = new SqliteCommand("Select * from Product", sqliteConnection))
                //    {
                //        sqliteConnection.Open();
                //        var reader = command.ExecuteReader();

                //        while (reader.Read())
                //        {
                //            var rowId = reader["rowguid"] != DBNull.Value? new Guid((string)reader["rowguid"]) : Guid.Empty;
                //        }
                //        sqliteConnection.Close();

                //    }
                //}
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }


            //Console.WriteLine("Sync Ended. Press a key to start again, or Escapte to end");
        } while (Console.ReadKey().Key != ConsoleKey.Escape);

        Console.WriteLine("End");
    }

    public static async Task SyncHttpThroughKestrellAsync()
    {
        // server provider
        // Create 2 Sql Sync providers
        var serverProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(serverDbName));
        var clientProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(clientDbName));

        // ----------------------------------
        // Client & Server side
        // ----------------------------------
        // snapshot directory
        var snapshotDirectory = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), "Snapshots");
        var options = new SyncOptions { BatchSize = 500, SnapshotsDirectory = snapshotDirectory };



        // ----------------------------------
        // Insert a value after snapshot created
        // ----------------------------------
        using (var c = serverProvider.CreateConnection())
        {
            var command = c.CreateCommand();
            command.CommandText = "INSERT INTO [dbo].[ProductCategory] ([Name]) VALUES ('Bikes revolution');";
            c.Open();
            command.ExecuteNonQuery();
            c.Close();
        }

        var configureServices = new Action<IServiceCollection>(services =>
        {
            // ----------------------------------
            // Web Server side
            // ----------------------------------
            var setup = new SyncSetup(new string[] { "ProductCategory", "Product" })
            {
                StoredProceduresPrefix = "s",
                StoredProceduresSuffix = "",
                TrackingTablesPrefix = "t",
                TrackingTablesSuffix = "",
                TriggersPrefix = "",
                TriggersSuffix = "t"
            };

            services.AddSyncServer<SqlSyncProvider>(serverProvider.ConnectionString, "dd", setup, options);

            // ----------------------------------
            // Create a snapshot
            // ----------------------------------
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine($"Creating snapshot");
            var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options, setup, "dd");
            remoteOrchestrator.CreateSnapshotAsync().GetAwaiter().GetResult();
            Console.WriteLine($"Done.");
            Console.ResetColor();

        });

        var serverHandler = new RequestDelegate(async context =>
        {
            var webServerManager = context.RequestServices.GetService(typeof(WebServerManager)) as WebServerManager;

            var progress = new SynchronousProgress<ProgressArgs>(pa =>
            {
                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.WriteLine($"{pa.Context.SyncStage}\t {pa.Message}");
                Console.ResetColor();
            });

            //// override sync type
            //var orch = webServerManager.GetOrchestrator(context);
            //orch.OnServerScopeLoaded(sla =>
            //{
            //    sla.Context.SyncType = SyncType.Reinitialize;
            //});

            await webServerManager.HandleRequestAsync(context, default, progress);
        });

        using (var server = new KestrellTestServer(configureServices))
        {
            var clientHandler = new ResponseDelegate(async (serviceUri) =>
            {
                do
                {
                    Console.Clear();
                    Console.WriteLine("Web sync start");
                    try
                    {
                        //var localSetup = new SyncSetup()
                        //{
                        //    StoredProceduresPrefix = "cli",
                        //    StoredProceduresSuffix = "",
                        //    TrackingTablesPrefix = "cli",
                        //    TrackingTablesSuffix = "",
                        //    TriggersPrefix = "cli",
                        //    TriggersSuffix = ""
                        //};
                        var agent = new SyncAgent(clientProvider, new WebClientOrchestrator(serviceUri), options, "dd");
                        var progress = new SynchronousProgress<ProgressArgs>(pa => Console.WriteLine($"{pa.Context.SyncStage}\t {pa.Message}"));
                        var s = await agent.SynchronizeAsync(progress);
                        Console.WriteLine(s);
                    }
                    catch (SyncException e)
                    {
                        Console.WriteLine(e.ToString());
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine("UNKNOW EXCEPTION : " + e.Message);
                    }


                    Console.WriteLine("Sync Ended. Press a key to start again, or Escapte to end");
                } while (Console.ReadKey().Key != ConsoleKey.Escape);


            });
            await server.Run(serverHandler, clientHandler);
        }

    }

    /// <summary>
    /// Testing client clean metadatas
    /// </summary>
    private static async Task CleanClientMetadatasAsync()
    {
        // Create the setup used for your sync process
        var tables = new string[] {"ProductCategory",
                    "ProductDescription", "ProductModel",
                    "Product", "ProductModelProductDescription",
                    "Address", "Customer", "CustomerAddress",
                    "SalesOrderHeader", "SalesOrderDetail" };

        var setup = new SyncSetup(tables)
        {
            // optional :
            StoredProceduresPrefix = "cli",
            StoredProceduresSuffix = "",
            TrackingTablesPrefix = "cli",
            TrackingTablesSuffix = ""
        };

        var syncOptions = new SyncOptions
        {
            ScopeInfoTableName = "client_scopeinfo"
        };

        var sqlSyncProvider = new SqlSyncProvider("Data Source=(localdb)\\mssqllocaldb; Initial Catalog=Client; Integrated Security=true;");
        var orchestrator = new LocalOrchestrator(sqlSyncProvider, syncOptions, setup);

        // delelete metadatas
        var dmd = await orchestrator.DeleteMetadatasAsync();

        Console.WriteLine(dmd);
    }

    /// <summary>
    /// Testing server clean metadatas
    /// </summary>
    private static async Task CleanServerMetadatasAsync()
    {
        // Create the setup used for your sync process
        var tables = new string[] {"ProductCategory",
                    "ProductDescription", "ProductModel",
                    "Product", "ProductModelProductDescription",
                    "Address", "Customer", "CustomerAddress",
                    "SalesOrderHeader", "SalesOrderDetail" };
        var setup = new SyncSetup(tables)
        {
            // optional :
            StoredProceduresPrefix = "server",
            StoredProceduresSuffix = "",
            TrackingTablesPrefix = "server",
            TrackingTablesSuffix = ""
        };

        var remoteSqlSyncProvider = new SqlSyncProvider("Data Source=(localdb)\\mssqllocaldb; Initial Catalog=AdventureWorks; Integrated Security=true;");
        var remoteOrchestrator = new RemoteOrchestrator(remoteSqlSyncProvider, new SyncOptions(), setup);

        // delelete metadatas
        var dmd = await remoteOrchestrator.DeleteMetadatasAsync();

        Console.WriteLine(dmd);
    }

    /// <summary>
    /// Test a client syncing through a web api
    /// </summary>
    private static async Task SyncThroughWebApiAsync()
    {
        var clientProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(clientDbName));

        var handler = new HttpClientHandler { AutomaticDecompression = DecompressionMethods.GZip };
        var client = new HttpClient(handler) { Timeout = TimeSpan.FromMinutes(5) };

        var proxyClientProvider = new WebClientOrchestrator("https://localhost:44369/api/Sync", null, null, client);

        // ----------------------------------
        // Client side
        // ----------------------------------
        var clientOptions = new SyncOptions
        {
            ScopeInfoTableName = "client_scopeinfo",
            BatchDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDiretory(), "sync_client"),
            BatchSize = 50,
            CleanMetadatas = true,
            UseBulkOperations = true,
            UseVerboseErrors = false,
        };

        var clientSetup = new SyncSetup
        {
            StoredProceduresPrefix = "cli",
            StoredProceduresSuffix = "",
            TrackingTablesPrefix = "cli",
            TrackingTablesSuffix = "",
            TriggersPrefix = "",
            TriggersSuffix = "",
        };



        var agent = new SyncAgent(clientProvider, proxyClientProvider, clientOptions, clientSetup);

        agent.Parameters.Add("CompanyName", "A Bike Store");

        Console.WriteLine("Press a key to start (be sure web api is running ...)");
        Console.ReadKey();
        do
        {
            Console.Clear();
            Console.WriteLine("Web sync start");
            try
            {
                var progress = new SynchronousProgress<ProgressArgs>(pa => Console.WriteLine($"{pa.Context.SessionId} - {pa.Context.SyncStage}\t {pa.Message}"));



                var serverSchema = await proxyClientProvider.GetSchemaAsync(default, progress);

                var serverScope = await proxyClientProvider.GetServerScopeAsync();



                // var s = await agent.SynchronizeAsync(progress);

                //Console.WriteLine(s);

            }
            catch (SyncException e)
            {
                Console.WriteLine(e.Message);
            }
            catch (Exception e)
            {
                Console.WriteLine("UNKNOW EXCEPTION : " + e.Message);
            }


            Console.WriteLine("Sync Ended. Press a key to start again, or Escapte to end");
        } while (Console.ReadKey().Key != ConsoleKey.Escape);

        Console.WriteLine("End");

    }


    private static async Task SynchronizeThenChangeSetupAsync()
    {
        // Create 2 Sql Sync providers
        var serverProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(serverDbName));
        //var clientProvider = new MySqlSyncProvider(DbHelper.GetMySqlDatabaseConnectionString(clientDbName));
        var clientProvider = new SqlSyncProvider(DBHelper.GetMySqlDatabaseConnectionString(clientDbName));
        //var clientProvider = new SqliteSyncProvider("client.db");

        var setup = new SyncSetup(new string[] { "Address", "Customer", "CustomerAddress", "SalesOrderHeader", "SalesOrderDetail", "BuildVersion" });
        // Add pref suf
        setup.StoredProceduresPrefix = "s";
        setup.StoredProceduresSuffix = "";
        setup.TrackingTablesPrefix = "t";
        setup.TrackingTablesSuffix = "";

        // Creating an agent that will handle all the process
        var agent = new SyncAgent(clientProvider, serverProvider, new SyncOptions(), setup);

        // Using the Progress pattern to handle progession during the synchronization
        var progress = new SynchronousProgress<ProgressArgs>(s =>
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"{s.Context.SyncStage}:\t{s.Message}");
            Console.ResetColor();
        });

        var remoteProgress = new SynchronousProgress<ProgressArgs>(s =>
        {
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine($"{s.Context.SyncStage}:\t{s.Message}");
            Console.ResetColor();
        });


        agent.AddRemoteProgress(remoteProgress);
        var s1 = await agent.SynchronizeAsync(progress);

        Console.WriteLine(s1);

        // Change setup
        setup.StoredProceduresPrefix = "dms";
        setup.TrackingTablesPrefix = "dms";
        setup.TriggersPrefix = "dms";
        setup.Tables.Clear();
        setup.Tables.Add("Product");

        //// Get scope_server
        //var scope_server = await agent.RemoteOrchestrator.GetServerScopeAsync();
        //var oldSetup = scope_server.Setup;

        //var newSchema = await agent.RemoteOrchestrator.GetSchemaAsync();

        //await agent.RemoteOrchestrator.MigrationAsync(newSchema, oldSetup, setup);
        //await agent.LocalOrchestrator.MigrationAsync(newSchema, oldSetup, setup);

        do
        {
            Console.Clear();
            Console.WriteLine("Sync Start");
            try
            {
                // Launch the sync process
                //if (!agent.Parameters.Contains("CompanyName"))
                //    agent.Parameters.Add("CompanyName", "Professional Sales and Service");

                var r1 = await agent.SynchronizeAsync(SyncType.Reinitialize, progress);

                // Write results
                Console.WriteLine(r1);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }


            //Console.WriteLine("Sync Ended. Press a key to start again, or Escapte to end");
        } while (Console.ReadKey().Key != ConsoleKey.Escape);

        Console.WriteLine("End");
    }

}

internal class scope
{
    public Guid sync_scope_id { get; set; }
    public string sync_scope_name { get; set; }
}