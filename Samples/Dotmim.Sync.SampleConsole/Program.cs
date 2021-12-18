using Dotmim.Sync;
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
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Serilog;
using Serilog.Events;
using System.Data.Common;
using Dotmim.Sync.MySql;
using System.Linq;
using Microsoft.Data.SqlClient;
using Dotmim.Sync.MariaDB;
#if NET5_0 || NET6_0
using MySqlConnector;
#elif NETSTANDARD
using MySql.Data.MySqlClient;
#endif

using System.Diagnostics;

internal class Program
{
    public static string serverDbName = "AdventureWorks";
    public static string serverProductCategoryDbName = "AdventureWorksProductCategory";
    public static string clientDbName = "Client";
    public static string[] allTables = new string[] {"ProductDescription", "ProductCategory",
                                                    "ProductModel", "Product",
                                                    "Address", "Customer", "CustomerAddress",
                                                    "SalesOrderHeader", "SalesOrderDetail" };


    public static string[] regipro_tables = new string[] {
        "actualites", "analytics_exchange", "analytics_group", "analytics_group_clients",
        "analytics_group_fournisseurs", "analytics_publishing", "analytics_turnovers",
        "annonces", "annonces_fixed_price", "annonces_freeze", "annonces_ipinfos", "annonces_metadatas",
        "api", "api_lesechos", "api_request", "api_tmp", "attestations", "attestations_benchmarks",
        "attestations_signed", "avocats", "bank", "bank_banned", "bank_codes", "bank_expenses", "bank_paypal", "bank_stripe", "borne_status",
        "cache_db", "certifications", "certifications_users",
        "clients", "clients_comments", "clients_metas", "clients_schemas", "clients_test",
        "codevoie_infogreffe", "commandes", "commandes_certification", "commandes_locked", "commandes_relances", "commandes_remises_client",
        "commandes_remises_fournisseur", "commandes_translated", "comptabilite_ecritures", "comptabilite_errors", "comptabilite_etats",
        "comptabilite_etats_rapprochements", "comptabilite_exports", "comptabilite_params", "comptabilite_pieces", "comptabilite_rapprochements",
        "departements", "devis", "dnid", "documents",
        "entreprises",
        "exchange", "exchange2", "exchange_turnovers", "expedition_freeze",
        "export_checkpoint", "facturation_freeze", "forecast", "forfaits", "formalites", "formalites_responses", "formalites_workflows", "fournisseurs",
        "greffes", "greffes_infogreffe", "http_auth", "huissiers", "iban", "infogreffe_unittests", "journaux", "journaux_dates_special", "journaux_favoris",
        "journaux_fermeture", "journaux_freeze", "journaux_partenariats", "journaux_price_variations", "justificatifs", "justificatifs_numeriques",
        "justificatifs_pages",
        "justificatifs_papier", "justificatifs_papier_events", "medialex_api_logs", "numerotation", "open_data_bodacc", "paiements",
        "paiements_configurations", "passwords_requests",
        "prolegal_sessions",
        "prospects_convert", "provisions",
        "regiepro_sessions",
        "reglements",
        "relances_clients", "releves_clients", "releves_clients_pieces", "releves_com_clients", "releves_fournisseurs", "releves_fournisseurs_pieces",
        "releves_temp", "releves_temp_pieces", "repartitions", "robot_drafts", "robot_publications", "robot_translate", "sent_emails", "sent_emails_events",
        "sent_emails_events_metas", "sepas_order", "sip_events", "tribunaux_commerce", "tribunaux_grande_instance",

        //"users_sessions", -------------------------------

        "utilisateurs",
        "values_compare",
        "villes",

        //"villes_infogreffe", ----------------------------

        "zipcode_error"
    };
    public static string[] oneTable = new string[] { "ProductCategory" };


    private static async Task Main(string[] args)
    {
        //var serverProvider = new MariaDBSyncProvider(DBHelper.GetConnectionString("RegieProStaging"));
        //var clientProvider = new MariaDBSyncDownloadOnlyProvider(DBHelper.GetMariadbDatabaseConnectionString("Client2"));
        //var setup = new SyncSetup(regipro_tables)
        //{
        //    TrackingTablesPrefix = "_sync_",
        //    TrackingTablesSuffix = ""
        //};
        //var snapshotDirectory = Path.Combine("C:\\Tmp\\Snapshots");
        //var options = new SyncOptions() { SnapshotsDirectory = snapshotDirectory };

        var serverProvider = new SqlSyncChangeTrackingProvider(DBHelper.GetDatabaseConnectionString(serverDbName));
        //var clientDatabaseName = Path.GetRandomFileName().Replace(".", "").ToLowerInvariant() + ".db";
        //var clientProvider = new SqliteSyncProvider(clientDatabaseName);

        var clientProvider = new SqlSyncChangeTrackingProvider(DBHelper.GetDatabaseConnectionString(clientDbName));
        var setup = new SyncSetup(new string[] { "ProductCategory" });
        var options = new SyncOptions() { ProgressLevel = SyncProgressLevel.Information };

        //setup.Tables["ProductCategory"].Columns.AddRange(new string[] { "ProductCategoryID", "ParentProductCategoryID", "Name" });
        //setup.Tables["ProductDescription"].Columns.AddRange(new string[] { "ProductDescriptionID", "Description" });
        //setup.Filters.Add("ProductCategory", "ParentProductCategoryID", null, true);

        //var serverProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString("RegiePro"));
        //var clientProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString("Client"));
        //var setup = new SyncSetup(regipro_tables);
        //var options = new SyncOptions() { BatchSize = 10000, ProgressLevel = SyncProgressLevel.Information };

        options.SnapshotsDirectory = Path.Combine("C:\\Tmp\\Snapshots");

        //await GetChangesAsync(clientProvider, serverProvider, setup, options);
        //await ProvisionAsync(serverProvider, setup, options);
        await CreateSnapshotAsync(serverProvider, setup, options);
        await SynchronizeAsync(clientProvider, serverProvider, setup, options);
        //await SyncHttpThroughKestrellAsync(clientProvider, serverProvider, setup, options);



    }

    private static async Task GetChangesAsync(CoreProvider clientProvider, CoreProvider serverProvider, SyncSetup setup, SyncOptions options)
    {
        //var options = new SyncOptions();
        // Using the Progress pattern to handle progession during the synchronization
        var progress = new SynchronousProgress<ProgressArgs>(s =>
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"{s.ProgressPercentage:p}:\t{s.Source}:\t{s.Message}");
            Console.ResetColor();
        });

        do
        {
            Console.WriteLine("Sync start");
            try
            {
                Stopwatch stopw = new Stopwatch();
                stopw.Start();

                // Creating an agent that will handle all the process
                var agent = new SyncAgent(clientProvider, serverProvider, options, setup);

                var localScope = new ScopeInfo { Name = SyncOptions.DefaultScopeName, Id = Guid.NewGuid(), IsNewScope = true };
                var s = await agent.RemoteOrchestrator.GetChangesAsync(localScope);
                Console.WriteLine(s);
                s.ServerBatchInfo.Clear(true);

                stopw.Stop();
                Console.WriteLine($"Total duration :{stopw.Elapsed:hh\\.mm\\:ss\\.fff}");
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

    }


    private static async Task SynchronizeAsync(CoreProvider clientProvider, CoreProvider serverProvider, SyncSetup setup, SyncOptions options)
    {
        //var options = new SyncOptions();
        // Using the Progress pattern to handle progession during the synchronization
        var progress = new SynchronousProgress<ProgressArgs>(s =>
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"{s.ProgressPercentage:p}:  \t[{s.Source[..Math.Min(4, s.Source.Length)]}] {s.TypeName}:\t{s.Message}");
            Console.ResetColor();
        });


        // Creating an agent that will handle all the process
        var agent = new SyncAgent(clientProvider, serverProvider, options, setup);


        // This event is raised before selecting the changes for a particular table
        // you still can change the DbCommand generated, if you need to
        agent.RemoteOrchestrator.OnTableChangesSelecting(tcsa =>
        {
            Console.WriteLine($"Table {tcsa.SchemaTable.GetFullName()}: " +
                $"Selecting rows from datasource {tcsa.Source}");
        });

        // This event is raised for each row read from the datasource.
        // You can change the values of args.SyncRow if you need to.
        // this row will be later serialized on disk
        agent.RemoteOrchestrator.OnTableChangesSelectedSyncRow(args =>
        {
            args.SyncRow["Name"] = $"D{args.SyncRow["Name"]}";
            Console.WriteLine(args.SyncRow);

        });

        // The table is read. The batch parts infos are generated and already available on disk
        agent.RemoteOrchestrator.OnTableChangesSelected(tcsa =>
        {
            Console.WriteLine($"Table {tcsa.SchemaTable.GetFullName()}: " +
                $"Files generated count:{tcsa.BatchPartInfos.Count()}. " +
                $"Rows Count:{tcsa.TableChangesSelected.TotalChanges}");
        });


        // The table is read. The batch parts infos are generated and already available on disk
        agent.LocalOrchestrator.OnTableChangesSelected(async tcsa =>
        {
            foreach (var bpi in tcsa.BatchPartInfos)
            {
                var table = await tcsa.BatchInfo.LoadBatchPartInfoAsync(bpi);

                foreach (var row in table.Rows.ToArray())
                {
                   
                }

                await tcsa.BatchInfo.SaveBatchPartInfoAsync(bpi, table);
            }
        });

        agent.LocalOrchestrator.OnTableChangesApplyingSyncRows(args =>
        {
            foreach (var syncRow in args.SyncRows)
                Console.WriteLine(syncRow);
        });
        do
        {
            Console.Clear();
            Console.WriteLine("Sync start");
            try
            {
                var s = await agent.SynchronizeAsync(SyncType.Normal, progress);
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

    }

    private static async Task ProvisionAsync(CoreProvider serverProvider, SyncSetup setup, SyncOptions options)
    {
        var snapshotProgress = new SynchronousProgress<ProgressArgs>(s =>
        {
            Console.ForegroundColor = ConsoleColor.Blue;
            Console.WriteLine($"{s.ProgressPercentage:p}:  \t[{s.Source[..Math.Min(4, s.Source.Length)]}] {s.TypeName}:\t{s.Message}");
            Console.ResetColor();
        });

        Console.WriteLine($"Provision");

        var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options, setup);

        try
        {
            Stopwatch stopw = new Stopwatch();
            stopw.Start();

            await remoteOrchestrator.ProvisionAsync(progress: snapshotProgress);

            stopw.Stop();
            Console.WriteLine($"Total duration :{stopw.Elapsed:hh\\.mm\\:ss\\.fff}");
        }
        catch (Exception e)
        {
            Console.WriteLine("UNKNOW EXCEPTION : " + e.Message);
        }
    }

    private static async Task DeprovisionAsync(CoreProvider serverProvider, SyncSetup setup, SyncOptions options)
    {

        var snapshotProgress = new SynchronousProgress<ProgressArgs>(pa =>
        {
            Console.ForegroundColor = ConsoleColor.Blue;
            Console.WriteLine($"{pa.ProgressPercentage:p}\t {pa.Message}");
            Console.ResetColor();
        });

        Console.WriteLine($"Deprovision ");

        var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options, setup);

        try
        {
            Stopwatch stopw = new Stopwatch();
            stopw.Start();

            await remoteOrchestrator.DeprovisionAsync(progress: snapshotProgress);

            stopw.Stop();
            Console.WriteLine($"Total duration :{stopw.Elapsed:hh\\.mm\\:ss\\.fff}");
        }
        catch (Exception e)
        {
            Console.WriteLine("UNKNOW EXCEPTION : " + e.Message);
        }
    }

    private static async Task CreateSnapshotAsync(CoreProvider serverProvider, SyncSetup setup, SyncOptions options)
    {
        var snapshotProgress = new SynchronousProgress<ProgressArgs>(s =>
        {
            Console.ForegroundColor = ConsoleColor.Blue;
            Console.WriteLine($"{s.ProgressPercentage:p}:  \t[{s.Source[..Math.Min(4, s.Source.Length)]}] {s.TypeName}:\t{s.Message}");
            Console.ResetColor();
        });

        Console.WriteLine($"Creating snapshot");

        var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options, setup);

        try
        {
            Stopwatch stopw = new Stopwatch();
            stopw.Start();

            await remoteOrchestrator.CreateSnapshotAsync(progress: snapshotProgress);

            stopw.Stop();
            Console.WriteLine($"Total duration :{stopw.Elapsed:hh\\.mm\\:ss\\.fff}");
        }
        catch (Exception e)
        {
            Console.WriteLine("UNKNOW EXCEPTION : " + e.Message);
        }
    }


    public static async Task SyncHttpThroughKestrellAsync(CoreProvider clientProvider, CoreProvider serverProvider, SyncSetup setup, SyncOptions options)
    {

        var configureServices = new Action<IServiceCollection>(services =>
        {
            var snapshotDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDiretory());
            var serverOptions = options;
            var syncSetup = new SyncSetup(allTables);

            services.AddSyncServer<SqlSyncProvider>(serverProvider.ConnectionString, syncSetup, serverOptions);
        });

        var serverHandler = new RequestDelegate(async context =>
        {
            try
            {
                var webServerOrchestrator = context.RequestServices.GetService(typeof(WebServerOrchestrator)) as WebServerOrchestrator;

                await webServerOrchestrator.HandleRequestAsync(context);

            }
            catch (Exception ex)
            {
                Debug.WriteLine(ex);
                throw;
            }

        });

        using var server = new KestrellTestServer(configureServices, false);
        var clientHandler = new ResponseDelegate(async (serviceUri) =>
        {
            do
            {
                Console.WriteLine("Web sync start");
                try
                {
                    var startTime = DateTime.Now;

                    var localOrchestrator = new WebClientOrchestrator(serviceUri);

                    var localProgress = new SynchronousProgress<ProgressArgs>(s =>
                    {
                        var tsEnded = TimeSpan.FromTicks(DateTime.Now.Ticks);
                        var tsStarted = TimeSpan.FromTicks(startTime.Ticks);
                        var durationTs = tsEnded.Subtract(tsStarted);

                        Console.ForegroundColor = ConsoleColor.Green;
                        Console.WriteLine($"{durationTs:mm\\:ss\\.fff} {s.ProgressPercentage:p}:\t{s.Message}");
                        Console.ResetColor();
                    });

                    var agent = new SyncAgent(clientProvider, localOrchestrator, options);

                    var s = await agent.SynchronizeAsync(localProgress);
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

    private static async Task SynchronizeOutdatedAsync()
    {
        // Create 2 Sql Sync providers
        var serverProvider = new SqlSyncChangeTrackingProvider(DBHelper.GetDatabaseConnectionString(serverDbName));
        var clientProvider = new SqlSyncChangeTrackingProvider(DBHelper.GetDatabaseConnectionString(clientDbName));

        // Using the Progress pattern to handle progession during the synchronization
        var progress = new SynchronousProgress<ProgressArgs>(s =>
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"{s.ProgressPercentage:p}:\t{s.Source}:\t{s.Message}");
            Console.ResetColor();
        });

        var setup = new SyncSetup(allTables);
        var options = new SyncOptions();

        // Creating an agent that will handle all the process
        var agent = new SyncAgent(clientProvider, serverProvider, options, setup);

        agent.LocalOrchestrator.OnOutdated(oa =>
        {
            Console.WriteLine("Outdated. Reinitiliaze mode");
            oa.Action = OutdatedAction.Reinitialize;
        });

        try
        {
            Console.WriteLine("First Sync");
            var s = await agent.SynchronizeAsync();
            Console.WriteLine(s);

            Console.WriteLine("Generates Outdated date value in Server");

            // Generate a lower sync_timestamp on client
            var clientConnection = new SqlConnection(DBHelper.GetDatabaseConnectionString(clientDbName));
            var clientCommand = new SqlCommand($"Update scope_info set scope_last_server_sync_timestamp=-1", clientConnection);
            clientConnection.Open();
            clientCommand.ExecuteNonQuery();
            clientConnection.Close();


            Console.WriteLine("Second Sync");
            var s2 = await agent.SynchronizeAsync();
            Console.WriteLine(s2);

            Console.WriteLine("Third Sync");
            var s3 = await agent.SynchronizeAsync();
            Console.WriteLine(s3);

        }
        catch (SyncException e)
        {
            Console.WriteLine(e.ToString());
        }
        catch (Exception e)
        {
            Console.WriteLine("UNKNOW EXCEPTION : " + e.Message);
        }
    }

    private static async Task ScenarioAsync()
    {

        // Create 2 Sql Sync providers
        var serverProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(serverDbName));
        var clientDatabaseName = Path.GetRandomFileName().Replace(".", "").ToLowerInvariant() + ".db";
        var clientProvider = new SqliteSyncProvider(clientDatabaseName);

        var options = new SyncOptions();

        var originalSetup = new SyncSetup(new string[] { "ProductCategory" });

        var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options, originalSetup);
        var localOrchestrator = new LocalOrchestrator(clientProvider, options, originalSetup);

        // Creating an agent that will handle all the process
        var agent = new SyncAgent(localOrchestrator, remoteOrchestrator);

        var s = await agent.SynchronizeAsync();
        Console.WriteLine(s);

        // Add a new column to SQL server provider
        await AddNewColumn(serverProvider.CreateConnection(),
            "ProductCategory", "CreationDate", "datetime", "default(getdate())");

        // Add a new column to SQLite client provider
        await AddNewColumn(clientProvider.CreateConnection(),
            "ProductCategory", "CreationDate", "datetime");

        // Deprovision server and client
        await remoteOrchestrator.DeprovisionAsync(
            SyncProvision.StoredProcedures | SyncProvision.Triggers | SyncProvision.TrackingTable);
        await localOrchestrator.DeprovisionAsync(
            SyncProvision.StoredProcedures | SyncProvision.Triggers | SyncProvision.TrackingTable);

        var newSetup = new SyncSetup(new string[] { "ProductCategory", "Product" });

        // re create orchestrators with new setup
        remoteOrchestrator = new RemoteOrchestrator(serverProvider, options, newSetup);
        localOrchestrator = new LocalOrchestrator(clientProvider, options, newSetup);

        // Provision again the server 
        await remoteOrchestrator.ProvisionAsync(
            SyncProvision.StoredProcedures | SyncProvision.Triggers | SyncProvision.TrackingTable);

        // Get the server schema to be sure we can create the table on client side
        var schema = await remoteOrchestrator.GetSchemaAsync();

        // Provision local orchestrator based on server schema
        // Adding option Table to be sure I'm provisioning the new table
        await localOrchestrator.ProvisionAsync(schema,
            SyncProvision.StoredProcedures | SyncProvision.Triggers | SyncProvision.TrackingTable | SyncProvision.Table);

        // Sync with Reinitialize
        agent = new SyncAgent(localOrchestrator, remoteOrchestrator);

        s = await agent.SynchronizeAsync(SyncType.Reinitialize);
        Console.WriteLine(s);

    }

    private static async Task AddNewColumn(DbConnection connection,
        string tableName, string columnName, string columnType,
        string defaultValue = default)
    {
        var command = connection.CreateCommand();
        command.CommandText = $"ALTER TABLE {tableName} ADD {columnName} {columnType} NULL {defaultValue}";
        command.Connection = connection;
        command.CommandType = CommandType.Text;

        await connection.OpenAsync();
        command.ExecuteNonQuery();
        await connection.CloseAsync();


    }


    private static async Task UpdateSetupAndProvisionAsync()
    {
        // [Required]: Get a connection string to your server data source
        var serverProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(serverDbName));

        // [Required] Tables involved in the sync process:
        var tables = new string[] {"ProductCategory", "ProductModel", "Product",
            "Address", "Customer", "CustomerAddress", "SalesOrderHeader", "SalesOrderDetail" };

        var syncSetup = new SyncSetup(tables);

        // Using the Progress pattern to handle progession during the synchronization
        var progress = new SynchronousProgress<ProgressArgs>(s => Console.WriteLine($"{s.Source}:\t{s.Message}"));

        var orchestrator = new RemoteOrchestrator(serverProvider, new SyncOptions(), syncSetup);

        await orchestrator.DeprovisionAsync(SyncProvision.StoredProcedures | SyncProvision.Triggers, progress: progress);
        await orchestrator.ProvisionAsync(SyncProvision.StoredProcedures | SyncProvision.Triggers, progress: progress);

    }
    public static async Task ForceUpgradeClientAsync()
    {
        // server provider
        // Create 2 Sql Sync providers
        var serverProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(serverDbName));
        var clientProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(clientDbName));

        //var clientDatabaseName = Path.GetRandomFileName().Replace(".", "").ToLowerInvariant() + ".db";
        //var clientProvider = new SqliteSyncProvider(clientDatabaseName);

        var configureServices = new Action<IServiceCollection>(services =>
        {
            var serverOptions = new SyncOptions()
            {
                DisableConstraintsOnApplyChanges = false,
            };

            var tables = new string[] {"ProductCategory", "ProductModel", "Product",
            "Address", "Customer", "CustomerAddress", "SalesOrderHeader", "SalesOrderDetail" };

            var syncSetup = new SyncSetup(tables);

            services.AddSyncServer<SqlSyncProvider>(serverProvider.ConnectionString, syncSetup, serverOptions);
        });

        var serverHandler = new RequestDelegate(async context =>
        {
            try
            {
                var webServerOrchestrator = context.RequestServices.GetService(typeof(WebServerOrchestrator)) as WebServerOrchestrator;

                await webServerOrchestrator.HandleRequestAsync(context);

            }
            catch (Exception ex)
            {
                Debug.WriteLine(ex);
                throw;
            }

        });

        using var server = new KestrellTestServer(configureServices, false);
        var clientHandler = new ResponseDelegate(async (serviceUri) =>
        {
            do
            {
                Console.WriteLine("Web sync start");
                try
                {
                    var startTime = DateTime.Now;

                    var remoteOrchestrator = new WebClientOrchestrator(serviceUri);

                    var clientOptions = new SyncOptions();

                    var localProgress = new SynchronousProgress<ProgressArgs>(s =>
                    {
                        var tsEnded = TimeSpan.FromTicks(DateTime.Now.Ticks);
                        var tsStarted = TimeSpan.FromTicks(startTime.Ticks);
                        var durationTs = tsEnded.Subtract(tsStarted);

                        Console.ForegroundColor = ConsoleColor.Green;
                        Console.WriteLine($"{durationTs:mm\\:ss\\.fff} {s.ProgressPercentage:p}:\t{s.Message}");
                        Console.ResetColor();
                    });


                    var agent = new SyncAgent(clientProvider, remoteOrchestrator, clientOptions);

                    // fake setup to deprovision one table to migrate
                    var setup = new SyncSetup(new string[] { "Customer" });

                    // creating a localorchestrator with this fake setup
                    var localOrchestrator = new LocalOrchestrator(clientProvider, clientOptions, setup);

                    // Deprovision all stored procedure for the table from the fake setup
                    await localOrchestrator.DeprovisionAsync(SyncProvision.StoredProcedures | SyncProvision.Triggers | SyncProvision.TrackingTable);

                    // getting server scope with new column
                    var serverFullSchema = await agent.RemoteOrchestrator.GetSchemaAsync().ConfigureAwait(false);

                    // Provision again
                    await agent.LocalOrchestrator.ProvisionAsync(serverFullSchema).ConfigureAwait(false);

                    // make a sync
                    var s = await agent.SynchronizeAsync(localProgress);
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

    private static async Task SynchronizeWithOneFilterAsync()
    {
        // Create 2 Sql Sync providers
        var serverProvider = new SqlSyncChangeTrackingProvider(DBHelper.GetDatabaseConnectionString("MediaStore2"));
        //var clientProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(clientDbName));

        var clientDatabaseName = Path.GetRandomFileName().Replace(".", "").ToLowerInvariant() + ".db";
        var clientProvider = new SqliteSyncProvider(clientDatabaseName);

        var options = new SyncOptions
        {
            DisableConstraintsOnApplyChanges = true
        };

        var tables = new string[] { "media.Album", "media.Artist", "media.Customer", "media.Invoice", "media.InvoiceItem", "media.Track" };

        var setup = new SyncSetup(tables);

        var customerFilter = new SetupFilter("Customer", "media");
        customerFilter.AddParameter("CustomerId", "Customer", "media", false);
        customerFilter.AddWhere("CustomerId", "Customer", "CustomerId", "media");
        setup.Filters.Add(customerFilter);

        var invoiceCustomerFilter = new SetupFilter("Invoice", "media");
        invoiceCustomerFilter.AddParameter("CustomerId", "Customer", "media", false);
        invoiceCustomerFilter.AddJoin(Join.Inner, "Customer").On("media.Invoice", "CustomerId", "media.Customer", "CustomerId");
        invoiceCustomerFilter.AddWhere("CustomerId", "Customer", "CustomerId", "media");
        setup.Filters.Add(invoiceCustomerFilter);

        var invoiceItemCustomerFilter = new SetupFilter("InvoiceItem", "media");
        invoiceItemCustomerFilter.AddParameter("CustomerId", "Customer", "media", false);
        invoiceItemCustomerFilter.AddJoin(Join.Inner, "Invoice").On("media.InvoiceItem", "InvoiceId", "media.Invoice", "InvoiceId");
        invoiceItemCustomerFilter.AddJoin(Join.Inner, "Customer").On("media.Invoice", "CustomerId", "media.Customer", "CustomerId");
        invoiceCustomerFilter.AddWhere("CustomerId", "Customer", "CustomerId", "media");
        setup.Filters.Add(invoiceItemCustomerFilter);


        // Creating an agent that will handle all the process
        var agent = new SyncAgent(clientProvider, serverProvider, options, setup);

        // Using the Progress pattern to handle progession during the synchronization
        var progress = new SynchronousProgress<ProgressArgs>(s =>
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"{s.ProgressPercentage:p}:\t{s.Message}");
            Console.ResetColor();
        });

        do
        {
            // Console.Clear();
            Console.WriteLine("Sync Start");
            try
            {
                if (!agent.Parameters.Contains("CustomerId"))
                    agent.Parameters.Add("CustomerId", 10);

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

    private static async Task SynchronizeHeavyTableAsync()
    {
        // Create 2 Sql Sync providers
        var serverProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString("HeavyTables"));

        var tables = new string[] { "Customer" };
        var setup = new SyncSetup(tables);

        var options = new SyncOptions { BatchSize = 3000 };

        // Using the Progress pattern to handle progession during the synchronization
        var progress = new SynchronousProgress<ProgressArgs>(s =>
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"{s.ProgressPercentage:p}:\t{s.Source}:\t{s.Message}");
            Console.ResetColor();
        });

        var clientDatabaseName = Path.GetRandomFileName().Replace(".", "").ToLowerInvariant() + ".db";
        for (int i = 0; i < 5; i++)
        {
            try
            {
                var clientProvider = new SqliteSyncProvider(clientDatabaseName);
                //var clientProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(clientDbName));
                var agent = new SyncAgent(clientProvider, serverProvider, options, setup);

                agent.LocalOrchestrator.OnTableChangesApplying(args => Console.WriteLine(args.Command.CommandText));

                var s = await agent.SynchronizeAsync(SyncType.Reinitialize, progress);
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
        }




    }

    private static async Task SynchronizeAsyncThenAddFilterAsync()
    {
        // Create 2 Sql Sync providers
        var serverProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(serverDbName));
        var clientProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(clientDbName));

        // ------------------------------------------
        // Step 1 : We want all the Customer rows
        // ------------------------------------------

        // Create a Setup for table customer only
        var setup = new SyncSetup(new string[] { "Customer" });

        // Creating an agent that will handle all the process
        var agent = new SyncAgent(clientProvider, serverProvider, setup);

        // Using the Progress pattern to handle progession during the synchronization
        var progress = new SynchronousProgress<ProgressArgs>(s =>
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"{s.ProgressPercentage:p}:\t{s.Source}:\t{s.Message}");
            Console.ResetColor();
        });

        var r = await agent.SynchronizeAsync(SyncType.Reinitialize, progress);
        Console.WriteLine(r);

        // ------------------------------------------
        // Step 2 : We want to add a filter to Customer
        // ------------------------------------------


        // Deprovision everything

        //// On server since it's change tracking, just remove the stored procedures and scope / scope history
        //await agent.RemoteOrchestrator.DeprovisionAsync(SyncProvision.StoredProcedures
        //    | SyncProvision.ServerScope | SyncProvision.ServerHistoryScope);

        //// On client, remove everything
        //await agent.LocalOrchestrator.DeprovisionAsync(SyncProvision.StoredProcedures
        //    | SyncProvision.Triggers | SyncProvision.TrackingTable
        //    | SyncProvision.ClientScope);

        // Add filter

        setup.Filters.Add("Customer", "CompanyName");

        if (!agent.Parameters.Contains("CompanyName"))
            agent.Parameters.Add("CompanyName", "Professional Sales and Service");

        r = await agent.SynchronizeAsync(SyncType.Reinitialize, progress);

        Console.WriteLine(r);

    }

    public static async Task SyncHttpThroughKestrellAndTestDateTimeSerializationAsync()
    {
        // server provider
        // Create 2 Sql Sync providers
        var serverProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString("AdvProductCategory"));
        var clientProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(clientDbName));
        //var clientProvider = new SqliteSyncProvider("AdvHugeD.db");

        // ----------------------------------
        // Client & Server side
        // ----------------------------------
        // snapshot directory
        // Sync options
        var options = new SyncOptions
        {
            BatchDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDiretory(), "Tmp"),
            BatchSize = 10000,
        };

        // Create the setup used for your sync process
        //var tables = new string[] { "Employees" };


        var localProgress = new SynchronousProgress<ProgressArgs>(s =>
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"{s.ProgressPercentage:p}:\t{s.Message}");
            Console.ResetColor();
        });

        var configureServices = new Action<IServiceCollection>(services =>
        {
            services.AddSyncServer<SqlSyncProvider>(serverProvider.ConnectionString, new string[] { "ProductCategory" }, options);

        });

        var serverHandler = new RequestDelegate(async context =>
        {
            var webServerOrchestrator = context.RequestServices.GetService(typeof(WebServerOrchestrator)) as WebServerOrchestrator;

            await webServerOrchestrator.HandleRequestAsync(context);
        });

        using var server = new KestrellTestServer(configureServices);
        var clientHandler = new ResponseDelegate(async (serviceUri) =>
        {
            Console.WriteLine("First Sync. Web sync start");
            try
            {

                var localDateTime = DateTime.Now;
                var utcDateTime = DateTime.UtcNow;

                var localOrchestrator = new WebClientOrchestrator(serviceUri);

                var agent = new SyncAgent(clientProvider, localOrchestrator, options);
                await agent.SynchronizeAsync(localProgress);


                string commandText = "Insert into ProductCategory (Name, ModifiedDate) Values (@Name, @ModifiedDate)";
                var connection = clientProvider.CreateConnection();

                connection.Open();

                var command = connection.CreateCommand();
                command.CommandText = commandText;
                command.Connection = connection;

                var p = command.CreateParameter();
                p.DbType = DbType.String;
                p.ParameterName = "@Name";
                p.Value = "TestUTC";
                command.Parameters.Add(p);

                p = command.CreateParameter();
                // Change DbTtpe to String for testing purpose
                p.DbType = DbType.String;
                p.ParameterName = "@ModifiedDate";
                p.Value = utcDateTime;
                command.Parameters.Add(p);


                command.ExecuteNonQuery();

                command.Parameters["@Name"].Value = "TestLocal";
                command.Parameters["@ModifiedDate"].Value = localDateTime;

                command.ExecuteNonQuery();


                connection.Close();

                // check
                connection.Open();

                commandText = "Select * from ProductCategory where Name='TestUTC'";
                command = connection.CreateCommand();
                command.CommandText = commandText;
                command.Connection = connection;

                var dr = command.ExecuteReader();
                dr.Read();

                Console.WriteLine($"UTC : {utcDateTime} - {dr["ModifiedDate"]}");

                dr.Close();


                commandText = "Select * from ProductCategory where Name='TestLocal'";
                command = connection.CreateCommand();
                command.CommandText = commandText;
                command.Connection = connection;

                dr = command.ExecuteReader();
                dr.Read();

                Console.WriteLine($"Local : {localDateTime} - {dr["ModifiedDate"]}");

                dr.Close();

                connection.Close();

                Console.WriteLine("Sync");

                var s = await agent.SynchronizeAsync(localProgress);
                Console.WriteLine(s);

                // check results on server
                connection = serverProvider.CreateConnection();

                // check
                connection.Open();

                commandText = "Select * from ProductCategory where Name='TestUTC'";
                command = connection.CreateCommand();
                command.CommandText = commandText;
                command.Connection = connection;

                dr = command.ExecuteReader();
                dr.Read();

                Console.WriteLine($"UTC : {utcDateTime} - {dr["ModifiedDate"]}");

                dr.Close();


                commandText = "Select * from ProductCategory where Name='TestLocal'";
                command = connection.CreateCommand();
                command.CommandText = commandText;
                command.Connection = connection;

                dr = command.ExecuteReader();
                dr.Read();

                Console.WriteLine($"Local : {localDateTime} - {dr["ModifiedDate"]}");

                dr.Close();

                connection.Close();



            }
            catch (SyncException e)
            {
                Console.WriteLine(e.ToString());
            }
            catch (Exception e)
            {
                Console.WriteLine("UNKNOW EXCEPTION : " + e.Message);
            }



        });
        await server.Run(serverHandler, clientHandler);

    }

    private static async Task Snapshot_Then_ReinitializeAsync()
    {
        var clientFileName = "AdventureWorks.db";

        var tables = new string[] { "Customer" };

        var setup = new SyncSetup(tables)
        {
            // optional :
            StoredProceduresPrefix = "ussp_",
            StoredProceduresSuffix = "",
            TrackingTablesPrefix = "",
            TrackingTablesSuffix = "_tracking"
        };
        setup.Tables["Customer"].SyncDirection = SyncDirection.DownloadOnly;

        var options = new SyncOptions();

        // Using the Progress pattern to handle progession during the synchronization
        var progress = new SynchronousProgress<ProgressArgs>(s => Console.WriteLine($"{s.ProgressPercentage:p}:\t{s.Source}:\t{s.Message}"));

        // Be sure client database file is deleted is already exists
        if (File.Exists(clientFileName))
            File.Delete(clientFileName);

        // Create 2 Sql Sync providers
        // sql with change tracking enabled
        var serverProvider = new SqlSyncChangeTrackingProvider(DBHelper.GetDatabaseConnectionString(serverDbName));
        var clientProvider = new SqliteSyncProvider(clientFileName);

        // Creating an agent that will handle all the process
        var agent = new SyncAgent(clientProvider, serverProvider, options, setup);

        Console.WriteLine();
        Console.WriteLine("----------------------");
        Console.WriteLine("0 - Initiliaze. Initialize Client database and get all Customers");

        // First sync to initialize
        var r = await agent.SynchronizeAsync(progress);
        Console.WriteLine(r);


        // DeprovisionAsync
        Console.WriteLine();
        Console.WriteLine("----------------------");
        Console.WriteLine("1 - Deprovision The Server");

        var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options, setup);

        // We are in change tracking mode, so no need to deprovision triggers and tracking table.
        await remoteOrchestrator.DeprovisionAsync(SyncProvision.StoredProcedures, progress: progress);

        // DeprovisionAsync
        Console.WriteLine();
        Console.WriteLine("----------------------");
        Console.WriteLine("2 - Provision Again With New Setup");

        tables = new string[] { "Customer", "ProductCategory" };

        setup = new SyncSetup(tables)
        {
            // optional :
            StoredProceduresPrefix = "ussp_",
            StoredProceduresSuffix = "",
            TrackingTablesPrefix = "",
            TrackingTablesSuffix = "_tracking"
        };
        setup.Tables["Customer"].SyncDirection = SyncDirection.DownloadOnly;
        setup.Tables["ProductCategory"].SyncDirection = SyncDirection.DownloadOnly;

        remoteOrchestrator = new RemoteOrchestrator(serverProvider, options, setup);

        var newSchema = await remoteOrchestrator.ProvisionAsync(SyncProvision.StoredProcedures | SyncProvision.Triggers | SyncProvision.TrackingTable, progress: progress);

        // Snapshot
        Console.WriteLine();
        Console.WriteLine("----------------------");
        Console.WriteLine("3 - Create Snapshot");

        var snapshotDirctory = Path.Combine(SyncOptions.GetDefaultUserBatchDiretory(), "Snapshots");

        options = new SyncOptions
        {
            SnapshotsDirectory = snapshotDirctory,
            BatchSize = 5000
        };

        remoteOrchestrator = new RemoteOrchestrator(serverProvider, options, setup);
        // Create a snapshot
        var bi = await remoteOrchestrator.CreateSnapshotAsync(progress: progress);

        Console.WriteLine("Create snapshot done.");
        Console.WriteLine($"Rows Count in the snapshot:{bi.RowsCount}");
        foreach (var bpi in bi.BatchPartsInfo)
            foreach (var table in bpi.Tables)
                Console.WriteLine($"File: {bpi.FileName}. Table {table.TableName}: Rows Count:{table.RowsCount}");

        // Snapshot
        Console.WriteLine();
        Console.WriteLine("----------------------");
        Console.WriteLine("4 - Sync again with Reinitialize Mode");


        agent = new SyncAgent(clientProvider, serverProvider, options, setup);

        r = await agent.SynchronizeAsync(SyncType.Reinitialize, progress);
        Console.WriteLine(r);


        Console.WriteLine();
        Console.WriteLine("----------------------");
        Console.WriteLine("5 - Check client rows");

        using var sqliteConnection = new SqliteConnection(clientProvider.ConnectionString);

        sqliteConnection.Open();

        var command = new SqliteCommand("Select count(*) from Customer", sqliteConnection);
        var customerCount = (long)command.ExecuteScalar();

        command = new SqliteCommand("Select count(*) from ProductCategory", sqliteConnection);
        var productCategoryCount = (long)command.ExecuteScalar();

        Console.WriteLine($"Customer Rows Count on Client Database:{customerCount} rows");
        Console.WriteLine($"ProductCategory Rows Count on Client Database:{productCategoryCount} rows");

        sqliteConnection.Close();

    }

    /// <summary>
    /// Test a client syncing through a web api
    /// </summary>
    private static async Task SyncThroughWebApiAsync()
    {
        var clientProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(clientDbName));

        var handler = new HttpClientHandler { AutomaticDecompression = DecompressionMethods.GZip };
        var client = new HttpClient(handler) { Timeout = TimeSpan.FromMinutes(5) };

        var proxyClientProvider = new WebClientOrchestrator("https://localhost:44313/api/Sync", client: client);

        var options = new SyncOptions
        {
            BatchDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDiretory(), "Tmp"),
            BatchSize = 2000,
        };

        // Create the setup used for your sync process
        //var tables = new string[] { "Employees" };


        var remoteProgress = new SynchronousProgress<ProgressArgs>(pa =>
        {
            Console.ForegroundColor = ConsoleColor.Gray;
            Console.WriteLine($"{pa.ProgressPercentage:p}\t {pa.Message}");
            Console.ResetColor();
        });

        var snapshotProgress = new SynchronousProgress<ProgressArgs>(pa =>
        {
            Console.ForegroundColor = ConsoleColor.Blue;
            Console.WriteLine($"{pa.ProgressPercentage:p}\t {pa.Message}");
            Console.ResetColor();
        });

        var localProgress = new SynchronousProgress<ProgressArgs>(s =>
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"{s.ProgressPercentage:p}:\t{s.Message}");
            Console.ResetColor();
        });


        var agent = new SyncAgent(clientProvider, proxyClientProvider, options);


        Console.WriteLine("Press a key to start (be sure web api is running ...)");
        Console.ReadKey();
        do
        {
            Console.Clear();
            Console.WriteLine("Web sync start");
            try
            {

                var s = await agent.SynchronizeAsync(SyncType.Reinitialize, localProgress);
                Console.WriteLine(s);

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

    private static async Task SynchronizeWithFiltersAndMultiScopesAsync()
    {
        // Create 2 Sql Sync providers
        var serverProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(serverDbName));
        var clientProvider1 = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(clientDbName));
        var clientProvider2 = new SqliteSyncProvider("clientX3.db");


        var configureServices = new Action<IServiceCollection>(services =>
        {

            // Setup 1 : contains all tables, all columns with filter
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
            setup.StoredProceduresPrefix = "filtered";
            setup.StoredProceduresSuffix = "";
            setup.TrackingTablesPrefix = "t";
            setup.TrackingTablesSuffix = "";

            var options = new SyncOptions();

            services.AddSyncServer<SqlSyncProvider>(serverProvider.ConnectionString, "Filters", setup);

            //contains only some tables with subset of columns
            var setup2 = new SyncSetup(new string[] { "Address", "Customer", "CustomerAddress" });

            setup2.Tables["Customer"].Columns.AddRange(new string[] { "CustomerID", "FirstName", "LastName" });
            setup2.StoredProceduresPrefix = "restricted";
            setup2.StoredProceduresSuffix = "";
            setup2.TrackingTablesPrefix = "t";
            setup2.TrackingTablesSuffix = "";

            services.AddSyncServer<SqlSyncProvider>(serverProvider.ConnectionString, "Restricted", setup2, options);

        });

        var serverHandler = new RequestDelegate(async context =>
        {
            var webServerOrchestrator = context.RequestServices.GetService(typeof(WebServerOrchestrator)) as WebServerOrchestrator;

            var progress = new SynchronousProgress<ProgressArgs>(pa =>
            {
                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.WriteLine($"{pa.ProgressPercentage:p}\t {pa.Message}");
                Console.ResetColor();
            });
            await webServerOrchestrator.HandleRequestAsync(context, default, progress);

        });


        using var server = new KestrellTestServer(configureServices);

        var clientHandler = new ResponseDelegate(async (serviceUri) =>
        {
            do
            {
                Console.Clear();
                Console.WriteLine("Web sync start");
                try
                {
                    var webClientOrchestrator = new WebClientOrchestrator(serviceUri);
                    var agent = new SyncAgent(clientProvider1, webClientOrchestrator, "Filters");

                    // Launch the sync process
                    if (!agent.Parameters.Contains("CompanyName"))
                        agent.Parameters.Add("CompanyName", "Professional Sales and Service");

                    // Using the Progress pattern to handle progession during the synchronization
                    var progress = new SynchronousProgress<ProgressArgs>(s =>
                {
                    Console.ForegroundColor = ConsoleColor.Green;
                    Console.WriteLine($"{s.ProgressPercentage:p}:\t{s.Source}:\t{s.Message}");
                    Console.ResetColor();
                });

                    var s = await agent.SynchronizeAsync(progress);
                    Console.WriteLine(s);


                    var agent2 = new SyncAgent(clientProvider2, webClientOrchestrator, "Restricted");

                    // Using the Progress pattern to handle progession during the synchronization
                    var progress2 = new SynchronousProgress<ProgressArgs>(s =>
                {
                    Console.ForegroundColor = ConsoleColor.DarkGreen;
                    Console.WriteLine($"{s.ProgressPercentage:p}:\t{s.Source}:\t{s.Message}");
                    Console.ResetColor();
                });
                    s = await agent2.SynchronizeAsync(progress2);
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


    private static async Task SynchronizeWithFiltersAsync()
    {
        // Create 2 Sql Sync providers
        var serverProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(serverDbName));
        //var clientProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(clientDbName));

        var clientDatabaseName = Path.GetRandomFileName().Replace(".", "").ToLowerInvariant() + ".db";
        var clientProvider = new SqliteSyncProvider(clientDatabaseName);

        var setup = new SyncSetup(new string[] {"ProductCategory",
                  "ProductModel", "Product",
                  "Address", "Customer", "CustomerAddress",
                  "SalesOrderHeader", "SalesOrderDetail" });

        // ----------------------------------------------------
        // Horizontal Filter: On rows. Removing rows from source
        // ----------------------------------------------------
        // Over all filter : "we Want only customer from specific city and specific postal code"
        // First level table : Address
        // Second level tables : CustomerAddress
        // Third level tables : Customer, SalesOrderHeader
        // Fourth level tables : SalesOrderDetail

        // Create a filter on table Address on City Washington
        // Optional : Sub filter on PostalCode, for testing purpose
        var addressFilter = new SetupFilter("Address");

        // For each filter, you have to provider all the input parameters
        // A parameter could be a parameter mapped to an existing colum : That way you don't have to specify any type, length and so on ...
        // We can specify if a null value can be passed as parameter value : That way ALL addresses will be fetched
        // A default value can be passed as well, but works only on SQL Server (MySql is a damn shity thing)
        addressFilter.AddParameter("City", "Address", true);

        // Or a parameter could be a random parameter bound to anything. In that case, you have to specify everything
        // (This parameter COULD BE bound to a column, like City, but for the example, we go for a custom parameter)
        addressFilter.AddParameter("postal", DbType.String, true, null, 20);

        // Then you map each parameter on wich table / column the "where" clause should be applied
        addressFilter.AddWhere("City", "Address", "City");
        addressFilter.AddWhere("PostalCode", "Address", "postal");
        setup.Filters.Add(addressFilter);

        var addressCustomerFilter = new SetupFilter("CustomerAddress");
        addressCustomerFilter.AddParameter("City", "Address", true);
        addressCustomerFilter.AddParameter("postal", DbType.String, true, null, 20);

        // You can join table to go from your table up (or down) to your filter table
        addressCustomerFilter.AddJoin(Join.Left, "Address").On("CustomerAddress", "AddressId", "Address", "AddressId");

        // And then add your where clauses
        addressCustomerFilter.AddWhere("City", "Address", "City");
        addressCustomerFilter.AddWhere("PostalCode", "Address", "postal");
        setup.Filters.Add(addressCustomerFilter);

        var customerFilter = new SetupFilter("Customer");
        customerFilter.AddParameter("City", "Address", true);
        customerFilter.AddParameter("postal", DbType.String, true, null, 20);
        customerFilter.AddJoin(Join.Left, "CustomerAddress").On("CustomerAddress", "CustomerId", "Customer", "CustomerId");
        customerFilter.AddJoin(Join.Left, "Address").On("CustomerAddress", "AddressId", "Address", "AddressId");
        customerFilter.AddWhere("City", "Address", "City");
        customerFilter.AddWhere("PostalCode", "Address", "postal");
        setup.Filters.Add(customerFilter);

        var orderHeaderFilter = new SetupFilter("SalesOrderHeader");
        orderHeaderFilter.AddParameter("City", "Address", true);
        orderHeaderFilter.AddParameter("postal", DbType.String, true, null, 20);
        orderHeaderFilter.AddJoin(Join.Left, "CustomerAddress").On("CustomerAddress", "CustomerId", "SalesOrderHeader", "CustomerId");
        orderHeaderFilter.AddJoin(Join.Left, "Address").On("CustomerAddress", "AddressId", "Address", "AddressId");
        orderHeaderFilter.AddWhere("City", "Address", "City");
        orderHeaderFilter.AddWhere("PostalCode", "Address", "postal");
        setup.Filters.Add(orderHeaderFilter);

        var orderDetailsFilter = new SetupFilter("SalesOrderDetail");
        orderDetailsFilter.AddParameter("City", "Address", true);
        orderDetailsFilter.AddParameter("postal", DbType.String, true, null, 20);
        orderDetailsFilter.AddJoin(Join.Left, "SalesOrderHeader").On("SalesOrderHeader", "SalesOrderID", "SalesOrderDetail", "SalesOrderID");
        orderDetailsFilter.AddJoin(Join.Left, "CustomerAddress").On("CustomerAddress", "CustomerId", "SalesOrderHeader", "CustomerId");
        orderDetailsFilter.AddJoin(Join.Left, "Address").On("CustomerAddress", "AddressId", "Address", "AddressId");
        orderDetailsFilter.AddWhere("City", "Address", "City");
        orderDetailsFilter.AddWhere("PostalCode", "Address", "postal");
        setup.Filters.Add(orderDetailsFilter);


        var options = new SyncOptions();

        // Creating an agent that will handle all the process
        var agent = new SyncAgent(clientProvider, serverProvider, options, setup);

        // Using the Progress pattern to handle progession during the synchronization
        var progress = new SynchronousProgress<ProgressArgs>(s =>
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"{s.ProgressPercentage:p}:\t{s.Message}");
            Console.ResetColor();
        });

        do
        {
            // Console.Clear();
            Console.WriteLine("Sync Start");
            try
            {

                if (!agent.Parameters.Contains("City"))
                    agent.Parameters.Add("City", "Toronto");

                // Because I've specified that "postal" could be null, I can set the value to DBNull.Value (and then get all postal code in Toronto city)
                if (!agent.Parameters.Contains("postal"))
                    agent.Parameters.Add("postal", DBNull.Value);

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

    private static async Task SynchronizeWithLoggerAsync()
    {

        //docker run -it --name seq -p 5341:80 -e ACCEPT_EULA=Y datalust/seq

        // Create 2 Sql Sync providers
        var serverProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(serverDbName));
        var clientProvider = new SqlSyncProvider(DBHelper.GetDatabaseConnectionString(clientDbName));
        //var clientProvider = new SqliteSyncProvider("clientX.db");

        var setup = new SyncSetup(new string[] { "Address", "Customer", "CustomerAddress", "SalesOrderHeader", "SalesOrderDetail" });
        //var setup = new SyncSetup(new string[] { "Customer" });
        //var setup = new SyncSetup(new[] { "Customer" });
        //setup.Tables["Customer"].Columns.AddRange(new[] { "CustomerID", "FirstName", "LastName" });


        //Log.Logger = new LoggerConfiguration()
        //    .Enrich.FromLogContext()
        //    .MinimumLevel.Verbose()
        //    .MinimumLevel.Override("Microsoft", LogEventLevel.Warning)
        //    .WriteTo.Console()
        //    .CreateLogger();

        // *) create a console logger
        //var loggerFactory = LoggerFactory.Create(builder => { builder.AddConsole().SetMinimumLevel(LogLevel.Trace); });
        //var logger = loggerFactory.CreateLogger("Dotmim.Sync");
        //options.Logger = logger;

        // *) create a seq logger
        var loggerFactory = LoggerFactory.Create(builder => { builder.AddSeq().SetMinimumLevel(LogLevel.Debug); });
        var logger = loggerFactory.CreateLogger("Dotmim.Sync");


        // *) create a serilog logger
        //var loggerFactory = LoggerFactory.Create(builder => { builder.AddSerilog().SetMinimumLevel(LogLevel.Trace); });
        //var logger = loggerFactory.CreateLogger("SyncAgent");
        //options.Logger = logger;

        // *) Using Serilog with Seq
        //var serilogLogger = new LoggerConfiguration()
        //    .Enrich.FromLogContext()
        //    .MinimumLevel.Debug()
        //    .MinimumLevel.Override("Microsoft", LogEventLevel.Warning)
        //    .WriteTo.Seq("http://localhost:5341")
        //    .CreateLogger();


        //var actLogging = new Action<SyncLoggerOptions>(slo =>
        //{
        //    slo.AddConsole();
        //    slo.SetMinimumLevel(LogLevel.Information);
        //});

        ////var loggerFactory = LoggerFactory.Create(builder => builder.AddSerilog().AddConsole().SetMinimumLevel(LogLevel.Information));

        //var loggerFactory = LoggerFactory.Create(builder => builder.AddSerilog(serilogLogger));

        //loggerFactory.AddSerilog(serilogLogger);

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

        var options = new SyncOptions();
        options.BatchSize = 500;
        options.Logger = logger;
        //options.Logger = new SyncLogger().AddConsole().SetMinimumLevel(LogLevel.Debug);


        // Creating an agent that will handle all the process
        var agent = new SyncAgent(clientProvider, serverProvider, options, setup);

        // Using the Progress pattern to handle progession during the synchronization
        var progress = new SynchronousProgress<ProgressArgs>(s =>
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"{s.ProgressPercentage:p}:\t{s.Message}");
            Console.ResetColor();
        });

        do
        {
            // Console.Clear();
            Console.WriteLine("Sync Start");
            try
            {
                // Launch the sync process
                //if (!agent.Parameters.Contains("CompanyName"))
                //    agent.Parameters.Add("CompanyName", "Professional Sales and Service");

                var s1 = await agent.SynchronizeAsync(progress);

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


}
