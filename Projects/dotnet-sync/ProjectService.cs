using Dotmim.Sync.Builders;
using Dotmim.Sync.Data;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.SQLite;
using Dotmim.Sync.SqlServer;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.Tools
{
    public class ProjectService
    {
        private readonly string projectName;
        private readonly List<Argument> args;


        public ProjectService(string projectName, List<Argument> args)
        {
            this.projectName = projectName;
            this.args = args;
        }

        public void Execute()
        {
            if (args.Count > 1)
                throw new Exception("Too many arguments. See help: dotnet sync --help");

            if (args.Count == 0)
                throw new Exception("No argument specified. See help: dotnet sync --help");

            var arg = args[0];

            if (!string.IsNullOrEmpty(projectName) && string.IsNullOrEmpty(arg.Value))
                arg.Value = projectName;

            switch (arg.ArgumentType)
            {
                case ArgumentType.None:
                    return;
                case ArgumentType.RootVersion:
                    return;
                case ArgumentType.RootHelp:
                    return;
                case ArgumentType.RootSync:
                    this.Sync(arg.Value);
                    return;
                case ArgumentType.RootVerbose:
                    return;
                case ArgumentType.ProjectNew:
                    this.CreateProject(arg.Value);
                    return;
                case ArgumentType.ProjectInfo:
                    this.GetProjectInfo(arg.Value);
                    return;
                case ArgumentType.ProjectDelete:
                    this.RemoveProject(arg.Value);
                    return;
                case ArgumentType.ProjectList:
                    this.GetProjectsList();
                    return;
            }

            throw new Exception("this command is incorrect. Try dotnet sync project --help to get availables arguments for project command.");

        }

        internal SyncContext Sync(string value)
        {
            if (String.IsNullOrEmpty(value))
                throw new Exception("Loading a project requires a name. Ex : dotnet sync --load project01");

            Project project = DataStore.Current.LoadProject(value);

            if (project == null)
                throw new Exception($"Project {value} does not exists.");

            if (project.ServerProvider == null || string.IsNullOrEmpty(project.ServerProvider.ConnectionString))
                throw new Exception($"Server provider for project {project.Name} is not correctly defined. See help: dotnet sync provider --help");

            if (project.ClientProvider == null || string.IsNullOrEmpty(project.ClientProvider.ConnectionString))
                throw new Exception($"Client provider for project {project.Name} is not correctly defined. See help: dotnet sync provider --help");

            if (project.Tables == null || project.Tables.Count <= 0)
                throw new Exception($"No table configured for project {project.Name}. See help: dotnet sync table --help");

            IProvider serverProvider, clientprovider;
            switch (project.ServerProvider.ProviderType)
            {
                case ProviderType.Sqlite:
                    throw new Exception("Can't use SQLite as a server provider");
                case ProviderType.SqlServer:
                default:
                    serverProvider = new SqlSyncProvider(project.ServerProvider.ConnectionString);
                    break;
            }

            switch (project.ClientProvider.ProviderType)
            {
                case ProviderType.Sqlite:
                    clientprovider = new SQLiteSyncProvider(project.ClientProvider.ConnectionString);
                    break;
                case ProviderType.SqlServer:
                default:
                    clientprovider = new SqlSyncProvider(project.ClientProvider.ConnectionString);
                    break;
            }

            SyncConfiguration syncConfiguration = new SyncConfiguration();

            foreach (var t in project.Tables.OrderBy(tbl => tbl.Order))
            {
                // Potentially user can pass something like [SalesLT].[Product]
                // or SalesLT.Product or Product. ObjectNameParser will handle it
                ObjectNameParser parser = new ObjectNameParser(t.Name);

                var tableName = parser.ObjectName;
                var schema = string.IsNullOrEmpty(t.Schema) ? parser.SchemaName : t.Schema;

                var dmTable = new DmTable(tableName);

                if (!String.IsNullOrEmpty(schema))
                    dmTable.Schema = schema;

                dmTable.SyncDirection = t.Direction;

                syncConfiguration.Add(dmTable);
            }

            syncConfiguration.BatchDirectory = string.IsNullOrEmpty(project.Configuration.BatchDirectory) ? null : project.Configuration.BatchDirectory;
            syncConfiguration.SerializationFormat = project.Configuration.SerializationFormat;
            syncConfiguration.UseBulkOperations = project.Configuration.UseBulkOperations;
            syncConfiguration.DownloadBatchSizeInKB = (int)Math.Min(Int32.MaxValue, project.Configuration.DownloadBatchSizeInKB);
            syncConfiguration.ConflictResolutionPolicy = project.Configuration.ConflictResolutionPolicy;

            SyncAgent agent = new SyncAgent(clientprovider, serverProvider, syncConfiguration);

            agent.SyncProgress += SyncProgress;
            // synchronous call
            var syncContext = agent.SynchronizeAsync().GetAwaiter().GetResult();
            agent.SyncProgress -= SyncProgress;

            var tsEnded = TimeSpan.FromTicks(syncContext.CompleteTime.Ticks);
            var tsStarted = TimeSpan.FromTicks(syncContext.StartTime.Ticks);

            var durationTs = tsEnded.Subtract(tsStarted);
            var durationstr = $"{durationTs.Hours}:{durationTs.Minutes}:{durationTs.Seconds}.{durationTs.Milliseconds}";

            Console.ForegroundColor = ConsoleColor.Green;
            var s = $"Synchronization done. " + Environment.NewLine +
                    $"\tTotal changes downloaded: {syncContext.TotalChangesDownloaded} " + Environment.NewLine+
                    $"\tTotal changes uploaded: {syncContext.TotalChangesUploaded}" + Environment.NewLine +
                    $"\tTotal duration :{durationstr} ";

            Console.WriteLine(s);
            Console.ResetColor();


            return syncContext;

        }


        private static void SyncProgress(object sender, SyncProgressEventArgs e)
        {
            var sessionId = e.Context.SessionId.ToString();

            switch (e.Context.SyncStage)
            {
                case SyncStage.BeginSession:
                    Console.WriteLine($"Begin Session.");
                    break;
                case SyncStage.EndSession:
                    Console.WriteLine($"End Session.");
                    break;
                case SyncStage.EnsureScopes:
                    Console.WriteLine($"Ensure Scopes");
                    break;
                case SyncStage.EnsureConfiguration:
                    Console.WriteLine($"Ensure Configuration");
                    if (e.Configuration != null)
                        Console.WriteLine($"\tConfiguration readed. {e.Configuration.ScopeSet.Tables.Count} table(s) involved.");
                    break;
                case SyncStage.EnsureDatabase:
                    Console.WriteLine($"Ensure Database");
                    break;
                case SyncStage.SelectingChanges:
                    Console.WriteLine($"Selecting changes...");
                    break;
                case SyncStage.SelectedChanges:
                    Console.WriteLine($"Changes selected : {e.ChangesStatistics.TotalSelectedChanges}");
                    break;
                case SyncStage.ApplyingChanges:
                    Console.WriteLine($"Applying changes...");
                    break;
                case SyncStage.ApplyingInserts:
                    Console.WriteLine($"\tApplying Inserts : {e.ChangesStatistics.AppliedChanges.Where(ac => ac.State == DmRowState.Added).Sum(ac => ac.ChangesApplied) }");
                    break;
                case SyncStage.ApplyingDeletes:
                    Console.WriteLine($"\tApplying Deletes : {e.ChangesStatistics.AppliedChanges.Where(ac => ac.State == DmRowState.Deleted).Sum(ac => ac.ChangesApplied) }");
                    break;
                case SyncStage.ApplyingUpdates:
                    Console.WriteLine($"\tApplying Updates : {e.ChangesStatistics.AppliedChanges.Where(ac => ac.State == DmRowState.Modified).Sum(ac => ac.ChangesApplied) }");
                    break;
                case SyncStage.AppliedChanges:
                    Console.WriteLine($"Changes applied : {e.ChangesStatistics.TotalAppliedChanges}");
                    break;
                case SyncStage.WriteMetadata:
                    if (e.Scopes != null)
                    {
                        Console.WriteLine($"Writing Scopes : ");
                        e.Scopes.ForEach(sc => Console.WriteLine($"\t{sc.Id} synced at {sc.LastSync}. "));
                    }
                    break;
                case SyncStage.CleanupMetadata:
                    Console.WriteLine($"CleanupMetadata");
                    break;
            }
        }


        /// <summary>
        /// Get all projects saved in SQLite database
        /// </summary>
        private void GetProjectsList()
        {
            Console.WriteLine($"{"PROJECT",-32}{"SERVER PROVIDER",-24}{"CLIENT PROVIDER",-24}{"TABLES",-0}");

            List<Project> projects = DataStore.Current.LoadAllProjects();

            foreach (var project in projects)
            {
                var projectName = project.Name;
                var projectNameLength = project.Name.Length;

                if (projectNameLength > 32)
                    projectName = $"{projectName.Substring(0, 28)}...";

                Console.WriteLine($"{projectName,-32}" +
                    $"{project.ServerProvider.ProviderType.ToString(),-24}" +
                    $"{project.ClientProvider.ProviderType.ToString(),-24}" +
                    $"{project.Tables.Count,-0}");
            }

        }

        /// <summary>
        /// Delete a project
        /// </summary>
        private void RemoveProject(string value)
        {
            if (String.IsNullOrEmpty(value))
                throw new Exception("Removing a project requires a name. Ex : dotnet sync --remove project01");

            Project project = DataStore.Current.LoadProject(value);

            if (project == null)
                throw new Exception($"Project {value} does not exists.");

            DataStore.Current.DeleteProject(project);

            Console.WriteLine($"Project {project.Name} deleted.");
        }

        /// <summary>
        /// Create a project
        /// </summary>
        private void CreateProject(string value)
        {
            if (String.IsNullOrEmpty(value))
                throw new Exception("Creating a project requires a name. Ex : dotnet sync --new project01");

            Project project = Project.CreateProject(value);
            DataStore.Current.SaveProject(project);
            Console.WriteLine($"Project {project.Name} created.");
        }

        /// <summary>
        /// Get project info
        /// </summary>
        private void GetProjectInfo(string value)
        {
            if (String.IsNullOrEmpty(value))
                throw new Exception("Loading a project requires a name. Ex : dotnet sync --load project01");

            Project project = DataStore.Current.LoadProject(value);

            if (project == null)
                throw new Exception($"Project {value} does not exists.");

            Console.WriteLine($"{"PROJECT",-32} {project.Name}");
            Console.WriteLine($"{"SERVER PROVIDER",-32} {project.ServerProvider.ProviderType.ToString()}");
            Console.WriteLine($"{"SERVER PROVIDER CS",-32} {project.ServerProvider.ConnectionString}");
            Console.WriteLine($"{"CLIENT PROVIDER",-32} {project.ClientProvider.ProviderType.ToString()}");
            Console.WriteLine($"{"CLIENT PROVIDER CS",-32} {project.ClientProvider.ConnectionString}");
            Console.WriteLine($"{"CONF CONFLICT",-32} {project.Configuration.ConflictResolutionPolicy.ToString()}");
            Console.WriteLine($"{"CONF BATCH DIR",-32} {project.Configuration.BatchDirectory}");
            Console.WriteLine($"{"CONF BATCH SIZE",-32} {project.Configuration.DownloadBatchSizeInKB}");
            Console.WriteLine($"{"CONF SERIALIZATION",-32} {project.Configuration.SerializationFormat.ToString()}");
            Console.WriteLine($"{"CONF BULK OPERATIONS",-32} {project.Configuration.UseBulkOperations.ToString()}");
            Console.WriteLine();
            Console.WriteLine($"{"TABLE",-32}{"SCHEMA",-24}{"DIRECTION",-16}{"ORDER",-0}");

            foreach (var table in project.Tables.OrderBy(tbl => tbl.Order))
            {
                var tableName = table.Name;
                var tableNameLength = table.Name.Length;

                if (tableNameLength > 32)
                    tableName = $"{tableName.Substring(0, 28)}...";

                Console.WriteLine($"{tableName,-32}" +
                    $"{table.Schema,-24}" +
                    $"{table.Direction.ToString(),-16}" +
                    $"{table.Order.ToString(),-0}");
            }
        }
    }
}
