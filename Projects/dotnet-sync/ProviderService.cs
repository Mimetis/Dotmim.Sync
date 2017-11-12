using System;
using System.Collections.Generic;

namespace Dotmim.Sync.Tools
{
    internal class ProviderService
    {
        private readonly string projectName;
        private List<Argument> args;

        public ProviderService(string projectName, List<Argument> arguments)
        {
            this.projectName = projectName;
            this.args = arguments;
        }

        internal void Execute()
        {
            if (args.Count == 0)
                throw new Exception("No argument specified. See help: dotnet sync provider --help");

            if (string.IsNullOrEmpty(projectName))
                throw new Exception("No project name specified. See help: dotnet sync provider --help");


            Project project = DataStore.Current.LoadProject(projectName);

            if (project == null)
                throw new Exception($"Project {projectName} does not exists. See help: dotnet sync --help");

            ProviderType providerType = ProviderType.SqlServer;
            String connectionString = null;
            SyncType syncType = SyncType.Server;

            bool providerSpecified = false, connectionStringSpecified = false, syncTypeSpecified = false;

            foreach (var arg in args)
            {
                switch (arg.ArgumentType)
                {
                    case ArgumentType.ProviderProviderType:

                        if (string.IsNullOrWhiteSpace(arg.Value))
                            throw new Exception("Provider type can't be null. See help: dotnet provider --help");

                        var ptype = arg.Value.Trim().ToLowerInvariant();

                        if (ptype == "sqlserver")
                            providerType = ProviderType.SqlServer;
                        if (ptype == "sqlite")
                            providerType = ProviderType.Sqlite;

                        providerSpecified = true;

                        break;
                    case ArgumentType.ProviderConnectionString:

                        if (string.IsNullOrWhiteSpace(arg.Value))
                            throw new Exception("Provider connection string can't be null. See help: dotnet provider --help");

                        connectionString = arg.Value.Trim().ToLowerInvariant();

                        connectionStringSpecified = true;

                        break;
                    case ArgumentType.ProviderSyncType:
                        if (string.IsNullOrWhiteSpace(arg.Value))
                            throw new Exception("Provider Sync type can't be null. See help: dotnet provider --help");

                        var stype = arg.Value.Trim().ToLowerInvariant();

                        if (stype == "server")
                            syncType = SyncType.Server;
                        if (stype == "client")
                            syncType = SyncType.Client;

                        syncTypeSpecified = true;

                        break;
                }

            }

            if (!syncTypeSpecified)
                throw new Exception("Specifying provider sync type is mandatory. See help: dotnet sync provider --help");

            if (syncType == SyncType.Server)
            {
                project.ServerProvider.ProviderType = providerSpecified ?  providerType : project.ServerProvider.ProviderType;
                project.ServerProvider.ConnectionString = connectionStringSpecified ? connectionString : project.ServerProvider.ConnectionString;
            }
            else
            {
                project.ClientProvider.ProviderType = providerSpecified ? providerType : project.ClientProvider.ProviderType;
                project.ClientProvider.ConnectionString = connectionStringSpecified ? connectionString : project.ClientProvider.ConnectionString;
            }

            // saving project
            DataStore.Current.SaveProject(project);

            Console.WriteLine($"Provider {syncType} saved to project {project.Name}");
        }

    }
}