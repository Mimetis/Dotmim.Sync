using System;
using System.Collections.Generic;
using System.IO;

namespace Dotmim.Sync.Tools
{
    internal class ConfigurationService
    {
        private readonly string projectName;
        private readonly List<Argument> args;

        public ConfigurationService(string projectName, List<Argument> args)
        {
            this.projectName = projectName;
            this.args = args;
        }

        internal void Execute()
        {
            if (args.Count == 0)
                throw new Exception("No argument specified. See help: dotnet sync Conf --help");

            if (string.IsNullOrEmpty(projectName))
                throw new Exception("No project name specified. See help: dotnet sync Conf --help");


            Project project = DataStore.Current.LoadProject(projectName);

            if (project == null)
                throw new Exception($"Project {projectName} does not exists. See help: dotnet sync Conf --help");

            Configuration configuration = project.Configuration;

            foreach (var arg in args)
            {
                switch (arg.ArgumentType)
                {
                    case ArgumentType.ConfigurationBatchDirectory:
                        if (string.IsNullOrWhiteSpace(arg.Value))
                            configuration.BatchDirectory = Path.Combine(Path.GetTempPath(), "DotmimSync");
                        else
                            configuration.BatchDirectory = arg.Value;
                        break;
                    case ArgumentType.ConfigurationBatchSize:

                        if (!Int64.TryParse(arg.Value, out long batchSize))
                            throw new Exception("Batch size must be an In64 value. See help: dotnet sync Conf --help");

                        configuration.DownloadBatchSizeInKB = batchSize;
                        break;
                    case ArgumentType.ConfigurationBulkOperations:

                        if (!bool.TryParse(arg.Value, out bool bulkOpe))
                            throw new Exception("Use bulk operation argument must be an True/False value. See help: dotnet sync Conf --help");

                        configuration.UseBulkOperations = bulkOpe;
                        break;
                    case ArgumentType.ConfigurationConflict:

                        var conflict = arg.Value.Trim().ToLowerInvariant();

                        if (conflict != "serverwins" && conflict != "clientwins")
                            throw new Exception("Conflict resolution must be serverwins or clientwins. See help: dotnet sync Conf --help");

                        configuration.ConflictResolutionPolicy = conflict == "serverwins" ? Enumerations.ConflictResolutionPolicy.ServerWins : Enumerations.ConflictResolutionPolicy.ClientWins;
                        break;
                    case ArgumentType.ConfigurationFormat:
                        var format = arg.Value.Trim().ToLowerInvariant();

                        if (format != "json" && format != "dm")
                            throw new Exception("Conflict resolution must be json or dm. See help: dotnet sync Conf --help");

                        configuration.SerializationFormat = format == "json" ? Enumerations.SerializationFormat.Json : Enumerations.SerializationFormat.Binary;
                        break;
                }

            }

            // saving project
            DataStore.Current.SaveProject(project);

            Console.WriteLine($"Configuration saved to project {project.Name}");
        }
    }
}