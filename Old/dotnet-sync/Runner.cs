using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Tools
{
    public class Runner
    {
        public static void Execute(string[] args)
        {
            // Ensure database is created
            DataStore.Current.EnsureDatabase();

            // parsing line
            ArgLine line = new ArgLine(args);
            line.Parse();

            if (line.Command == null || line.Command.CommandType == CommandType.None)
               throw new Exception("No specific command called. can be either project, table, conf or yaml. See help: dotnet sync --help");

            switch (line.Command.CommandType)
            {
                case CommandType.Project:
                    new ProjectService(line.ProjectName, line.Arguments).Execute();
                    break;
                case CommandType.Provider:
                    new ProviderService(line.ProjectName, line.Arguments).Execute();
                    break;
                case CommandType.Table:
                    new TableService(line.ProjectName, line.Arguments).Execute();
                    break;
                case CommandType.Conf:
                    new ConfigurationService(line.ProjectName, line.Arguments).Execute();
                    break;
                case CommandType.Yaml:
                    new YamlService(line.Arguments).Execute();
                    break;
            }
        }
    }
}
