using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

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
            Console.WriteLine($"{"TABLE",-32}{"SCHEMA",-24}{"DIRECTION",-0}");

            foreach (var table in project.Tables)
            {
                var tableName = table.Name;
                var tableNameLength = table.Name.Length;

                if (tableNameLength > 32)
                    tableName = $"{tableName.Substring(0, 28)}...";

                Console.WriteLine($"{tableName,-32}" +
                    $"{table.Schema,-24}" +
                    $"{table.Direction.ToString()}");
            }
        }
    }
}
