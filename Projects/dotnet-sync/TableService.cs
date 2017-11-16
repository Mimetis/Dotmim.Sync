using Dotmim.Sync.Enumerations;
using System;
using System.Collections.Generic;

namespace Dotmim.Sync.Tools
{
    internal class TableService
    {
        private List<Argument> args;
        private readonly String projectName;
        public TableService(string projectName, List<Argument> arguments)
        {
            this.projectName = projectName;
            this.args = arguments;
        }

        internal void Execute()
        {
            if (args.Count == 0)
                throw new Exception("No argument specified. See help: dotnet sync Table --help");

            if (String.IsNullOrEmpty(projectName))
                throw new Exception("Table commands need a project name. See help: dotnet sync Table --help");

            Project project = DataStore.Current.LoadProject(projectName);

            if (project == null)
                throw new Exception($"Project {projectName} does not exists.");

            Table table = new Table();

            foreach (var arg in args)
            {
                switch (arg.ArgumentType)
                {
                    case ArgumentType.TableAdd:
                        table.Name = arg.Value;
                        break;
                    case ArgumentType.TableOrder:

                        if (!int.TryParse(arg.Value, out int neworder))
                            throw new Exception("Table order is not specified correctly. See help: dotnet sync table --help ");

                        table.Order = neworder;
                        break;
                    case ArgumentType.TableSchema:
                        table.Schema = arg.Value;
                        break;
                    case ArgumentType.TableRemove:
                        if (args.Count > 1)
                            throw new Exception("Too much arguments for deleting a project. See help: dotnet sync table --help");

                        DataStore.Current.DeleteTable(this.projectName, arg.Value);

                        // exit foreach
                        return;
                    case ArgumentType.TableDirection:
                        if (arg.Value == "bidirectional" || arg.Value == "b")
                            table.Direction = SyncDirection.Bidirectional;
                        if (arg.Value == "uploadonly" || arg.Value == "u")
                            table.Direction = SyncDirection.UploadOnly;
                        if (arg.Value == "downloadonly" || arg.Value == "d")
                            table.Direction = SyncDirection.DownloadOnly;
                        break;
                }

            }

            if (string.IsNullOrEmpty(table.Name))
                throw new Exception("Table name is mandatory. Example : dotnet sync table --add Client. See help: dotnet sync table --help");

            DataStore.Current.SaveTable(this.projectName, table);

            Console.WriteLine($"Table {table.Name} added to project {this.projectName}.");
        }

    }
}