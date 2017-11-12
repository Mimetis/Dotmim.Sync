using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Dotmim.Sync.Tools
{
    public class ArgLine
    {
        private readonly string[] args;

        /// <summary>
        /// Gets or Sets the project name
        /// </summary>
        public String ProjectName { get; set; }

        /// <summary>
        /// Gets or Sets the Command option associated
        /// </summary>
        public Command Command { get; set; }

        /// <summary>
        /// Gets or Sets the arguments associated with the current command
        /// </summary>
        public List<Argument> Arguments { get; set; }

        public ArgLine(string[] args)
        {
            this.args = args;
        }

        /// <summary>
        /// Parsing the input line
        /// </summary>
        internal void Parse()
        {
            // parse the command called
            (var projectName, var command, var args) = ParseCommand2();

            this.ProjectName = projectName;
            this.Command = command;
            this.Arguments = args;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        private (String projectName, Command command, List<Argument> args) ParseCommand2()
        {
            var commandType = CommandType.None;
            var arguments = new List<Argument>();
            var projectName = string.Empty;
            var isArgumentType = false;
            var parsedValue = string.Empty;

            var queue = new Queue<string>();

            // first of all get the Command Type and remove it
            foreach (var arg in args)
            {
                var k = CheckIfCommand(arg);

                if (k.IsCommandType)
                    commandType = k.CommandType;
                else
                    queue.Enqueue(arg);
            }

            if (commandType == CommandType.None)
                commandType = CommandType.Project;

            // for each arg, we check if it's an alone component (so it's a project name)
            // or if it's a key value pair
            string argType = string.Empty;
            string argValue = string.Empty;

            while (queue.Count > 0)
            {
                // get next
                var queueArg = queue.Dequeue();

                // parse
                (parsedValue, isArgumentType) = ParseValue(queueArg);

                // if argument type
                if (isArgumentType)
                    argType = parsedValue;
                else
                    argValue = parsedValue;

                // if we have an argumentType and next one is an argumentType too, we create the Argument without value
                if (isArgumentType)
                {
                    // Get next one, without dequeuing
                    queue.TryPeek(out string nextQueueArg);

                    bool nextValueIsArgType;
                    string nextValue;
                    
                    // simulate next is arg type, but we are just at the end of args
                    if (string.IsNullOrEmpty(nextQueueArg))
                        nextValueIsArgType = true;
                    else
                        (nextValue, nextValueIsArgType) = ParseValue(nextQueueArg);

                    // if next is a type, even if no value we add it
                    if (nextValueIsArgType)
                    {
                        arguments.Add(ParseArgument(argType, argValue, commandType));
                        argType = null;
                        argValue = null;
                        continue;
                    }
                }

                // we have a value. we must have an argument type already. if not, it's the projectname
                if (!isArgumentType && !String.IsNullOrEmpty(argValue) && String.IsNullOrEmpty(argType))
                {
                    // try to affect an other project name, not possible
                    // probably a command type not spelled correctly
                    if (!string.IsNullOrEmpty(projectName))
                        throw new Exception("Incorrect syntax. be sure your command is correct. See help: dotnet sync --help");

                    projectName = argValue.Trim();
                    argType = null;
                    argValue = null;
                    continue;
                }

                // we have a combo
                if (!String.IsNullOrEmpty(argType) && !String.IsNullOrEmpty(argValue))
                {
                    arguments.Add(ParseArgument(argType, argValue, commandType));
                    argType = null;
                    argValue = null;
                }

            }

            return (projectName, new Command(commandType), arguments);
        }

        private (CommandType CommandType, bool IsCommandType) CheckIfCommand(string arg)
        {
            if (string.Equals(arg, "yaml", StringComparison.InvariantCultureIgnoreCase))
                return (CommandType.Yaml, true);

            if (string.Equals(arg, "provider", StringComparison.InvariantCultureIgnoreCase))
                return (CommandType.Provider, true);

            if (string.Equals(arg, "table", StringComparison.InvariantCultureIgnoreCase))
                return (CommandType.Table, true);

            if (string.Equals(arg, "conf", StringComparison.InvariantCultureIgnoreCase))
                return (CommandType.Conf, true);

            return (CommandType.None, false);
        }

        private (string parsedValue, bool isTerm) ParseValue(string value)
        {
            bool isLongterm = false;
            bool isShortTerm = true;
            string result = string.Empty;

            // check if it's a term
            isLongterm = value.StartsWith("--") ? true : false;
            isShortTerm = value.StartsWith("-") ? true : false;

            // gets an argument term
            if (isLongterm || isShortTerm)
            {
                // get the term
                result = value.Replace(isLongterm ? "--" : "-", string.Empty);
                return (result, true);
            }

            return (value, false);
        }

        private Argument ParseArgument(string argType, string argValue, CommandType commandType)
        {
            bool isLongterm = false;
            ArgumentType argumentType = ArgumentType.None;

            switch (commandType)
            {
                case CommandType.Project:
                    switch (argType)
                    {
                        case "version":
                            isLongterm = true;
                            argumentType = ArgumentType.RootVersion;
                            break;
                        case "v":
                            isLongterm = false;
                            argumentType = ArgumentType.RootVersion;
                            break;
                        case "help":
                            isLongterm = true;
                            argumentType = ArgumentType.RootHelp;
                            break;
                        case "h":
                            isLongterm = false;
                            argumentType = ArgumentType.RootHelp;
                            break;
                        case "sync":
                            isLongterm = true;
                            argumentType = ArgumentType.RootSync;
                            break;
                        case "s":
                            isLongterm = false;
                            argumentType = ArgumentType.RootSync;
                            break;
                        case "verbose":
                            isLongterm = true;
                            argumentType = ArgumentType.RootVerbose;
                            break;
                        case "new":
                            isLongterm = true;
                            argumentType = ArgumentType.ProjectNew;
                            break;
                        case "n":
                            isLongterm = false;
                            argumentType = ArgumentType.ProjectNew;
                            break;
                        case "info":
                            isLongterm = true;
                            argumentType = ArgumentType.ProjectInfo;
                            break;
                        case "i":
                            isLongterm = false;
                            argumentType = ArgumentType.ProjectInfo;
                            break;
                        case "remove":
                            isLongterm = true;
                            argumentType = ArgumentType.ProjectDelete;
                            break;
                        case "rm":
                        case "r":
                            isLongterm = false;
                            argumentType = ArgumentType.ProjectDelete;
                            break;
                        case "list":
                            isLongterm = true;
                            argumentType = ArgumentType.ProjectList;
                            break;
                        case "l":
                        case "ls":
                            isLongterm = false;
                            argumentType = ArgumentType.ProjectList;
                            break;
                        default:
                            argumentType = ArgumentType.None;
                            break;
                    }
                    break;
                case CommandType.Provider:
                    switch (argType)
                    {
                        // Provider
                        case "providertype":
                            isLongterm = true;
                            argumentType = ArgumentType.ProviderProviderType;
                            break;
                        case "p":
                        case "pt":
                            isLongterm = false;
                            argumentType = ArgumentType.ProviderProviderType;
                            break;
                        case "connectionstring":
                            isLongterm = true;
                            argumentType = ArgumentType.ProviderConnectionString;
                            break;
                        case "c":
                        case "cs":
                            isLongterm = false;
                            argumentType = ArgumentType.ProviderConnectionString;
                            break;
                        case "synctype":
                            isLongterm = true;
                            argumentType = ArgumentType.ProviderSyncType;
                            break;
                        case "s":
                        case "st":
                            isLongterm = false;
                            argumentType = ArgumentType.ProviderSyncType;
                            break;
                        default:
                            argumentType = ArgumentType.None;
                            break;
                    }
                    break;
                case CommandType.Table:
                    switch (argType)
                    {
                        // Tables
                        case "add":
                            isLongterm = true;
                            argumentType = ArgumentType.TableAdd;
                            break;
                        case "a":
                            isLongterm = false;
                            argumentType = ArgumentType.TableAdd;
                            break;
                        case "order":
                            isLongterm = true;
                            argumentType = ArgumentType.TableOrder;
                            break;
                        case "o":
                            isLongterm = false;
                            argumentType = ArgumentType.TableOrder;
                            break;
                        case "schema":
                            isLongterm = true;
                            argumentType = ArgumentType.TableSchema;
                            break;
                        case "s":
                            isLongterm = false;
                            argumentType = ArgumentType.TableSchema;
                            break;
                        case "remove":
                            isLongterm = true;
                            argumentType = ArgumentType.TableRemove;
                            break;
                        case "rm":
                        case "r":
                            isLongterm = false;
                            argumentType = ArgumentType.TableRemove;
                            break;
                        case "direction":
                            isLongterm = true;
                            argumentType = ArgumentType.TableDirection;
                            break;
                        case "d":
                            isLongterm = false;
                            argumentType = ArgumentType.TableDirection;
                            break;
                        default:
                            argumentType = ArgumentType.None;
                            break;
                    }
                    break;
                case CommandType.Conf:
                    switch (argType)
                    {
                        case "conflict":
                            isLongterm = true;
                            argumentType = ArgumentType.ConfigurationConflict;
                            break;
                        case "c":
                            isLongterm = false;
                            argumentType = ArgumentType.ConfigurationConflict;
                            break;
                        case "batchsize":
                            isLongterm = true;
                            argumentType = ArgumentType.ConfigurationBatchSize;
                            break;
                        case "bs":
                        case "s":
                            isLongterm = false;
                            argumentType = ArgumentType.ConfigurationBatchSize;
                            break;
                        case "batchdirectory":
                            isLongterm = true;
                            argumentType = ArgumentType.ConfigurationBatchDirectory;
                            break;
                        case "dir":
                        case "bd":
                        case "d":
                            isLongterm = false;
                            argumentType = ArgumentType.ConfigurationBatchDirectory;
                            break;
                        case "format":
                            isLongterm = true;
                            argumentType = ArgumentType.ConfigurationFormat;
                            break;
                        case "f":
                            isLongterm = false;
                            argumentType = ArgumentType.ConfigurationFormat;
                            break;
                        case "bulkoperations":
                            isLongterm = true;
                            argumentType = ArgumentType.ConfigurationBulkOperations;
                            break;
                        case "bo":
                        case "o":
                            isLongterm = false;
                            argumentType = ArgumentType.ConfigurationBulkOperations;
                            break;
                        default:
                            argumentType = ArgumentType.None;
                            break;
                    }
                    break;
                case CommandType.Yaml:
                    switch (argType)
                    {
                        // Yaml
                        case "f":
                            isLongterm = true;
                            argumentType = ArgumentType.YamlFileName;
                            break;
                        case "file":
                            isLongterm = false;
                            argumentType = ArgumentType.YamlFileName;
                            break;
                        default:
                            argumentType = ArgumentType.None;
                            break;
                    }
                    break;
                default:
                    argumentType = ArgumentType.None;
                    break;
            }

            return new Argument(argumentType, argType, argValue, isLongterm);
        }

    }
}
