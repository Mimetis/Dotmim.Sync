using System;
using System.Collections.Generic;
using System.IO;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;

namespace Dotmim.Sync.Tools
{
    internal class YamlService
    {
        private List<Argument> args;

        public YamlService(List<Argument> arguments)
        {
            this.args = arguments;
        }

        internal void Execute()
        {
            if (args.Count == 0)
                throw new Exception("No argument specified. See help: dotnet sync yaml --help");

            string fileName = null;
            foreach (var arg in args)
            {
                switch (arg.ArgumentType)
                {
                    case ArgumentType.YamlFileName:
                        fileName = arg.Value.Trim();
                        break;
                }
            }

            if (string.IsNullOrEmpty(fileName))
                throw new Exception("Yaml command need at least a fileName to load. Try dotnet sync yaml --help to get availables arguments for yaml command.");

            this.LoadProject(fileName);
        }

        private void LoadProject(string fileName)
        {
            var serializer = new DeserializerBuilder()
                            .WithNamingConvention(new CamelCaseNamingConvention())
                            .Build();

            FileInfo fi = new FileInfo(fileName);

            if (string.IsNullOrEmpty(fi.Extension))
                fileName += ".yml";
            else if (fi.Extension.ToLowerInvariant() != ".yml")
                throw new Exception("YAML filename extension should be blank or .yml");

            if (!File.Exists(fileName))
                throw new Exception($"Can't load YAML file {fileName}.");

            using (StreamReader streamReader = new StreamReader(fileName))
            {
                // load project from file
                var project = serializer.Deserialize<Project>(streamReader);

                // try to save it
                DataStore.Current.SaveProject(project);

                Console.WriteLine($"YAML file {fileName} correctly loaded. Project {project.Name} with {project.Tables.Count} table(s) loaded.");
            }
        }
    }
}