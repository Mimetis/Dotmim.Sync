using System;
using System.Collections.Generic;

namespace Dotmim.Sync.Tools
{
    internal class ProviderService
    {
        private readonly string projectName;
        private List<Argument> arguments;

        public ProviderService(string projectName, List<Argument> arguments)
        {
            this.projectName = projectName;
            this.arguments = arguments;
        }

        internal void Execute()
        {
            throw new NotImplementedException();
        }
    }
}