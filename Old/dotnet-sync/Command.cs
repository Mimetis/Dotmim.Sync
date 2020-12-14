using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Tools
{
    public class Command
    {
        /// <summary>
        /// Gets or Sets the name for the command
        /// </summary>
        public String Name { get; set; }

        /// <summary>
        /// Gets or Sets the command description. Used when called with help arg
        /// </summary>
        public String Description { get; set; }

        /// <summary>
        /// gets the command type associated
        /// </summary>
        public CommandType CommandType { get; }

        public Command(CommandType commandType)
        {
            this.CommandType = commandType;
        }

    }
}
