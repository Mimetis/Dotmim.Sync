using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Dotmim.Sync
{
    public class SyncLoggerOptions : LoggerFilterOptions
    {
        internal List<OutputWriter> outputWriters = new List<OutputWriter>();

        /// <summary>
        /// Adds an output to console when logging something
        /// </summary>
        public SyncLoggerOptions AddConsole()
        {
            if (!outputWriters.Any(w => w.Name == "Console"))
                outputWriters.Add(new ConsoleWriter());

            return this;
        }

        /// <summary>
        /// Adds an output to diagnostics debug window when logging something
        /// </summary>
        public SyncLoggerOptions AddDebug()
        {
            if (!outputWriters.Any(w => w.Name == "Debug"))
                outputWriters.Add(new DebugWriter());

            return this;
        }

        /// <summary>
        /// Adds minimum level : 0 Trace, 1 Debug, 2 Information, 3, Warning, 4 Error, 5 Critical, 6 None
        /// </summary>
        public SyncLoggerOptions SetMinimumLevel(LogLevel minimumLevel)
        {
            this.MinLevel = minimumLevel;
            return this;
        }

    }
}
