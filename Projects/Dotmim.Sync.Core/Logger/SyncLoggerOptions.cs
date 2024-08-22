using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using System.Linq;

namespace Dotmim.Sync
{
    /// <summary>
    /// Sync Logger Options.
    /// </summary>
    public class SyncLoggerOptions : LoggerFilterOptions
    {
        /// <summary>
        /// Gets the output writers.
        /// </summary>
        internal List<OutputWriter> OutputWriters { get; } = [];

        /// <summary>
        /// Adds an output to console when logging something.
        /// </summary>
        public SyncLoggerOptions AddConsole()
        {
            if (!this.OutputWriters.Any(w => w.Name == "Console"))
                this.OutputWriters.Add(new ConsoleWriter());

            return this;
        }

        /// <summary>
        /// Adds an output to diagnostics debug window when logging something.
        /// </summary>
        public SyncLoggerOptions AddDebug()
        {
            if (!this.OutputWriters.Any(w => w.Name == "Debug"))
                this.OutputWriters.Add(new DebugWriter());

            return this;
        }

        /// <summary>
        /// Adds minimum level : 0 Trace, 1 Debug, 2 Information, 3, Warning, 4 Error, 5 Critical, 6 None.
        /// </summary>
        public SyncLoggerOptions SetMinimumLevel(LogLevel minimumLevel)
        {
            this.MinLevel = minimumLevel;
            return this;
        }
    }
}