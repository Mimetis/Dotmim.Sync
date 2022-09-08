using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync
{
    public class SyncLoggerProvider : ILoggerProvider
    {
        SyncLoggerOptions syncOptions = new SyncLoggerOptions();

        /// <summary>
        /// Get default logger options
        /// </summary>
        public IConfigureOptions<LoggerFilterOptions> ConfigureOptions { get; }

        /// <summary>
        /// Get Sync logger options
        /// </summary>
        public IConfigureOptions<SyncLoggerOptions> ConfigureSyncOptions { get; }


        /// <summary>
        /// Get a sync logger provider. Options can come from both LoggerFilterOptions or from typed SyncLoggerOptions
        /// </summary>
        /// <param name="options"></param>
        /// <param name="syncOptions"></param>
        public SyncLoggerProvider(IConfigureOptions<LoggerFilterOptions> options = null, IConfigureOptions<SyncLoggerOptions> syncOptions = null)
        {
            this.ConfigureOptions = options;
            this.ConfigureSyncOptions = syncOptions;
        }

        public ILogger CreateLogger(string categoryName)
        {
            var syncLogger = new SyncLogger();

            if (this.ConfigureOptions != null)
                this.ConfigureOptions.Configure(syncOptions);

            // Override MinLevel, if coming from typed Sync Options
            if (this.ConfigureSyncOptions != null)
                this.ConfigureSyncOptions.Configure(syncOptions);

            if (syncOptions != null)
            {
                syncLogger.MinimumLevel = syncOptions.MinLevel;

                if (syncOptions.outputWriters != null && syncOptions.outputWriters.Count > 0)
                    syncLogger.outputWriters = syncOptions.outputWriters;
                else
                    syncLogger.AddDebug();
            }

            return syncLogger;
        }

        public void Dispose() { }
    }
}
