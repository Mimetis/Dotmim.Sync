using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;

namespace Dotmim.Sync
{
    /// <summary>
    /// Sync Logger Provider.
    /// </summary>
    public class SyncLoggerProvider : ILoggerProvider
    {
        private SyncLoggerOptions syncOptions = new();

        /// <summary>
        /// Gets get default logger options.
        /// </summary>
        public IConfigureOptions<LoggerFilterOptions> ConfigureOptions { get; }

        /// <summary>
        /// Gets get Sync logger options.
        /// </summary>
        public IConfigureOptions<SyncLoggerOptions> ConfigureSyncOptions { get; }

        /// <summary>
        /// Initializes a new instance of the <see cref="SyncLoggerProvider"/> class.
        /// </summary>
        public SyncLoggerProvider(IConfigureOptions<LoggerFilterOptions> options = null, IConfigureOptions<SyncLoggerOptions> syncOptions = null)
        {
            this.ConfigureOptions = options;
            this.ConfigureSyncOptions = syncOptions;
        }

        /// <summary>
        /// Create a new logger.
        /// </summary>
        public ILogger CreateLogger(string categoryName)
        {
            var syncLogger = new SyncLogger();

            if (this.ConfigureOptions != null)
                this.ConfigureOptions.Configure(this.syncOptions);

            // Override MinLevel, if coming from typed Sync Options
            if (this.ConfigureSyncOptions != null)
                this.ConfigureSyncOptions.Configure(this.syncOptions);

            if (this.syncOptions != null)
            {
                syncLogger.MinimumLevel = this.syncOptions.MinLevel;

                if (this.syncOptions.OutputWriters != null && this.syncOptions.OutputWriters.Count > 0)
                    syncLogger.OutputWriters.AddRange(this.syncOptions.OutputWriters);
                else
                    syncLogger.AddDebug();
            }

            return syncLogger;
        }

        /// <summary>
        /// Dispose the logger.
        /// </summary>
        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Dispose the logger.
        /// </summary>
        protected virtual void Dispose(bool cleanup)
        {
            if (cleanup)
            {
                this.syncOptions = null;
            }
        }
    }
}