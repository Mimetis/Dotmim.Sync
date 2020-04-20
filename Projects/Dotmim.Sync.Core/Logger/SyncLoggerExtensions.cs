using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync
{
    public static class SyncLoggerFactoryExtensions
    {
        /// <summary>
        /// Add SyncLogger to the logging pipeline.
        /// </summary>
        public static ILoggingBuilder AddSyncLogger(this ILoggingBuilder builder, Action<SyncLoggerOptions> configure = null)
        {
            if (builder == null) throw new ArgumentNullException(nameof(builder));

            // Add a singleton for the sync log provider
            builder.Services.AddSingleton<ILoggerProvider, SyncLoggerProvider>();

            // Filter is used to se the minimum level of log leve for a particular logging provider.
            // this minimum level is independant from the minimum level set bu the logger itself.
            // For ... some reasons.. I guess ... :)
            builder.AddFilter<SyncLoggerProvider>(null, LogLevel.Trace);

            // Add an action to configure the SyncLogger
            if (configure != null)
                builder.Services.Configure(configure);

            return builder;
        }
    }
}
