using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
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

            // Filter is used to se the minimum level of log level for a particular logging provider.
            // this minimum level is independant from the minimum level set bu the logger itself.
            // For ... some reasons.. I guess ... :)
            builder.AddFilter<SyncLoggerProvider>(null, LogLevel.Trace);

            // Add an action to configure the SyncLogger
            if (configure != null)
                builder.Services.Configure(configure);

            return builder;
        }

        public static string ToLogString(this DbConnection connection)
        {
            if (connection == null)
                return "null";

            return JsonConvert.SerializeObject(new { connection.DataSource, connection.Database, State=connection.State.ToString() });
        }
        public static string ToLogString(this DbTransaction transaction)
        {
            if (transaction == null)
                return "null";

            return transaction.Connection != null ? "In progress" : "Done";
        }
        public static string ToLogString(this DbCommand command)
        {
            if (command == null)
                return "null";

            return command.CommandText;
        }
    }
}
