using Dotmim.Sync.Extensions;
using Dotmim.Sync.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data.Common;

namespace Dotmim.Sync
{
    /// <summary>
    /// Sync logger factory extensions.
    /// </summary>
    public static class SyncLoggerFactoryExtensions
    {
        /// <summary>
        /// Add SyncLogger to the logging pipeline.
        /// </summary>
        public static ILoggingBuilder AddSyncLogger(this ILoggingBuilder builder, Action<SyncLoggerOptions> configure = null)
        {
            Guard.ThrowIfNull(builder);

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

        /// <summary>
        /// Returns a string that represents the current connection as a json payload.
        /// </summary>
        public static string ToLogString(this DbConnection connection)
        {
            if (connection == null)
                return "null";

            var serializer = SerializersFactory.JsonSerializerFactory.GetSerializer();

            var task = serializer.SerializeAsync(new { connection.DataSource, connection.Database, State = connection.State.ToString() });

            return System.Text.Encoding.UTF8.GetString(task.Result);
        }

        /// <summary>
        /// Returns a string that represents the current transaction as a json payload.
        /// </summary>
        public static string ToLogString(this DbTransaction transaction)
        {
            if (transaction == null)
                return "null";

            return transaction.Connection != null ? "In progress" : "Done";
        }

        /// <summary>
        /// Returns a string that represents the current command as a json payload.
        /// </summary>
        public static string ToLogString(this DbCommand command)
        {
            if (command == null)
                return "null";

            var serializer = SerializersFactory.JsonSerializerFactory.GetSerializer();

            var parameters = new List<object>();
            if (command.Parameters != null && command.Parameters.Count > 0)
            {

                foreach (DbParameter p in command.Parameters)
                    parameters.Add(new { Name = p.ParameterName, p.Value });
            }

            var s = serializer.Serialize(new { command.CommandText, Parameters = parameters });

            return s.ToUtf8String();
        }
    }
}