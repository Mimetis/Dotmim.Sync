using Dotmim.Sync.Enumerations;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync
{

    public static class SyncEventsExtensions
    {
        public static void LogTrace<T>(this ILogger logger, EventId id, T value) where T : class
        {
            if (!logger.IsEnabled(LogLevel.Trace))
                return;

            if (value is string)
            {
                logger.LogTrace(id, value as string, null);
                return;
            }

            var log = SyncLogger.GetLogMessageFrom(value, id);
            logger.LogTrace(id, log.Message, log.Args);
        }

        public static void LogCritical<T>(this ILogger logger, EventId id, T value) where T : class
        {
            if (!logger.IsEnabled(LogLevel.Critical))
                return;

            if (value is string)
            {
                logger.LogCritical(id, value as string, null);
                return;
            }

            var log = SyncLogger.GetLogMessageFrom(value, id);
            logger.LogCritical(id, log.Message, log.Args);
        }

        public static void LogDebug<T>(this ILogger logger, EventId id, T value) where T : class
        {
            if (!logger.IsEnabled(LogLevel.Debug))
                return;

            if (value is string)
            {
                logger.LogDebug(id, value as string, null);
                return;
            }

            var log = SyncLogger.GetLogMessageFrom(value, id);
            logger.LogDebug(id, log.Message, log.Args);
        }
        public static void LogInformation<T>(this ILogger logger, EventId id, T value) where T : class
        {
            if (!logger.IsEnabled(LogLevel.Information))
                return;

            if (value is string)
            {
                logger.LogInformation(id, value as string, null);
                return;
            }

            var log = SyncLogger.GetLogMessageFrom(value, id);
            logger.LogInformation(id, log.Message, log.Args);
        }
        public static void LogError<T>(this ILogger logger, EventId id, T value) where T : class
        {
            if (!logger.IsEnabled(LogLevel.Error))
                return;

            if (value is string)
            {
                logger.LogError(id, value as string, null);
                return;
            }

            var log = SyncLogger.GetLogMessageFrom(value, id);
            logger.LogError(id, log.Message, log.Args);
        }

    }

    public static partial class SyncEventsId
    {
        private static EventId CreateEventId(int id, string eventName) => new EventId(id, eventName);

        public static EventId Exception => CreateEventId(0, nameof(Exception));
        public static EventId ReportProgress => CreateEventId(5, nameof(ReportProgress));
        public static EventId Interceptor => CreateEventId(10, nameof(Interceptor));
    }
}
