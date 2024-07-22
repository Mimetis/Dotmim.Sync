using Microsoft.Extensions.Logging;
using System;

namespace Dotmim.Sync
{

    /// <summary>
    /// Sync Events Ids.
    /// </summary>
    public static class SyncEventsExtensions
    {
        /// <summary>
        /// Log a message with the Trace level. If the logger is not enabled, do nothing.
        /// </summary>
        public static void LogTrace<T>(this ILogger logger, EventId id, T value)
            where T : class
        {
            Guard.ThrowIfNull(logger);

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

        /// <summary>
        /// Log a message with the Debug level. If the logger is not enabled, do nothing.
        /// </summary>
        public static void LogCritical<T>(this ILogger logger, EventId id, T value)
            where T : class
        {
            Guard.ThrowIfNull(logger);
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

        /// <summary>
        /// Log a message with the Debug level. If the logger is not enabled, do nothing.
        /// </summary>
        public static void LogDebug<T>(this ILogger logger, EventId id, T value)
            where T : class
        {
            Guard.ThrowIfNull(logger);
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

        /// <summary>
        /// Log a message with the Error level. If the logger is not enabled, do nothing.
        /// </summary>
        public static void LogInformation<T>(this ILogger logger, EventId id, T value)
            where T : class
        {
            Guard.ThrowIfNull(logger);

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

        /// <summary>
        /// Log error message with the Error level. If the logger is not enabled, do nothing.
        /// </summary>
        public static void LogError<T>(this ILogger logger, EventId id, T value)
            where T : class
        {
            Guard.ThrowIfNull(logger);

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

    /// <summary>
    /// Sync Events Ids.
    /// </summary>
    public static class SyncEventsId
    {

        /// <summary>
        /// Gets the event id for the exception event.
        /// </summary>
        public static EventId Exception => CreateEventId(0, nameof(Exception));

        /// <summary>
        /// Gets the event id for the report event.
        /// </summary>
        public static EventId ReportProgress => CreateEventId(5, nameof(ReportProgress));

        /// <summary>
        /// Gets the event id for the interceptor event.
        /// </summary>
        public static EventId Interceptor => CreateEventId(10, nameof(Interceptor));

        /// <summary>
        /// Creates a new EventId.
        /// </summary>
        internal static EventId CreateEventId(int id, string eventName)
        {
            return new EventId(GetSyncEventId(eventName), eventName);
        }

        private static int GetSyncEventId(string eventName)
        {

            string concatInt = string.Empty;
            for (int i = 0; i < eventName.Length; i++)
            {
                var letter = Convert.ToInt32(eventName[i]).ToString();
                letter = letter.Substring(letter.Length - 1);
                if (string.IsNullOrEmpty(letter))
                    letter = "0";
                concatInt += letter;
            }

            return int.Parse(concatInt);
        }
    }
}