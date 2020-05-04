using Dotmim.Sync.Enumerations;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync
{

    public static class SyncEventsExtensions
    {
        //    /// <summary>
        //    /// Create a Log with an EventId generated from a SyncStage enumeration
        //    /// </summary>
        //    public static void Log(this ILogger logger, LogLevel logLevel, SyncStage stage, string message, params object[] args)
        //        => logger.Log(logLevel, new EventId((int)stage, Enum.GetName(typeof(SyncStage), stage)), message, args);
        //    /// <summary>
        //    /// Create a Log with an EventId generated from a SyncStage enumeration
        //    /// </summary>
        //    public static void Log(this ILogger logger, LogLevel logLevel, SyncStage stage, Exception exception, string message, params object[] args)
        //        => logger.Log(logLevel, new EventId((int)stage, Enum.GetName(typeof(SyncStage), stage)), exception, message, args);
        //    /// <summary>
        //    /// Create a Log with an EventId generated from a SyncStage enumeration
        //    /// </summary>
        //    public static void LogTrace(this ILogger logger, SyncStage stage, string message, params object[] args)
        //         => logger.LogTrace(new EventId((int)stage, Enum.GetName(typeof(SyncStage), stage)), message, args);
        //    /// <summary>
        //    /// Create a Log with an EventId generated from a SyncStage enumeration
        //    /// </summary>
        //    public static void LogTrace(this ILogger logger, SyncStage stage, Exception exception, string message, params object[] args)
        //        => logger.LogTrace(new EventId((int)stage, Enum.GetName(typeof(SyncStage), stage)), exception, message, args);
        //    /// <summary>
        //    /// Create a Log with an EventId generated from a SyncStage enumeration
        //    /// </summary>
        //    public static void LogCritical(this ILogger logger, SyncStage stage, string message, params object[] args)
        //        => logger.LogCritical(new EventId((int)stage, Enum.GetName(typeof(SyncStage), stage)), message, args);
        //    /// <summary>
        //    /// Create a Log with an EventId generated from a SyncStage enumeration
        //    /// </summary>
        //    public static void LogCritical(this ILogger logger, SyncStage stage, Exception exception, string message, params object[] args)
        //        => logger.LogCritical(new EventId((int)stage, Enum.GetName(typeof(SyncStage), stage)), exception, message, args);
        //    /// <summary>
        //    /// Create a Log with an EventId generated from a SyncStage enumeration
        //    /// </summary>
        //    public static void LogDebug(this ILogger logger, SyncStage stage, string message, params object[] args)
        //       => logger.LogDebug(new EventId((int)stage, Enum.GetName(typeof(SyncStage), stage)), message, args);
        //    /// <summary>
        //    /// Create a Log with an EventId generated from a SyncStage enumeration
        //    /// </summary>
        //    public static void LogDebug(this ILogger logger, SyncStage stage, Exception exception, string message, params object[] args)
        //        => logger.LogDebug(new EventId((int)stage, Enum.GetName(typeof(SyncStage), stage)), exception, message, args);
        //    /// <summary>
        //    /// Create a Log with an EventId generated from a SyncStage enumeration
        //    /// </summary>
        //    public static void LogError(this ILogger logger, SyncStage stage, string message, params object[] args)
        //      => logger.LogError(new EventId((int)stage, Enum.GetName(typeof(SyncStage), stage)), message, args);
        //    /// <summary>
        //    /// Create a Log with an EventId generated from a SyncStage enumeration
        //    /// </summary>
        //    public static void LogError(this ILogger logger, SyncStage stage, Exception exception, string message, params object[] args)
        //        => logger.LogError(new EventId((int)stage, Enum.GetName(typeof(SyncStage), stage)), exception, message, args);
        //    /// <summary>
        //    /// Create a Log with an EventId generated from a SyncStage enumeration
        //    /// </summary>
        //    public static void LogInformation(this ILogger logger, SyncStage stage, string message, params object[] args)
        //      => logger.LogInformation(new EventId((int)stage, Enum.GetName(typeof(SyncStage), stage)), message, args);
        //    /// <summary>
        //    /// Create a Log with an EventId generated from a SyncStage enumeration
        //    /// </summary>
        //    public static void LogInformation(this ILogger logger, SyncStage stage, Exception exception, string message, params object[] args)
        //        => logger.LogInformation(new EventId((int)stage, Enum.GetName(typeof(SyncStage), stage)), exception, message, args);
        //    /// <summary>
        //    /// Create a Log with an EventId generated from a SyncStage enumeration
        //    /// </summary>
        //    public static void LogWarning(this ILogger logger, SyncStage stage, string message, params object[] args)
        //       => logger.LogWarning(new EventId((int)stage, Enum.GetName(typeof(SyncStage), stage)), message, args);
        //    /// <summary>
        //    /// Create a Log with an EventId generated from a SyncStage enumeration
        //    /// </summary>
        //    public static void LogWarning(this ILogger logger, SyncStage stage, Exception exception, string message, params object[] args)
        //        => logger.LogWarning(new EventId((int)stage, Enum.GetName(typeof(SyncStage), stage)), exception, message, args);


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

    public static class SyncEventsId
    {
        // private static EventId CreateEventId(SyncStage stage) => new EventId((int)stage, Enum.GetName(typeof(SyncStage), stage));
        private static EventId CreateEventId(int id, string eventName) => new EventId(id, eventName);


        public static EventId Exception => CreateEventId(0, nameof(Exception));
        public static EventId ReportProgress => CreateEventId(5, nameof(ReportProgress));
        public static EventId Interceptor => CreateEventId(action + 10, nameof(Interceptor));



        public static EventId OpenConnection => CreateEventId(2, nameof(OpenConnection));
        public static EventId CloseConnection => CreateEventId(3, nameof(CloseConnection));


        public static EventId BeginSession => CreateEventId(1, nameof(BeginSession));
        public static EventId EndSession => CreateEventId(4, nameof(EndSession));

        private const int action = 100;
        public static EventId Provision => CreateEventId(action + 1, nameof(Provision));
        public static EventId Deprovision => CreateEventId(action + 2, nameof(Deprovision));
        public static EventId GetSchema => CreateEventId(action + 3, nameof(GetSchema));
        public static EventId MetadataCleaning => CreateEventId(action + 4, nameof(MetadataCleaning));
        public static EventId IsOutdated => CreateEventId(action + 5, nameof(IsOutdated));
        public static EventId GetClientScope => CreateEventId(action + 6, nameof(GetClientScope));
        public static EventId GetServerScope => CreateEventId(action + 7, nameof(GetServerScope));
        public static EventId GetServerScopeHistory => CreateEventId(action + 8, nameof(GetServerScopeHistory));
        public static EventId GetChanges => CreateEventId(action + 9, nameof(GetChanges));
        public static EventId ApplyChanges => CreateEventId(action + 10, nameof(ApplyChanges));
        public static EventId ApplySnapshot => CreateEventId(action + 11, nameof(ApplySnapshot));
        public static EventId CreateSnapshot => CreateEventId(action + 12, nameof(CreateSnapshot));
        public static EventId GetSnapshot => CreateEventId(action + 13, nameof(GetSnapshot));
        public static EventId Migration => CreateEventId(action + 14, nameof(Migration));
        public static EventId EnsureSchema => CreateEventId(action + 15, nameof(EnsureSchema));
        public static EventId ApplyThenGetChanges => CreateEventId(action + 16, nameof(ApplyThenGetChanges));
        public static EventId GetHello => CreateEventId(action + 17, nameof(GetHello));
        public static EventId GetLocalTimestamp => CreateEventId(action + 18, nameof(GetLocalTimestamp));
        public static EventId CreateDirectory => CreateEventId(action + 19, nameof(CreateDirectory));
        public static EventId AddFilter => CreateEventId(action + 20, nameof(AddFilter));
        public static EventId WriteClientScope => CreateEventId(action + 21, nameof(WriteClientScope));
        public static EventId WriteServerScope => CreateEventId(action + 22, nameof(WriteServerScope));
        public static EventId WriteServerScopeHistory => CreateEventId(action + 23, nameof(WriteServerScopeHistory));
        public static EventId DropDirectory => CreateEventId(action + 24, nameof(DropDirectory));
        public static EventId CreateBatch => CreateEventId(action + 25, nameof(CreateBatch));
        public static EventId CreateSnapshotSummary => CreateEventId(action + 26, nameof(CreateSnapshotSummary));
        public static EventId LoadSnapshotSummary => CreateEventId(action + 27, nameof(LoadSnapshotSummary));
        public static EventId DirectoryNotExists => CreateEventId(action + 28, nameof(DirectoryNotExists));
        public static EventId CreateTable => CreateEventId(action + 29, nameof(CreateTable));
        public static EventId DropTable => CreateEventId(action + 30, nameof(DropTable));
        public static EventId ResetTable => CreateEventId(action + 31, nameof(ResetTable));
        public static EventId ResolveConflicts => CreateEventId(action + 32, nameof(ResolveConflicts));


    }
}
