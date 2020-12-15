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
        public static EventId DropDirectory => CreateEventId(action + 24, nameof(DropDirectory));
        public static EventId CreateBatch => CreateEventId(action + 25, nameof(CreateBatch));
        public static EventId CreateSnapshotSummary => CreateEventId(action + 26, nameof(CreateSnapshotSummary));
        public static EventId LoadSnapshotSummary => CreateEventId(action + 27, nameof(LoadSnapshotSummary));
        public static EventId DirectoryNotExists => CreateEventId(action + 28, nameof(DirectoryNotExists));

        public static EventId CreateTable => CreateEventId(action + 29, nameof(CreateTable));
        public static EventId CreateSchemaName => CreateEventId(action + 29, nameof(CreateSchemaName));
        public static EventId DropTable => CreateEventId(action + 30, nameof(DropTable));
        public static EventId ResetTable => CreateEventId(action + 31, nameof(ResetTable));

        public static EventId CreateTrackingTable => CreateEventId(action + 29, nameof(CreateTrackingTable));
        public static EventId DropTrackingTable => CreateEventId(action + 30, nameof(DropTrackingTable));
        public static EventId RenameTrackingTable => CreateEventId(action + 31, nameof(RenameTrackingTable));

        public static EventId ResolveConflicts => CreateEventId(action + 32, nameof(ResolveConflicts));

        public static EventId CreateTrigger => CreateEventId(action + 33, nameof(CreateTrigger));
        public static EventId DropTrigger => CreateEventId(action + 35, nameof(DropTrigger));

        public static EventId CreateStoredProcedure => CreateEventId(action + 36, nameof(CreateStoredProcedure));
        public static EventId DropStoredProcedure => CreateEventId(action + 38, nameof(DropStoredProcedure));

        public static EventId EnableConstraints => CreateEventId(action + 39, nameof(EnableConstraints));
        public static EventId DisableConstraints => CreateEventId(action + 40, nameof(DisableConstraints));

        public static EventId GetScopeInfo => CreateEventId(action + 21, nameof(GetScopeInfo));
        public static EventId UpsertScopeInfo => CreateEventId(action + 21, nameof(UpsertScopeInfo));
        public static EventId CreateScopeTable => CreateEventId(action + 21, nameof(CreateScopeTable));
        public static EventId DropScopeTable => CreateEventId(action + 21, nameof(DropScopeTable));
    }
}
