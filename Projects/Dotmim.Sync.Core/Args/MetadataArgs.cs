using Dotmim.Sync.Enumerations;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    public class MetadataCleaningArgs : ProgressArgs
    {
        public IEnumerable<IScopeInfo> ScopeInfos { get; }
        public long TimeStampStart { get; }

        public MetadataCleaningArgs(SyncContext context, IEnumerable<IScopeInfo> scopeInfos, long timeStampStart, DbConnection connection, DbTransaction transaction)
        : base(context, connection, transaction)

        {
            this.ScopeInfos = scopeInfos;
            this.TimeStampStart = timeStampStart;
        }
        public override string Source => Connection.Database;
        public override string Message => $"Cleaning Metadatas.";
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;

        public override int EventId => SyncEventsId.MetadataCleaning.Id;
    }

    public class MetadataCleanedArgs : ProgressArgs
    {
        public MetadataCleanedArgs(SyncContext context, DatabaseMetadatasCleaned databaseMetadatasCleaned, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.DatabaseMetadatasCleaned = databaseMetadatasCleaned;
        }

        /// <summary>
        /// Gets or Sets the rows count cleaned for all tables, during a DeleteMetadatasAsync call
        /// </summary>
        public DatabaseMetadatasCleaned DatabaseMetadatasCleaned { get; set; }
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Information;

        public override string Source => Connection.Database;
        public override string Message => $"Tables Cleaned:{DatabaseMetadatasCleaned.Tables.Count}. Rows Cleaned:{DatabaseMetadatasCleaned.RowsCleanedCount}.";

        public override int EventId => SyncEventsId.MetadataCleaned.Id;
    }


    public static partial class InterceptorsExtensions
    {
        /// <summary>
        /// Intercept the provider action when a provider is cleaning metadata
        /// </summary>
        public static void OnMetadataCleaning(this BaseOrchestrator orchestrator, Action<MetadataCleaningArgs> action)
            => orchestrator.SetInterceptor(action);
        /// <summary>
        /// Intercept the provider action when a provider is cleaning metadata
        /// </summary>
        public static void OnMetadataCleaning(this BaseOrchestrator orchestrator, Func<MetadataCleaningArgs, Task> action)
            => orchestrator.SetInterceptor(action);

        /// Intercept the provider action when a provider has cleaned metadata
        /// </summary>
        public static void OnMetadataCleaned(this BaseOrchestrator orchestrator, Action<MetadataCleanedArgs> action)
            => orchestrator.SetInterceptor(action);
        /// Intercept the provider action when a provider has cleaned metadata
        /// </summary>
        public static void OnMetadataCleaned(this BaseOrchestrator orchestrator, Func<MetadataCleanedArgs, Task> action)
            => orchestrator.SetInterceptor(action);

    }

    public static partial class SyncEventsId
    {
        public static EventId MetadataCleaning => CreateEventId(3000, nameof(MetadataCleaning));
        public static EventId MetadataCleaned => CreateEventId(3050, nameof(MetadataCleaned));
    }

}