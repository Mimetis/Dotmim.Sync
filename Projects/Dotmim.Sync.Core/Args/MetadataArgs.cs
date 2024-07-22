using Dotmim.Sync.Enumerations;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    /// <summary>
    /// Raised as an argument when a provider is about to cleanup metadata.
    /// </summary>
    public class MetadataCleaningArgs : ProgressArgs
    {
        /// <inheritdoc cref="MetadataCleaningArgs"/>
        public MetadataCleaningArgs(SyncContext context, IEnumerable<ScopeInfo> scopeInfos, long timeStampStart, DbConnection connection, DbTransaction transaction)
        : base(context, connection, transaction)
        {
            this.ScopeInfos = scopeInfos;
            this.TimeStampStart = timeStampStart;
        }

        /// <summary>
        /// Gets the scope infos to be cleaned.
        /// </summary>
        public IEnumerable<ScopeInfo> ScopeInfos { get; }

        /// <summary>
        /// Gets the lower timestamp limit to clean metadata.
        /// </summary>
        public long TimeStampStart { get; }

        /// <inheritdoc cref="ProgressArgs.Message"/>
        public override string Message => $"Cleaning Metadatas. TimestampStart:{this.TimeStampStart}";

        /// <inheritdoc cref="ProgressArgs.ProgressLevel"/>
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;

        /// <inheritdoc cref="ProgressArgs.EventId"/>
        public override int EventId => SyncEventsId.MetadataCleaning.Id;
    }

    /// <summary>
    /// Raised as an argument when a provider has cleaned metadata.
    /// </summary>
    public class MetadataCleanedArgs : ProgressArgs
    {
        /// <inheritdoc cref="MetadataCleanedArgs"/>
        public MetadataCleanedArgs(SyncContext context, DatabaseMetadatasCleaned databaseMetadatasCleaned, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.DatabaseMetadatasCleaned = databaseMetadatasCleaned;
        }

        /// <summary>
        /// Gets or Sets the rows count cleaned for all tables, during a DeleteMetadatasAsync call.
        /// </summary>
        public DatabaseMetadatasCleaned DatabaseMetadatasCleaned { get; set; }

        /// <inheritdoc cref="ProgressArgs.ProgressLevel"/>
        public override SyncProgressLevel ProgressLevel => this.DatabaseMetadatasCleaned != null && this.DatabaseMetadatasCleaned.RowsCleanedCount > 0 ? SyncProgressLevel.Information : SyncProgressLevel.Debug;

        /// <inheritdoc cref="ProgressArgs.Message"/>
        public override string Message =>
            this.DatabaseMetadatasCleaned == null
            ? $"MetadataCleanedArgs progress."
            : $"Tables Cleaned:{this.DatabaseMetadatasCleaned.Tables.Count}. Rows Cleaned:{this.DatabaseMetadatasCleaned.RowsCleanedCount}. TimestampLimit:{this.DatabaseMetadatasCleaned.TimestampLimit}";

        /// <inheritdoc cref="ProgressArgs.EventId"/>
        public override int EventId => SyncEventsId.MetadataCleaned.Id;
    }

    /// <summary>
    /// Interceptors extensions.
    /// </summary>
    public partial class InterceptorsExtensions
    {
        /// <summary>
        /// Intercept the provider action when a provider is cleaning metadata.
        /// </summary>
        public static Guid OnMetadataCleaning(this BaseOrchestrator orchestrator, Action<MetadataCleaningArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider action when a provider is cleaning metadata.
        /// </summary>
        public static Guid OnMetadataCleaning(this BaseOrchestrator orchestrator, Func<MetadataCleaningArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider action when a provider has cleaned metadata.
        /// </summary>
        public static Guid OnMetadataCleaned(this BaseOrchestrator orchestrator, Action<MetadataCleanedArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider action when a provider has cleaned metadata.
        /// </summary>
        public static Guid OnMetadataCleaned(this BaseOrchestrator orchestrator, Func<MetadataCleanedArgs, Task> action)
            => orchestrator.AddInterceptor(action);
    }

    /// <summary>
    /// Sync Events Id.
    /// </summary>
    public partial class SyncEventsId
    {
        /// <summary>
        /// Gets the unique event id.
        /// </summary>
        public static EventId MetadataCleaning => CreateEventId(3000, nameof(MetadataCleaning));

        /// <summary>
        /// Gets the unique event id.
        /// </summary>
        public static EventId MetadataCleaned => CreateEventId(3050, nameof(MetadataCleaned));
    }
}