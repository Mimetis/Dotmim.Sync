using Dotmim.Sync.Builders;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync
{

    public class SchemaLoadingArgs : ProgressArgs
    {
        public SchemaLoadingArgs(SyncContext context, SyncSetup setup, DbConnection connection, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.Setup = setup;
        }

        /// <summary>
        /// Gets the Setup to be load.
        /// </summary>
        public SyncSetup Setup { get; }
        public override string Message => $"synced tables count: {this.Setup.Tables.Count}";

        public override int EventId => SyncEventsId.SchemaLoading.Id;
    }

    public class SchemaLoadedArgs : ProgressArgs
    {
        public SchemaLoadedArgs(SyncContext context, SyncSet schema, DbConnection connection, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.Schema = schema;
        }

        /// <summary>
        /// Gets the schema loaded.
        /// </summary>
        public SyncSet Schema { get; }
        public override string Message => $"synced tables count: {this.Schema.Tables.Count}";

        public override int EventId => SyncEventsId.SchemaLoaded.Id;
    }

    /// <summary>
    /// Partial interceptors extensions 
    /// </summary>
    public static partial class InterceptorsExtensions
    {

        /// <summary>
        /// Intercept the provider when schema is loaded
        /// </summary>
        public static void OnSchemaLoaded(this BaseOrchestrator orchestrator, Action<SchemaLoadedArgs> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider when schema is loading
        /// </summary>
        public static void OnSchemaLoading(this BaseOrchestrator orchestrator, Action<SchemaLoadingArgs> action)
            => orchestrator.SetInterceptor(action);
    }
    public static partial class SyncEventsId
    {
        public static EventId SchemaLoading => CreateEventId(6000, nameof(SchemaLoading));
        public static EventId SchemaLoaded => CreateEventId(6100, nameof(SchemaLoaded));

    }
}
