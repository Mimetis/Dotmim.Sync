using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
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
        public override string Source => Connection.Database;
        public override string Message => $"Loading Schema For {this.Setup.Tables.Count} Tables.";
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;
        public override int EventId => SyncEventsId.SchemaLoading.Id;
    }

    public class SchemaLoadedArgs : ProgressArgs
    {
        public SchemaLoadedArgs(SyncContext context, SyncSet schema, DbConnection connection, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.Schema = schema;
        }
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Information;

        /// <summary>
        /// Gets the schema loaded.
        /// </summary>
        public SyncSet Schema { get; }
        public override string Source => Connection.Database;
        public override string Message => $"[{Connection.Database}] Schema Loaded For {this.Schema.Tables.Count} Tables.";

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
        /// Intercept the provider when schema is loaded
        /// </summary>
        public static void OnSchemaLoaded(this BaseOrchestrator orchestrator, Func<SchemaLoadedArgs, Task> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider when schema is loading
        /// </summary>
        public static void OnSchemaLoading(this BaseOrchestrator orchestrator, Action<SchemaLoadingArgs> action)
            => orchestrator.SetInterceptor(action);
        /// <summary>
        /// Intercept the provider when schema is loading
        /// </summary>
        public static void OnSchemaLoading(this BaseOrchestrator orchestrator, Func<SchemaLoadingArgs, Task> action)
            => orchestrator.SetInterceptor(action);
    }
    public static partial class SyncEventsId
    {
        public static EventId SchemaLoading => CreateEventId(6000, nameof(SchemaLoading));
        public static EventId SchemaLoaded => CreateEventId(6050, nameof(SchemaLoaded));

    }
}
