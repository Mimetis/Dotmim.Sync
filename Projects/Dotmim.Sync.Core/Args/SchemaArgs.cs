using Dotmim.Sync.Enumerations;
using System;
using System.Data.Common;
using System.Threading.Tasks;

namespace Dotmim.Sync
{

    /// <summary>
    /// Event args generated before a schema is loaded.
    /// </summary>
    public class SchemaLoadingArgs : ProgressArgs
    {
        /// <inheritdoc cref="SchemaLoadingArgs" />
        public SchemaLoadingArgs(SyncContext context, SyncSetup setup, DbConnection connection, DbTransaction transaction = null)
            : base(context, connection, transaction) => this.Setup = setup;

        /// <summary>
        /// Gets the Setup containing all the tables to load in a SyncSchema instance.
        /// </summary>
        public SyncSetup Setup { get; }

        /// <inheritdoc cref="ProgressArgs.Message" />/>
        public override string Message => $"Loading Schema For {this.Setup.Tables.Count} Tables.";

        /// <inheritdoc cref="ProgressArgs.ProgressLevel" />/>
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;

        /// <inheritdoc cref="ProgressArgs.EventId" />/>
        public override int EventId => 6000;
    }

    /// <summary>
    /// Event args generated after a schema is loaded.
    /// </summary>
    public class SchemaLoadedArgs : ProgressArgs
    {
        /// <inheritdoc cref="SchemaLoadedArgs" />
        public SchemaLoadedArgs(SyncContext context, SyncSet schema, DbConnection connection, DbTransaction transaction = null)
            : base(context, connection, transaction) => this.Schema = schema;

        /// <summary>
        /// Gets the schema loaded.
        /// </summary>
        public SyncSet Schema { get; }

        /// <inheritdoc cref="ProgressArgs.ProgressLevel" />/>
        public override SyncProgressLevel ProgressLevel => this.Schema != null && this.Schema.HasTables ? SyncProgressLevel.Information : SyncProgressLevel.Debug;

        /// <inheritdoc cref="ProgressArgs.Message" />/>
        public override string Message => $"Schema Loaded For {this.Schema.Tables.Count} Tables.";

        /// <inheritdoc cref="ProgressArgs.EventId" />/>
        public override int EventId => 6050;
    }

    /// <summary>
    /// Partial Interceptors extensions.
    /// </summary>
    public partial class InterceptorsExtensions
    {

        /// <summary>
        /// Intercept the provider when schema is loaded.
        /// </summary>
        public static Guid OnSchemaLoaded(this BaseOrchestrator orchestrator, Action<SchemaLoadedArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when schema is loaded.
        /// </summary>
        public static Guid OnSchemaLoaded(this BaseOrchestrator orchestrator, Func<SchemaLoadedArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when schema is loading.
        /// </summary>
        public static Guid OnSchemaLoading(this BaseOrchestrator orchestrator, Action<SchemaLoadingArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when schema is loading.
        /// </summary>
        public static Guid OnSchemaLoading(this BaseOrchestrator orchestrator, Func<SchemaLoadingArgs, Task> action)
            => orchestrator.AddInterceptor(action);
    }
}