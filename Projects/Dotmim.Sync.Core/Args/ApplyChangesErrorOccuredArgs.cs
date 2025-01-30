﻿using Dotmim.Sync.Enumerations;
using System;
using System.Data.Common;
using System.Threading.Tasks;

namespace Dotmim.Sync
{

    /// <summary>
    /// Argument used during OnApplyChangesErrorOccured() interceptor. You need to provide a Resolution (<see cref="ErrorResolution" /> enumeration).
    /// </summary>
    public class ApplyChangesErrorOccuredArgs : ProgressArgs
    {
        /// <inheritdoc cref="ApplyChangesErrorOccuredArgs" />
        public ApplyChangesErrorOccuredArgs(SyncContext context, SyncRow errorRow,
            SyncTable schemaChangesTable, SyncRowState applyType, Exception exception, ErrorResolution errorPolicy,
            DbConnection connection, DbTransaction transaction)
            : base(context, connection, transaction)
        {
            this.ErrorRow = errorRow;
            this.SchemaTable = schemaChangesTable;
            this.ApplyType = applyType;
            this.Exception = exception;
            this.Resolution = errorPolicy;
        }

        /// <summary>
        /// Gets the current row that has failed to applied.
        /// </summary>
        public SyncRow ErrorRow { get; }

        /// <summary>
        /// Gets the error row schema table.
        /// </summary>
        public SyncTable SchemaTable { get; }

        /// <summary>
        /// Gets the type of apply command (Upsert / Delete).
        /// </summary>
        public SyncRowState ApplyType { get; }

        /// <summary>
        /// Gets the current exception.
        /// </summary>
        public Exception Exception { get; }

        /// <summary>
        /// Gets or Sets the resolution fo the current error.
        /// </summary>
        public ErrorResolution Resolution { get; set; } = ErrorResolution.Throw;

        /// <summary>
        /// Gets the overall progress.
        /// </summary>
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;

        /// <summary>
        /// Gets the error message.
        /// </summary>
        public override string Message => $"Error: {this.Exception.Message}. Row:{this.ErrorRow}. ApplyType:{this.ApplyType}";

        /// <summary>
        /// Gets the unique event id.
        /// </summary>
        public override int EventId => 301;
    }

    /// <summary>
    /// Interceptors extensions.
    /// </summary>
    public partial class InterceptorsExtensions
    {

        /// <summary>
        /// Intercept the provider when an apply change is failing.
        /// </summary>
        public static Guid OnApplyChangesErrorOccured(this BaseOrchestrator orchestrator, Action<ApplyChangesErrorOccuredArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when an apply change is failing.
        /// </summary>
        public static Guid OnApplyChangesErrorOccured(this BaseOrchestrator orchestrator, Func<ApplyChangesErrorOccuredArgs, Task> action)
            => orchestrator.AddInterceptor(action);
    }
}