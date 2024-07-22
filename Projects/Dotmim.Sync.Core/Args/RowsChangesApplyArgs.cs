using Dotmim.Sync.Batch;
using Dotmim.Sync.Enumerations;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    /// <summary>
    /// Event args before a batch changes is going to be applied on a datasource.
    /// </summary>
    public class RowsChangesApplyingArgs : ProgressArgs
    {

        /// <inheritdoc cref="RowsChangesApplyingArgs"/>
        public RowsChangesApplyingArgs(SyncContext context, BatchInfo batchInfo, List<SyncRow> syncRows, SyncTable schemaTable, SyncRowState state, DbCommand command, DbConnection connection, DbTransaction transaction)
            : base(context, connection, transaction)
        {
            this.State = state;
            this.Command = command;
            this.BatchInfo = batchInfo;
            this.SyncRows = syncRows;
            this.SchemaTable = schemaTable;
        }

        /// <summary>
        /// Gets or sets a value indicating whether the changes to applied should be canceled.
        /// </summary>
        public bool Cancel { get; set; }

        /// <summary>
        /// Gets or sets the command to be executed to apply the changes to the datasource.
        /// </summary>
        public DbCommand Command { get; set; }

        /// <summary>
        /// Gets the RowState of the applied rows.
        /// </summary>
        public SyncRowState State { get; }

        /// <summary>
        /// Gets batchinfo serialized on disk, containing the rows to be applied.
        /// </summary>
        public BatchInfo BatchInfo { get; }

        /// <summary>
        /// Gets the rows to be applied.
        /// </summary>
        public List<SyncRow> SyncRows { get; }

        /// <summary>
        /// Gets the schema of the rows to be applied.
        /// </summary>
        public SyncTable SchemaTable { get; }

        /// <inheritdoc cref="ProgressArgs.ProgressLevel"/>
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;

        /// <inheritdoc cref="ProgressArgs.Message"/>
        public override string Message => $"Applying [{this.SchemaTable.GetFullName()}] batch rows. State:{this.State}. Count:{this.SyncRows.Count}";

        /// <inheritdoc cref="ProgressArgs.EventId"/>
        public override int EventId => 13100;
    }

    /// <summary>
    /// Event args after a batch changes has been applied on a datasource.
    /// </summary>
    public class RowsChangesAppliedArgs : ProgressArgs
    {
        /// <inheritdoc cref="RowsChangesAppliedArgs"/>
        public RowsChangesAppliedArgs(SyncContext context, BatchInfo batchInfo, List<SyncRow> syncRows, SyncTable schemaTable, SyncRowState state,
            int appliedCount, Exception exception, DbConnection connection, DbTransaction transaction)
            : base(context, connection, transaction)
        {
            this.State = state;
            this.AppliedCount = appliedCount;
            this.Exception = exception;
            this.BatchInfo = batchInfo;
            this.SyncRows = syncRows;
            this.SchemaTable = schemaTable;
        }

        /// <summary>
        /// Gets the RowState of the applied rows.
        /// </summary>
        public SyncRowState State { get; }

        /// <summary>
        /// Gets get result of sql statement (if AppliedCount == 1, it means row has been applied).
        /// </summary>
        public int AppliedCount { get; }

        /// <summary>
        /// Gets if not null, an execption has been raised.
        /// </summary>
        public Exception Exception { get; }

        /// <summary>
        /// Gets batchinfo from where SyncRows are coming.
        /// </summary>
        public BatchInfo BatchInfo { get; }

        /// <summary>
        /// Gets syncRows applied (or not if Exception is not null).
        /// </summary>
        public List<SyncRow> SyncRows { get; }

        /// <summary>
        /// Gets syncRow schema.
        /// </summary>
        public SyncTable SchemaTable { get; }

        /// <inheritdoc cref="ProgressArgs.ProgressLevel"/>
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;

        /// <inheritdoc cref="ProgressArgs.Message"/>
        public override string Message => $"Applying [{this.SchemaTable.GetFullName()}] batch rows. State:{this.State}. Count:{this.SyncRows.Count}";

        /// <inheritdoc cref="ProgressArgs.EventId"/>
        public override int EventId => 13150;
    }

    /// <summary>
    /// Event args when a batch was not applied successfully and DMS tries to fallback to one row per row applying.
    /// </summary>
    public class RowsChangesFallbackFromBatchToSingleRowApplyingArgs : ProgressArgs
    {
        /// <inheritdoc cref="RowsChangesFallbackFromBatchToSingleRowApplyingArgs"/>
        public RowsChangesFallbackFromBatchToSingleRowApplyingArgs(SyncContext context, Exception exception, BatchInfo batchInfo, List<SyncRow> syncRows, SyncTable schemaTable, SyncRowState state, DbCommand command, DbConnection connection, DbTransaction transaction)
            : base(context, connection, transaction)
        {
            this.State = state;
            this.Command = command;
            this.Exception = exception;
            this.BatchInfo = batchInfo;
            this.SyncRows = syncRows;
            this.SchemaTable = schemaTable;
        }

        /// <summary>
        /// Gets or sets a value indicating whether the command should be canceled.
        /// </summary>
        public bool Cancel { get; set; }

        /// <summary>
        /// Gets or sets the command to be executed to apply the changes to the datasource, line by line.
        /// </summary>
        public DbCommand Command { get; set; }

        /// <summary>
        /// Gets the RowState of the applied rows.
        /// </summary>
        public SyncRowState State { get; }

        /// <summary>
        /// Gets the exception that caused the fallback.
        /// </summary>
        public Exception Exception { get; }

        /// <summary>
        /// Gets the batchinfo from where SyncRows are coming.
        /// </summary>
        public BatchInfo BatchInfo { get; }

        /// <summary>
        /// Gets the rows to be applied.
        /// </summary>
        public List<SyncRow> SyncRows { get; }

        /// <summary>
        /// Gets the schema of the rows to be applied.
        /// </summary>
        public SyncTable SchemaTable { get; }

        // <inheritdoc cref="ProgressArgs.ProgressLevel"/>
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;

        // <inheritdoc cref="ProgressArgs.Message"/>
        public override string Message => $"Fallback from Batch to Row per Row on table [{this.SchemaTable.GetFullName()}]. Error:{this.Exception?.Message}. State:{this.State}. Count:{this.SyncRows.Count}";

        // <inheritdoc cref="ProgressArgs.EventId"/>
        public override int EventId => 13160;
    }

    /// <summary>
    /// Interceptor called before and after applying a batch of rows to the local (client or server) database.
    /// </summary>
    public partial class InterceptorsExtensions
    {
        /// <summary>
        /// Occurs just before applying a batch of rows to the local (client or server) database.
        /// <para>
        /// The number of rows to be applied here is depending on:
        /// <list type="bullet">
        /// The batch size you have set in your SyncOptions instance : <c>SyncOptions.BatchSize</c> (Default is 2 Mo)
        /// </list>
        /// <list type="bullet">
        /// The max number of rows to applied in one single instruction : <c>Provider.BulkBatchMaxLinesCount</c> (Default is 10 000 rows per instruction)
        /// </list>
        /// </para>
        /// <example>
        /// <code>
        /// localOrchestrator.OnRowsChangesApplying(async args =>
        /// {
        ///     Console.WriteLine($"- In memory rows that are going to be Applied");
        ///     foreach (var row in args.SyncRows)
        ///         Console.WriteLine(row);
        ///
        ///     Console.WriteLine();
        /// });
        /// </code>
        /// </example>
        /// </summary>
        public static Guid OnRowsChangesApplying(this BaseOrchestrator orchestrator, Action<RowsChangesApplyingArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <inheritdoc cref="OnRowsChangesApplying(BaseOrchestrator, Action{RowsChangesApplyingArgs})"/>
        public static Guid OnRowsChangesApplying(this BaseOrchestrator orchestrator, Func<RowsChangesApplyingArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Occurs when a batch was not applied successfully and DMS tries to fallback to one row per row applying.
        /// <example>
        /// <code>
        /// localOrchestrator.OnRowsChangesFallbackFromBatchToSingleRowApplying(async args =>
        /// {
        ///     Console.WriteLine($"- In memory rows that are going to be Applied one by one because of an error occured during batch applying");
        ///     Console.WriteLine($"- Error raised causing the fallback: {args.Exception.Message}");
        ///     Console.WriteLine($"- Rows count to apply one by one: {args.SyncRows.Count}");
        /// });
        /// </code>
        /// </example>
        /// </summary>
        public static Guid OnRowsChangesFallbackFromBatchToSingleRowApplying(this BaseOrchestrator orchestrator, Action<RowsChangesFallbackFromBatchToSingleRowApplyingArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <inheritdoc cref="OnRowsChangesFallbackFromBatchToSingleRowApplying(BaseOrchestrator, Action{RowsChangesFallbackFromBatchToSingleRowApplyingArgs})"/>
        public static Guid OnRowsChangesFallbackFromBatchToSingleRowApplying(this BaseOrchestrator orchestrator, Func<RowsChangesFallbackFromBatchToSingleRowApplyingArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Occurs just after a batch or rows have been applied to the local (client or server) database.
        /// <para>
        /// The number of rows applied here is depending on:
        /// <list type="bullet">
        /// The batch size you have set in your SyncOptions instance : <c>SyncOptions.BatchSize</c> (Default is 2 Mo)
        /// </list>
        /// <list type="bullet">
        /// The max number of rows to applied in one single instruction : <c>Provider.BulkBatchMaxLinesCount</c> (Default is 10 000 rows per instruction)
        /// </list>
        /// </para>
        /// <example>
        /// <code>
        /// localOrchestrator.OnRowsChangesApplied(async args =>
        /// {
        ///     Console.WriteLine($"- In memory rows that are applied, but still in the on going transaction");
        ///     foreach (var row in args.SyncRows)
        ///         Console.WriteLine(row);
        ///
        ///     Console.WriteLine();
        /// });
        /// </code>
        /// </example>
        /// </summary>
        public static Guid OnRowsChangesApplied(this BaseOrchestrator orchestrator, Action<RowsChangesAppliedArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <inheritdoc cref="OnRowsChangesApplied(BaseOrchestrator, Action{RowsChangesAppliedArgs})"/>
        public static Guid OnRowsChangesApplied(this BaseOrchestrator orchestrator, Func<RowsChangesAppliedArgs, Task> action)
            => orchestrator.AddInterceptor(action);
    }
}