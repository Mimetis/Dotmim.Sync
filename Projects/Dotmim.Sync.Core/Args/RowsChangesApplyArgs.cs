﻿using Dotmim.Sync.Batch;
using Dotmim.Sync.Enumerations;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    /// <summary>
    /// Event args before a batch changes is going to be applied on a datasource
    /// </summary>
    public class RowsChangesApplyingArgs : ProgressArgs
    {
        public bool Cancel { get; set; } = false;
        public DbCommand Command { get; set; }

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
        /// Gets the RowState of the applied rows
        /// </summary>
        public SyncRowState State { get; }
        public BatchInfo BatchInfo { get; }
        public List<SyncRow> SyncRows { get; }

        public SyncTable SchemaTable { get; }

        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;
        public override string Message => $"Applying [{this.SchemaTable.GetFullName()}] batch rows. State:{this.State}. Count:{this.SyncRows.Count()}";

        public override int EventId => SyncEventsId.RowsChangesApplying.Id;
    }


    /// <summary>
    /// Event args after a batch changes has been applied on a datasource
    /// </summary>
    public class RowsChangesAppliedArgs : ProgressArgs
    {
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
        /// Gets the RowState of the applied rows
        /// </summary>
        public SyncRowState State { get; }

        /// <summary>
        /// Get result of sql statement (if AppliedCount == 1, it means row has been applied)
        /// </summary>
        public int AppliedCount { get; }

        /// <summary>
        /// If not null, an execption has been raised
        /// </summary>
        public Exception Exception { get; }

        /// <summary>
        /// Batchinfo from where SyncRows are coming
        /// </summary>
        public BatchInfo BatchInfo { get; }

        /// <summary>
        /// SyncRows applied (or not if Exception is not null)
        /// </summary>
        public List<SyncRow> SyncRows { get; }

        /// <summary>
        /// SyncRow schema
        /// </summary>
        public SyncTable SchemaTable { get; }

        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;
        public override string Message => $"Applying [{this.SchemaTable.GetFullName()}] batch rows. State:{this.State}. Count:{this.SyncRows.Count()}";

        public override int EventId => SyncEventsId.RowsChangesApplying.Id;
    }


    public class RowsChangesFallbackFromBatchToSingleRowApplyingArgs : ProgressArgs
    {
        public bool Cancel { get; set; } = false;
        public DbCommand Command { get; set; }

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
        /// Gets the RowState of the applied rows
        /// </summary>
        public SyncRowState State { get; }
        public Exception Exception { get; }
        public BatchInfo BatchInfo { get; }
        public List<SyncRow> SyncRows { get; }

        public SyncTable SchemaTable { get; }

        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;
        public override string Message => $"Fallback from Batch to Row per Row on table [{this.SchemaTable.GetFullName()}]. Error:{Exception?.Message}. State:{this.State}. Count:{this.SyncRows.Count()}";

        public override int EventId => SyncEventsId.RowsChangesApplying.Id;
    }


    public static partial class InterceptorsExtensions
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
        /// Occurs when a batch was not applied successfully and DMS tries to fallback to one row per row applying
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
        /// Occurs just after a batch or rows have been applied to the local (client or server) database
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

    public static partial class SyncEventsId
    {
        public static EventId RowsChangesApplying => CreateEventId(13100, nameof(RowsChangesApplying));

    }
}
