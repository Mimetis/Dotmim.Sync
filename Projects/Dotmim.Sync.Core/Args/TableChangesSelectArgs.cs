using Dotmim.Sync.Batch;
using Dotmim.Sync.Enumerations;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;

namespace Dotmim.Sync
{

    /// <summary>
    /// Contains batches information about changes from underline data sources.
    /// </summary>
    public class TableChangesSelectedArgs : ProgressArgs
    {
        /// <inheritdoc cref="TableChangesSelectedArgs" />
        public TableChangesSelectedArgs(SyncContext context, BatchInfo batchInfo, IEnumerable<BatchPartInfo> batchPartInfos, SyncTable schemaTable, TableChangesSelected changesSelected, DbConnection connection, DbTransaction transaction)
            : base(context, connection, transaction)
        {
            this.BatchInfo = batchInfo;
            this.BatchPartInfos = batchPartInfos;
            this.SchemaTable = schemaTable;
            this.TableChangesSelected = changesSelected;
        }

        /// <summary>
        /// Gets the BatchInfo related.
        /// </summary>
        public BatchInfo BatchInfo { get; }

        /// <summary>
        /// Gets the SyncTable instances containing all changes selected.
        /// If you get this instance from a call from GetEstimatedChangesCount, this property is always null.
        /// </summary>
        public IEnumerable<BatchPartInfo> BatchPartInfos { get; }

        /// <summary>
        /// Gets the table schema.
        /// </summary>
        public SyncTable SchemaTable { get; }

        /// <summary>
        /// Gets the incremental summary of changes selected.
        /// </summary>
        public TableChangesSelected TableChangesSelected { get; }

        /// <inheritdoc cref="ProgressArgs.ProgressLevel"/>
        public override SyncProgressLevel ProgressLevel => this.TableChangesSelected != null && this.TableChangesSelected.TotalChanges > 0 ? SyncProgressLevel.Information : SyncProgressLevel.Debug;

        /// <inheritdoc cref="ProgressArgs.Message"/>
        public override string Message =>
            this.TableChangesSelected == null
            ? "TableChangesSelectedArgs progress."
            : $"[{this.TableChangesSelected.TableName}] [Total] Upserts:{this.TableChangesSelected.Upserts}. Deletes:{this.TableChangesSelected.Deletes}. Total:{this.TableChangesSelected.TotalChanges}.";

        /// <inheritdoc cref="ProgressArgs.EventId"/>
        public override int EventId => 13200;
    }

    /// <summary>
    /// Raise before selecting changes will occur.
    /// </summary>
    public class TableChangesSelectingArgs : ProgressArgs
    {

        /// <inheritdoc cref="TableChangesSelectingArgs" />
        public TableChangesSelectingArgs(SyncContext context, SyncTable schemaTable, DbCommand command, DbConnection connection, DbTransaction transaction)
            : base(context, connection, transaction)
        {
            this.SchemaTable = schemaTable;
            this.Command = command;
        }

        /// <summary>
        /// Gets or sets a value indicating whether the get changes command should be canceled.
        /// </summary>
        public bool Cancel { get; set; }

        /// <summary>
        /// Gets or sets the command to be executed.
        /// </summary>
        public DbCommand Command { get; set; }

        /// <summary>
        /// Gets the table from where the changes are going to be selected.
        /// </summary>
        public SyncTable SchemaTable { get; }

        /// <inheritdoc cref="ProgressArgs.Message"/>
        public override string Message => $"[{this.SchemaTable.GetFullName()}] Getting Changes.";

        /// <inheritdoc cref="ProgressArgs.ProgressLevel"/>
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;

        /// <inheritdoc cref="ProgressArgs.EventId"/>
        public override int EventId => 13250;
    }

    /// <summary>
    /// Intercept the changes selected from the database.
    /// </summary>
    public partial class InterceptorsExtensions
    {
        /// <summary>
        /// Occurs when changes are going to be queried from the underline database for a particular table.
        /// <example>
        /// <code>
        /// var localOrchestrator = new LocalOrchestrator(clientProvider);
        /// localOrchestrator.OnTableChangesSelecting(args =>
        /// {
        ///     Console.WriteLine($"Getting changes from local database " +
        ///                       $"for table:{args.SchemaTable.GetFullName()}");
        ///
        ///     Console.WriteLine($"{args.Command.CommandText}");
        /// });
        /// </code>
        /// </example>
        /// </summary>
        public static Guid OnTableChangesSelecting(this BaseOrchestrator orchestrator, Action<TableChangesSelectingArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <inheritdoc cref="OnTableChangesSelecting(BaseOrchestrator, Action{TableChangesSelectingArgs})"/>
        public static Guid OnTableChangesSelecting(this BaseOrchestrator orchestrator, Func<TableChangesSelectingArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Occurs once a table is fully read during the get changes step. rows are already serialized on disk.
        /// <example>
        /// <code>
        /// localOrchestrator.OnTableChangesSelected(args =>
        /// {
        ///   Console.WriteLine($"Table: {args.SchemaTable.GetFullName()} read. Rows count:{args.BatchInfo.RowsCount}.");
        ///   Console.WriteLine($"Directory: {args.BatchInfo.DirectoryName}. Number of files: {args.BatchPartInfos?.Count()} ");
        ///   Console.WriteLine($"Changes: {args.TableChangesSelected.TotalChanges} ({args.TableChangesSelected.Upserts}/{args.TableChangesSelected.Deletes})");
        /// });
        /// </code>
        /// </example>
        /// </summary>
        public static Guid OnTableChangesSelected(this BaseOrchestrator orchestrator, Action<TableChangesSelectedArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <inheritdoc cref="OnTableChangesSelected(BaseOrchestrator, Action{TableChangesSelectedArgs})"/>
        public static Guid OnTableChangesSelected(this BaseOrchestrator orchestrator, Func<TableChangesSelectedArgs, Task> action)
            => orchestrator.AddInterceptor(action);
    }
}