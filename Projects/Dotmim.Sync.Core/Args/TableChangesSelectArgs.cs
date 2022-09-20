using Dotmim.Sync.Batch;
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
    /// Contains batches information about changes from underline data sources
    /// </summary>
    public class TableChangesSelectedArgs : ProgressArgs
    {
        public TableChangesSelectedArgs(SyncContext context, BatchInfo batchInfo, IEnumerable<BatchPartInfo> batchPartInfos, SyncTable schemaTable, TableChangesSelected changesSelected, DbConnection connection, DbTransaction transaction)
            : base(context, connection, transaction)
        {
            this.BatchInfo = batchInfo;
            this.BatchPartInfos = batchPartInfos;
            this.SchemaTable = schemaTable;
            this.TableChangesSelected = changesSelected;
        }

        /// <summary>
        /// Gets the BatchInfo related
        /// </summary>
        public BatchInfo BatchInfo { get; }

        /// <summary>
        /// Gets the SyncTable instances containing all changes selected.
        /// If you get this instance from a call from GetEstimatedChangesCount, this property is always null
        /// </summary>
        public IEnumerable<BatchPartInfo> BatchPartInfos { get; }

        /// <summary>
        /// Gets the table schema
        /// </summary>
        public SyncTable SchemaTable { get; }

        /// <summary>
        /// Gets the incremental summary of changes selected
        /// </summary>
        public TableChangesSelected TableChangesSelected { get; }

        public override SyncProgressLevel ProgressLevel => this.TableChangesSelected.TotalChanges > 0 ? SyncProgressLevel.Information : SyncProgressLevel.Debug;

        public override string Source => Connection.Database;
        public override string Message => $"[{this.TableChangesSelected.TableName}] [Total] Upserts:{this.TableChangesSelected.Upserts}. Deletes:{this.TableChangesSelected.Deletes}. Total:{this.TableChangesSelected.TotalChanges}.";
        public override int EventId => SyncEventsId.TableChangesSelected.Id;
    }

    /// <summary>
    /// Raise before selecting changes will occur
    /// </summary>
    public class TableChangesSelectingArgs : ProgressArgs
    {
        public bool Cancel { get; set; } = false;
        public DbCommand Command { get; set; }
        public TableChangesSelectingArgs(SyncContext context, SyncTable schemaTable, DbCommand command, DbConnection connection, DbTransaction transaction)
            : base(context, connection, transaction)
        {
            this.SchemaTable = schemaTable;
            this.Command = command;
        }

        /// <summary>
        /// Gets the table from where the changes are going to be selected.
        /// </summary>
        public SyncTable SchemaTable { get; }
        public override string Source => Connection.Database;
        public override string Message => $"[{this.SchemaTable.GetFullName()}] Getting Changes.";
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;
        public override int EventId => SyncEventsId.TableChangesSelecting.Id;
    }



    public static partial class InterceptorsExtensions
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
        /// Occurs once a table is fully read during the get changes step. rows are already serialized on disk
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

    public static partial class SyncEventsId
    {
        public static EventId TableChangesSelecting => CreateEventId(13000, nameof(TableChangesSelecting));
        public static EventId TableChangesSelected => CreateEventId(13050, nameof(TableChangesSelected));
    }


}
