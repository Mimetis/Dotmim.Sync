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
    public class ColumnCreatedArgs : ProgressArgs
    {
        public string ColumnName { get; }
        public SyncTable Table { get; }
        public ParserName TableName { get; }

        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Trace;

        public ColumnCreatedArgs(SyncContext context, string columnName, SyncTable table, ParserName tableName, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.TableName = tableName;
            this.ColumnName = columnName;
            this.Table = table;
        }

        public override string Message => $"[{ColumnName}] Added.";

        public override int EventId => SyncEventsId.ColumnCreated.Id;
    }

    public class ColumnCreatingArgs : ProgressArgs
    {
        public bool Cancel { get; set; } = false;
        public DbCommand Command { get; set; }
        public string ColumnName { get; }
        public SyncTable Table { get; }
        public ParserName TableName { get; }

        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Trace;
        public ColumnCreatingArgs(SyncContext context, string columnName, SyncTable table, ParserName tableName, DbCommand command, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.ColumnName = columnName;
            this.Table = table;
            this.TableName = tableName;
            this.Command = command;
        }
        public override string Message => $"[{ColumnName}] Adding.";
        public override int EventId => SyncEventsId.ColumnCreating.Id;

    }

    public class ColumnDroppedArgs : ProgressArgs
    {
        public string ColumnName { get; }
        public SyncTable Table { get; }
        public ParserName TableName { get; }

        public ColumnDroppedArgs(SyncContext context, string columnName, SyncTable table, ParserName tableName, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.TableName = tableName;
            this.ColumnName = columnName;
            this.Table = table;
        }
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Trace;

        public override string Message => $"[{ColumnName}] Dropped.";
        public override int EventId => SyncEventsId.ColumnDropped.Id;
    }

    public class ColumnDroppingArgs : ProgressArgs
    {
        public bool Cancel { get; set; } = false;
        public DbCommand Command { get; set; }
        public string ColumnName { get; }
        public SyncTable Table { get; }
        public ParserName TableName { get; }

        public ColumnDroppingArgs(SyncContext context, string columnName, SyncTable table, ParserName tableName, DbCommand command, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.Command = command;
            this.TableName = tableName;
            this.ColumnName = columnName;
            this.Table = table;
        }
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Trace;

        public override string Message => $"[{ColumnName}] Dropping.";

        public override int EventId => SyncEventsId.ColumnDropping.Id;

    }


    /// <summary>
    /// Partial interceptors extensions 
    /// </summary>
    public static partial class InterceptorsExtensions
    {
        /// <summary>
        /// Intercept the provider when a column is creating
        /// </summary>
        public static Guid OnColumnCreating(this BaseOrchestrator orchestrator, Action<ColumnCreatingArgs> action)
            => orchestrator.AddInterceptor(action);
        /// <summary>
        /// Intercept the provider when a column is creating
        /// </summary>
        public static Guid OnColumnCreating(this BaseOrchestrator orchestrator, Func<TableCreatingArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when a column is created
        /// </summary>
        public static Guid OnColumnCreated(this BaseOrchestrator orchestrator, Action<ColumnCreatedArgs> action)
            => orchestrator.AddInterceptor(action);
        /// <summary>
        /// Intercept the provider when a column is created
        /// </summary>
        public static Guid OnColumnCreated(this BaseOrchestrator orchestrator, Func<ColumnCreatedArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when a column is dropping
        /// </summary>
        public static Guid OnColumnDropping(this BaseOrchestrator orchestrator, Action<ColumnDroppingArgs> action)
            => orchestrator.AddInterceptor(action);
        /// <summary>
        /// Intercept the provider when a column is dropping
        /// </summary>
        public static Guid OnColumnDropping(this BaseOrchestrator orchestrator, Func<ColumnDroppingArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when a column is dropped
        /// </summary>
        public static Guid OnColumnDropped(this BaseOrchestrator orchestrator, Action<ColumnDroppedArgs> action)
            => orchestrator.AddInterceptor(action);
        /// <summary>
        /// Intercept the provider when a column is dropped
        /// </summary>
        public static Guid OnColumnDropped(this BaseOrchestrator orchestrator, Func<ColumnDroppedArgs, Task> action)
            => orchestrator.AddInterceptor(action);

    }

    public static partial class SyncEventsId
    {
        public static EventId ColumnCreating => CreateEventId(12300, nameof(ColumnCreating));
        public static EventId ColumnCreated => CreateEventId(12350, nameof(ColumnCreated));
        public static EventId ColumnDropping => CreateEventId(12400, nameof(ColumnDropping));
        public static EventId ColumnDropped => CreateEventId(12450, nameof(ColumnDropped));
    }

}
