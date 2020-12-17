using Dotmim.Sync.Builders;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;

namespace Dotmim.Sync
{


    public class SchemaNameCreatedArgs : ProgressArgs
    {
        public SyncTable Table { get; }

        public SchemaNameCreatedArgs(SyncContext context, SyncTable table, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.Table = table;
        }

        public override string Message => $"[{Connection.Database}] [{this.Table.SchemaName}] schema created.";

        public override int EventId => 43;
    }

    public class SchemaNameCreatingArgs : ProgressArgs
    {
        public bool Cancel { get; set; } = false;
        public DbCommand Command { get; }
        public SyncTable Table { get; }

        public SchemaNameCreatingArgs(SyncContext context, SyncTable table, DbCommand command, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.Table = table;
            this.Command = command;
        }

    }

    public class TableCreatedArgs : ProgressArgs
    {
        public SyncTable Table { get; }
        public ParserName TableName { get; }

        public TableCreatedArgs(SyncContext context, SyncTable table, ParserName tableName, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.TableName = tableName;
            this.Table = table;
        }

        public override string Message => $"[{Connection.Database}] [{this.Table.GetFullName()}] table created.";

        public override int EventId => 43;
    }

    public class TableCreatingArgs : ProgressArgs
    {
        public bool Cancel { get; set; } = false;
        public DbCommand Command { get; }
        public SyncTable Table { get; }
        public ParserName TableName { get; }

        public TableCreatingArgs(SyncContext context, SyncTable table, ParserName tableName, DbCommand command, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.Table = table;
            this.TableName = tableName;
            this.Command = command;
        }

        public override string Message => $"[{Connection.Database}] [{Table.GetFullName()}] table creating.";

    }

    public class TableDroppedArgs : ProgressArgs
    {
        public SyncTable Table { get; }
        public ParserName TableName { get; }

        public TableDroppedArgs(SyncContext context, SyncTable table, ParserName tableName, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.TableName = tableName;
            this.Table = table;
        }

        public override string Message => $"[{Connection.Database}] [{Table.GetFullName()}] table dropped.";

        public override int EventId => 45;
    }

    public class TableDroppingArgs : ProgressArgs
    {
        public bool Cancel { get; set; } = false;
        public DbCommand Command { get; }
        public SyncTable Table { get; }
        public ParserName TableName { get; }

        public TableDroppingArgs(SyncContext context, SyncTable table, ParserName tableName, DbCommand command, DbConnection connection = null, DbTransaction transaction = null)
            : base(context,  connection, transaction)
        {
            this.Command = command;
            this.TableName = tableName;
            this.Table = table;
        }

        public override string Message => $"[{Connection.Database}] [{Table.GetFullName()}] table dropping.";


    }


    /// <summary>
    /// Partial interceptors extensions 
    /// </summary>
    public static partial class InterceptorsExtensions
    {
        /// <summary>
        /// Intercept the provider when database schema is created (works only on SQL Server)
        /// </summary>
        public static void OnSchemaNameCreated(this BaseOrchestrator orchestrator, Action<SchemaNameCreatedArgs> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider when database schema is creating (works only on SQL Server)
        /// </summary>
        public static void OnSchemaNameCreating(this BaseOrchestrator orchestrator, Action<SchemaNameCreatingArgs> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider when a table is creating
        /// </summary>
        public static void OnTableCreating(this BaseOrchestrator orchestrator, Action<TableCreatingArgs> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider when a table is created
        /// </summary>
        public static void OnTableCreated(this BaseOrchestrator orchestrator, Action<TableCreatedArgs> action)
            => orchestrator.SetInterceptor(action);


        /// <summary>
        /// Intercept the provider when a table is dropping
        /// </summary>
        public static void OnTableDropping(this BaseOrchestrator orchestrator, Action<TableDroppingArgs> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider when a table is dropped
        /// </summary>
        public static void OnTableDropped(this BaseOrchestrator orchestrator, Action<TableDroppedArgs> action)
            => orchestrator.SetInterceptor(action);




    }

}
