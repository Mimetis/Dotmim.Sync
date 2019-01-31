//using Dotmim.Sync.Data;
//using Dotmim.Sync.Enumerations;
//using System;
//using System.Collections.Generic;
//using System.Data.Common;
//using System.Linq;

//namespace Dotmim.Sync
//{

    

//    /// <summary>
//    /// Event args generated during BeginSession stage
//    /// </summary>
//    public class BeginSessionEventArgs : ProgressEventArgs
//    {
//        public BeginSessionEventArgs(string providerTypeName, SyncStage stage, DbConnection connection, DbTransaction transaction) : base(providerTypeName, stage, connection, transaction)
//        {
//        }
//    }
//    /// <summary>
//    /// Event args generated during EndSession stage
//    /// </summary>
//    public class EndSessionEventArgs : ProgressEventArgs
//    {
//        public EndSessionEventArgs(string providerTypeName, SyncStage stage, DbConnection connection, DbTransaction transaction) : base(providerTypeName, stage, connection, transaction)
//        {
//        }
//    }

//    /// <summary>
//    /// Events args generated after database configuration has been applied
//    /// </summary>
//    public class DatabaseAppliedEventArgs : ProgressEventArgs
//    {

//        public DatabaseAppliedEventArgs(string providerTypeName, SyncStage stage, string script, DbConnection connection, DbTransaction transaction) : base(providerTypeName, stage, connection, transaction)
//        {
//            this.Script = script;
//        }

//        /// <summary>
//        /// Gets the script generated before applying on database
//        /// </summary>
//        public String Script { get; }
//    }

//    /// <summary>
//    /// Event args generated before databas
//    /// </summary>
//    public class DatabaseApplyingEventArgs : ProgressEventArgs
//    {
//        public DatabaseApplyingEventArgs(string providerTypeName, SyncStage stage, DmSet schema, DbConnection connection, DbTransaction transaction) : base(providerTypeName, stage, connection, transaction)
//        {
//            this.Schema = schema;
//        }

//        /// <summary>
//        /// Gets the schema to be applied in the database
//        /// </summary>
//        public DmSet Schema{ get; private set; }

//        /// <summary>
//        /// Gets or Sets a boolean for overwriting the current schema. If True, all scripts are generated and applied
//        /// </summary>
//        public Boolean OverwriteSchema { get; set; }

//        /// <summary>
//        /// Gets or Sets a boolean value to specify if scripts should be generated, before applied.
//        /// </summary>
//        public Boolean GenerateScript { get; set; }

//    }


//    /// <summary>
//    /// Events args generated after database configuration has been applied
//    /// </summary>
//    public class DatabaseTableAppliedEventArgs : ProgressEventArgs
//    {

//        public DatabaseTableAppliedEventArgs(string providerTypeName, SyncStage stage, string tableName, string script, DbConnection connection, DbTransaction transaction) : base(providerTypeName, stage, connection, transaction)
//        {
//            TableName = tableName;
//            this.Script = script;
//        }
//        /// <summary>
//        /// Gets the table name where schema has been applied
//        /// </summary>
//        public string TableName { get; }

//        /// <summary>
//        /// Gets the script generated before if option is enabled
//        /// </summary>
//        public String Script { get; }
//    }

//    /// <summary>
//    /// Event args generated before databas
//    /// </summary>
//    public class DatabaseTableApplyingEventArgs : ProgressEventArgs
//    {
//        public DatabaseTableApplyingEventArgs(string providerTypeName, SyncStage stage, string tableName, DbConnection connection, DbTransaction transaction) : base(providerTypeName, stage, connection, transaction)
//        {
//            TableName = tableName;
//        }

//        /// <summary>
//        /// Gets the table name where schema has been applied
//        /// </summary>
//        public string TableName { get; }
//    }
    
//    /// <summary>
//    /// Events args generated after scope has been applied
//    /// </summary>
//    public class ScopeEventArgs : ProgressEventArgs
//    {
//        public ScopeEventArgs(string providerTypeName, SyncStage stage, ScopeInfo scope, DbConnection connection, DbTransaction transaction) : base(providerTypeName, stage, connection, transaction)
//        {
//            this.ScopeInfo = scope;
//        }

//        /// <summary>
//        /// Gets the current scope from the local database
//        /// </summary>
//        public ScopeInfo ScopeInfo { get; }
//    }

//    public class SchemaApplyingEventArgs : ProgressEventArgs
//    {
//        public SchemaApplyingEventArgs(string providerTypeName, SyncStage stage, DmSet schema, DbConnection connection, DbTransaction transaction) : base(providerTypeName, stage, connection, transaction)
//        {
//            this.Schema = schema;
//        }

//        /// <summary>
//        /// Gets or Sets a boolean for overwriting the current configuration. If True, all scripts are generated and applied
//        /// </summary>
//        public Boolean OverwriteConfiguration { get; set; }

//        /// <summary>
//        /// Gets the schema to be applied. If no tables are filled, the schema will be read.
//        /// </summary>
//        public DmSet Schema{ get; }
//    }

//    public class SchemaAppliedEventArgs : ProgressEventArgs
//    {
//        public SchemaAppliedEventArgs(string providerTypeName, SyncStage stage, DmSet schema, DbConnection connection, DbTransaction transaction) : base(providerTypeName, stage, connection, transaction)
//        {
//            this.Schema= schema;
//        }
//        /// <summary>
//        /// Get the schema applied
//        /// </summary>
//        public DmSet Schema{ get; }
//    }



//}
