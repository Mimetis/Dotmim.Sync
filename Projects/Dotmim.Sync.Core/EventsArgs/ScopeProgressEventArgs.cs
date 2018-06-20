using Dotmim.Sync.Data;
using Dotmim.Sync.Enumerations;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Dotmim.Sync
{

    public abstract class BaseProgressEventArgs
    {
        /// <summary>
        /// Gets the current stage
        /// </summary>
        public SyncStage Stage { get; }

        /// <summary>
        /// Gets or Sets the action to be taken : Could eventually Rollback the current processus
        /// </summary>
        public ChangeApplicationAction Action { get; set; }

        /// <summary>
        /// Gets the provider type name which raised the event
        /// </summary>
        public string ProviderTypeName { get; }

        /// <summary>
        /// Constructor
        /// </summary>
        public BaseProgressEventArgs(string providerTypeName, SyncStage stage)
        {
            this.ProviderTypeName = providerTypeName;
            this.Stage = stage;
        }

    }

    /// <summary>
    /// General sync progress. Only return a full string line
    /// </summary>
    public class ProgressEventArgs : BaseProgressEventArgs
    {
        public ProgressEventArgs(string providerTypeName, SyncStage stage, string message) : base(providerTypeName, stage)
        {
            this.Message = message;
        }

        public Dictionary<String, String> Properties { get; set; } = new Dictionary<string, string>();

        public String PropertiesMessage
        {
            get
            {
                if (Properties != null && Properties.Count > 0)
                    return String.Join(" ", Properties.Where(kvp => !String.IsNullOrEmpty(kvp.Value))
                                                      .Select((kvp) => $"{kvp.Key}: {kvp.Value}"));

                return string.Empty;
            }
        }

        public String Message { get; set; }
    }

    /// <summary>
    /// Event args generated during BeginSession stage
    /// </summary>
    public class BeginSessionEventArgs : BaseProgressEventArgs
    {
        public BeginSessionEventArgs(string providerTypeName, SyncStage stage) : base(providerTypeName, stage)
        {
        }
    }
    /// <summary>
    /// Event args generated during EndSession stage
    /// </summary>
    public class EndSessionEventArgs : BaseProgressEventArgs
    {
        public EndSessionEventArgs(string providerTypeName, SyncStage stage) : base(providerTypeName, stage)
        {
        }
    }

    /// <summary>
    /// Events args generated after database configuration has been applied
    /// </summary>
    public class DatabaseAppliedEventArgs : BaseProgressEventArgs
    {

        public DatabaseAppliedEventArgs(string providerTypeName, SyncStage stage, string script) : base(providerTypeName, stage)
        {
            this.Script = script;
        }

        /// <summary>
        /// Gets the script generated before applying on database
        /// </summary>
        public String Script { get; }
    }

    /// <summary>
    /// Event args generated before databas
    /// </summary>
    public class DatabaseApplyingEventArgs : BaseProgressEventArgs
    {
        public DatabaseApplyingEventArgs(string providerTypeName, SyncStage stage, DmSet schema) : base(providerTypeName, stage)
        {
            this.Schema = schema;
        }

        /// <summary>
        /// Gets the schema to be applied in the database
        /// </summary>
        public DmSet Schema{ get; private set; }

        /// <summary>
        /// Gets or Sets a boolean for overwriting the current schema. If True, all scripts are generated and applied
        /// </summary>
        public Boolean OverwriteSchema { get; set; }

        /// <summary>
        /// Gets or Sets a boolean value to specify if scripts should be generated, before applied.
        /// </summary>
        public Boolean GenerateScript { get; set; }

    }


    /// <summary>
    /// Events args generated after database configuration has been applied
    /// </summary>
    public class DatabaseTableAppliedEventArgs : BaseProgressEventArgs
    {

        public DatabaseTableAppliedEventArgs(string providerTypeName, SyncStage stage, string tableName, string script) : base(providerTypeName, stage)
        {
            TableName = tableName;
            this.Script = script;
        }
        /// <summary>
        /// Gets the table name where schema has been applied
        /// </summary>
        public string TableName { get; }

        /// <summary>
        /// Gets the script generated before if option is enabled
        /// </summary>
        public String Script { get; }
    }

    /// <summary>
    /// Event args generated before databas
    /// </summary>
    public class DatabaseTableApplyingEventArgs : BaseProgressEventArgs
    {
        public DatabaseTableApplyingEventArgs(string providerTypeName, SyncStage stage, string tableName) : base(providerTypeName, stage)
        {
            TableName = tableName;
        }

        /// <summary>
        /// Gets the table name where schema has been applied
        /// </summary>
        public string TableName { get; }
    }
    /// <summary>
    /// Events args generated after scope has been applied
    /// </summary>
    public class ScopeEventArgs : BaseProgressEventArgs
    {
        public ScopeEventArgs(string providerTypeName, SyncStage stage, ScopeInfo scope) : base(providerTypeName, stage)
        {
            this.ScopeInfo = scope;
        }

        /// <summary>
        /// Gets the current scope from the local database
        /// </summary>
        public ScopeInfo ScopeInfo { get; }
    }

    public class SchemaApplyingEventArgs : BaseProgressEventArgs
    {
        public SchemaApplyingEventArgs(string providerTypeName, SyncStage stage, DmSet schema) : base(providerTypeName, stage)
        {
            this.Schema = schema;
        }

        /// <summary>
        /// Gets or Sets a boolean for overwriting the current configuration. If True, all scripts are generated and applied
        /// </summary>
        public Boolean OverwriteConfiguration { get; set; }

        /// <summary>
        /// Gets the schema to be applied. If no tables are filled, the schema will be read.
        /// </summary>
        public DmSet Schema{ get; }
    }

    public class SchemaAppliedEventArgs : BaseProgressEventArgs
    {
        public SchemaAppliedEventArgs(string providerTypeName, SyncStage stage, DmSet schema) : base(providerTypeName, stage)
        {
            this.Schema= schema;
        }
        /// <summary>
        /// Get the schema applied
        /// </summary>
        public DmSet Schema{ get; }
    }



}
