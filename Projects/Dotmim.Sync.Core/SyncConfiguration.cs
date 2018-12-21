using Dotmim.Sync.Builders;
using Dotmim.Sync.Data;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Filter;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.Serialization;

namespace Dotmim.Sync
{
    [Serializable]
    public class SyncConfiguration
    {
        public const string DMSET_NAME = "DotmimSync";

        [NonSerialized]
        [JsonIgnore]
        private DmSet scopeSet;

        private string _trackingTablesSuffix;
        private string _trackingTablesPrefix;
        private string _storedProceduresSuffix;
        private string _storedProceduresPrefix;
        private string _triggersSuffix;
        private string _triggersPrefix;

        /// <summary>
        /// Gets or Sets the default conflict resolution policy.
        /// </summary>
        public ConflictResolutionPolicy ConflictResolutionPolicy { get; set; } = ConflictResolutionPolicy.ServerWins;

        /// <summary>
        /// Gets or Sets the DmSet Schema used for synchronization
        /// </summary>

        [JsonIgnore]
        public DmSet Schema
        {
            get => this.scopeSet;
            set => this.scopeSet = value;
        }

        /// <summary>
        /// Gets or Sets the directory used for batch mode.
        /// Default value is [Windows Temp Path]/[DotmimSync]
        /// </summary>
        [DataMember(Name = "BD")]
        public string BatchDirectory { get; set; }


        /// <summary>
        /// Gets or Sets the size used for downloading in batch mode. 
        /// Default is 0 (no batch mode)
        /// </summary>
        [DataMember(Name = "DBSKB")]
        public int DownloadBatchSizeInKB { get; set; }

        /// <summary>
        /// Gets or Sets the archive name, saved in the BatchDirectory, 
        /// containing the zip starter for any new client
        /// </summary>
        [DataMember(Name = "A")]
        public string Archive { get; set; }

        /// <summary>
        /// Gets/Sets the serialization converter object. Default is Json
        /// </summary>
        [DataMember(Name = "SF")]
        public SerializationFormat SerializationFormat { get; set; }

        /// <summary>
        /// Gets/Sets the log level for sync operations. Default value is false.
        /// </summary>
        [DataMember(Name = "UVE")]
        public bool UseVerboseErrors { get; set; }

        /// <summary>
        /// Gets or Sets if we should use the bulk operations. Default is true.
        /// If provider doe not support bulk operations, this option is overrided to false.
        /// </summary>
        [DataMember(Name = "UBO")]
        public bool UseBulkOperations { get; set; } = true;

        /// <summary>
        /// Gets or Sets if we should cleaning tmp dir files after sync.
        /// </summary>
        [DataMember(Name = "CM")]
        public bool CleanMetadatas { get; set; } = true;

        /// <summary>
        /// Gets or Sets the current scope name
        /// </summary>
        [DataMember(Name = "SN")]
        public string ScopeName { get; set; }

        /// <summary>
        /// Filters applied on tables
        /// </summary>
        [DataMember(Name = "F")]
        public ICollection<FilterClause> Filters { get; set; }

        /// <summary>
        /// Specify a prefix for naming stored procedure. Default is empty string
        /// </summary>
        [DataMember(Name = "SPP")]
        public string StoredProceduresPrefix
        {
            get => this._storedProceduresPrefix;
            set
            {
                if (this._storedProceduresPrefix != value)
                {
                    this._storedProceduresPrefix = value;

                    if (this.Schema != null || this.Schema.Tables != null)
                    {
                        foreach (var tbl in this.Schema.Tables)
                        {
                            tbl.StoredProceduresPrefix = this._storedProceduresPrefix;
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Specify a suffix for naming stored procedures. Default is empty string
        /// </summary>
        [DataMember(Name = "SPS")]
        public string StoredProceduresSuffix
        {
            get => this._storedProceduresSuffix;
            set
            {
                if (this._storedProceduresSuffix != value)
                {
                    this._storedProceduresSuffix = value;

                    if (this.Schema != null || this.Schema.Tables != null)
                    {
                        foreach (var tbl in this.Schema.Tables)
                        {
                            tbl.StoredProceduresSuffix = this._storedProceduresSuffix;
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Specify a prefix for naming stored procedure. Default is empty string
        /// </summary>
        [DataMember(Name = "TP")]
        public string TriggersPrefix
        {
            get => this._triggersPrefix;
            set
            {
                if (this._triggersPrefix != value)
                {
                    this._triggersPrefix = value;

                    if (this.Schema != null || this.Schema.Tables != null)
                    {
                        foreach (var tbl in this.Schema.Tables)
                        {
                            tbl.TriggersPrefix = this._triggersPrefix;
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Specify a suffix for naming stored procedures. Default is empty string
        /// </summary>
        [DataMember(Name = "TS")]
        public string TriggersSuffix
        {
            get => this._triggersSuffix;
            set
            {
                if (this._triggersSuffix != value)
                {
                    this._triggersSuffix = value;

                    if (this.Schema != null || this.Schema.Tables != null)
                    {
                        foreach (var tbl in this.Schema.Tables)
                        {
                            tbl.TriggersSuffix = this._triggersSuffix;
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Specify a prefix for naming tracking tables. Default is empty string
        /// </summary>
        [DataMember(Name = "TTP")]
        public string TrackingTablesPrefix
        {
            get => this._trackingTablesPrefix;
            set
            {
                if (this._trackingTablesPrefix != value)
                {
                    this._trackingTablesPrefix = value;

                    if (this.Schema != null || this.Schema.Tables != null)
                    {
                        foreach (var tbl in this.Schema.Tables)
                        {
                            tbl.TrackingTablesPrefix = this._trackingTablesPrefix;
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Specify a suffix for naming tracking tables.
        /// </summary>
        [DataMember(Name = "TTS")]
        public string TrackingTablesSuffix
        {
            get => this._trackingTablesSuffix;
            set
            {
                if (this._trackingTablesSuffix != value)
                {
                    this._trackingTablesSuffix = value;

                    if (this.Schema != null || this.Schema.Tables != null)
                    {
                        foreach (var tbl in this.Schema.Tables)
                        {
                            tbl.TrackingTablesSuffix = this._trackingTablesSuffix;
                        }
                    }

                }

            }
        }

        /// <summary>
        /// Gets or Sets the scope_info table name. Default is scope_info
        /// </summary>
        [DataMember(Name = "SIT")]
        public string ScopeInfoTableName { get; set; }

        /// <summary>
        /// Get the default apply action on conflict resolution.
        /// Default is ServerWins
        /// </summary>
        public static ApplyAction GetApplyAction(ConflictResolutionPolicy policy) => policy == ConflictResolutionPolicy.ServerWins ?
             ApplyAction.Continue :
             ApplyAction.RetryWithForceWrite;



        public SyncConfiguration()
        {
            this.Schema = new DmSet(DMSET_NAME);
            this.ConflictResolutionPolicy = ConflictResolutionPolicy.ServerWins;
            this.DownloadBatchSizeInKB = 0;
            this.UseVerboseErrors = false;
            this.BatchDirectory = Path.Combine(Path.GetTempPath(), "DotmimSync");
            this.SerializationFormat = SerializationFormat.Json;
            this.Filters = new List<FilterClause>();
            this.ScopeInfoTableName = "scope_info";
            this.ScopeName = "DefaultScope";
            this.CleanMetadatas = true;
        }

        public SyncConfiguration(string[] tables) : this()
        {
            if (tables.Length <= 0)
                return;

            foreach (var table in tables)
                this.Add(table);
        }

        public SyncConfiguration Clone()
        {
            var syncConfiguration = new SyncConfiguration
            {
                BatchDirectory = this.BatchDirectory,
                CleanMetadatas = this.CleanMetadatas,
                ConflictResolutionPolicy = this.ConflictResolutionPolicy,
                DownloadBatchSizeInKB = this.DownloadBatchSizeInKB,
                Schema = this.Schema.Clone(),
                UseBulkOperations = this.UseBulkOperations,
                UseVerboseErrors = this.UseVerboseErrors,
                SerializationFormat = this.SerializationFormat,
                Archive = this.Archive,
                TrackingTablesSuffix = this.TrackingTablesSuffix,
                TrackingTablesPrefix = this.TrackingTablesPrefix,
                StoredProceduresPrefix = this.StoredProceduresPrefix,
                StoredProceduresSuffix = this.StoredProceduresSuffix,
                TriggersPrefix = this.TriggersPrefix,
                TriggersSuffix = this.TriggersSuffix,
                ScopeInfoTableName = this.ScopeInfoTableName,
                ScopeName = this.ScopeName
            };

            if (this.Filters != null)
                foreach (var p in this.Filters)
                    syncConfiguration.Filters.Add(new FilterClause(p.TableName, p.ColumnName, p.ColumnType));

            return syncConfiguration;
        }


        public int Count
        {
            get
            {
                if (this.Schema == null)
                    return 0;
                if (this.Schema.Tables == null)
                    return 0;
                return this.Schema.Tables.Count;
            }
        }

        public bool IsReadOnly => false;

        /// <summary>
        /// Adding tables to configuration
        /// </summary>
        public void Add(string[] tables)
        {
            foreach (var table in tables)
                this.Add(table);
        }

        /// <summary>
        /// Adding tables to configuration
        /// </summary>
        public void Add(string table)
        {
            if (this.Schema == null || this.Schema.Tables == null)
                throw new InvalidOperationException($"Can't add new table {table} in Configuration, ScopeSet is null");

            // Potentially user can pass something like [SalesLT].[Product]
            // or SalesLT.Product or Product. ObjectNameParser will handle it
            var parser = new ObjectNameParser(table);

            var tableName = parser.ObjectName;
            var schema = parser.SchemaName;

            if (!this.Schema.Tables.Contains(tableName))
            {
                var dmTable = new DmTable(tableName);
                if (!string.IsNullOrEmpty(schema))
                    dmTable.Schema = schema;

                dmTable.StoredProceduresPrefix = this.StoredProceduresPrefix;
                dmTable.StoredProceduresSuffix = this.StoredProceduresSuffix;
                dmTable.TrackingTablesPrefix = this.TrackingTablesPrefix;
                dmTable.TrackingTablesSuffix = this.TrackingTablesSuffix;

                this.Schema.Tables.Add(dmTable);
            }
        }

        /// <summary>
        /// Adding table to configuration
        /// </summary>
        public void Add(DmTable item)
        {
            if (this.Schema == null || this.Schema.Tables == null)
                throw new InvalidOperationException("Can't add a dmTable in Configuration, ScopeSet is null");

            item.StoredProceduresPrefix = this.StoredProceduresPrefix;
            item.StoredProceduresSuffix = this.StoredProceduresSuffix;
            item.TrackingTablesPrefix = this.TrackingTablesPrefix;
            item.TrackingTablesSuffix = this.TrackingTablesSuffix;

            this.Schema.Tables.Add(item);
        }

    }
}
