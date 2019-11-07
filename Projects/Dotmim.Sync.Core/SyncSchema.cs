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
    public class SyncSchema
    {
        public const string DMSET_NAME = "DotmimSync";

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
        [DataMember(Name = "S")]
        public DmSet Set
        {
            get; set;
        }

        /// <summary>
        /// Gets/Sets the serialization converter object. Default is Json
        /// </summary>
        [DataMember(Name = "SF")]
        public SerializationFormat SerializationFormat { get; set; }

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

                    if (this.Set != null || this.Set.Tables != null)
                    {
                        foreach (var tbl in this.Set.Tables)
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

                    if (this.Set != null || this.Set.Tables != null)
                    {
                        foreach (var tbl in this.Set.Tables)
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

                    if (this.Set != null || this.Set.Tables != null)
                    {
                        foreach (var tbl in this.Set.Tables)
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

                    if (this.Set != null || this.Set.Tables != null)
                    {
                        foreach (var tbl in this.Set.Tables)
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

                    if (this.Set != null || this.Set.Tables != null)
                    {
                        foreach (var tbl in this.Set.Tables)
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

                    if (this.Set != null || this.Set.Tables != null)
                    {
                        foreach (var tbl in this.Set.Tables)
                        {
                            tbl.TrackingTablesSuffix = this._trackingTablesSuffix;
                        }
                    }

                }

            }
        }


        /// <summary>
        /// Get the default apply action on conflict resolution.
        /// Default is ServerWins
        /// </summary>
        public static ApplyAction GetApplyAction(ConflictResolutionPolicy policy) => policy == ConflictResolutionPolicy.ServerWins ?
             ApplyAction.Continue :
             ApplyAction.RetryWithForceWrite;

        public SyncSchema()
        {
            this.Set = new DmSet(DMSET_NAME);
            this.ConflictResolutionPolicy = ConflictResolutionPolicy.ServerWins;
            this.SerializationFormat = SerializationFormat.Json;
            this.Filters = new List<FilterClause>();
            this.ScopeName = SyncOptions.DefaultScopeName;
        }

        public SyncSchema(string[] tables) : this()
        {
            if (tables.Length <= 0)
                return;

            foreach (var table in tables)
                this.Add(table);
        }

        public SyncSchema Clone()
        {
            var syncConfiguration = new SyncSchema
            {
                ConflictResolutionPolicy = this.ConflictResolutionPolicy,
                Set = this.Set.Clone(),
                SerializationFormat = this.SerializationFormat,
                TrackingTablesSuffix = this.TrackingTablesSuffix,
                TrackingTablesPrefix = this.TrackingTablesPrefix,
                StoredProceduresPrefix = this.StoredProceduresPrefix,
                StoredProceduresSuffix = this.StoredProceduresSuffix,
                TriggersPrefix = this.TriggersPrefix,
                TriggersSuffix = this.TriggersSuffix,
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
                if (this.Set == null)
                    return 0;
                if (this.Set.Tables == null)
                    return 0;
                return this.Set.Tables.Count;
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
            if (this.Set == null || this.Set.Tables == null)
                throw new InvalidOperationException($"Can't add new table {table} in Configuration, ScopeSet is null");

            // Potentially user can pass something like [SalesLT].[Product]
            // or SalesLT.Product or Product. ObjectNameParser will handle it
            var parser = ParserName.Parse(table);

            var tableName = parser.ObjectName;
            var schema = parser.SchemaName;

            if (!this.Set.Tables.Contains(tableName))
            {
                var dmTable = new DmTable(tableName);
                if (!string.IsNullOrEmpty(schema))
                    dmTable.Schema = schema;

                dmTable.StoredProceduresPrefix = this.StoredProceduresPrefix;
                dmTable.StoredProceduresSuffix = this.StoredProceduresSuffix;
                dmTable.TrackingTablesPrefix = this.TrackingTablesPrefix;
                dmTable.TrackingTablesSuffix = this.TrackingTablesSuffix;

                this.Set.Tables.Add(dmTable);
            }
        }

        /// <summary>
        /// Adding table to configuration
        /// </summary>
        public void Add(DmTable item)
        {
            if (this.Set == null || this.Set.Tables == null)
                throw new InvalidOperationException("Can't add a dmTable in Configuration, ScopeSet is null");

            item.StoredProceduresPrefix = this.StoredProceduresPrefix;
            item.StoredProceduresSuffix = this.StoredProceduresSuffix;
            item.TrackingTablesPrefix = this.TrackingTablesPrefix;
            item.TrackingTablesSuffix = this.TrackingTablesSuffix;

            this.Set.Tables.Add(item);
        }

    }
}
