using Dotmim.Sync.Builders;
using Dotmim.Sync.Data;
using Dotmim.Sync.Data.Surrogate;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Filter;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.Serialization;

namespace Dotmim.Sync
{
    [DataContract]
    public class SyncSchema
    {
        public const string DMSET_NAME = "DotmimSync";

        private string trackingTablesSuffix;
        private string trackingTablesPrefix;
        private string storedProceduresSuffix;
        private string storedProceduresPrefix;
        private string triggersSuffix;
        private string triggersPrefix;
        private DmSet dmSet = null;

        /// <summary>
        /// Gets or Sets the default conflict resolution policy.
        /// </summary>
        public ConflictResolutionPolicy ConflictResolutionPolicy { get; set; } = ConflictResolutionPolicy.ServerWins;

        /// <summary>
        /// Gets or Sets the DmSet Schema used for synchronization
        /// </summary>
        public DmSet GetSet() 
        {
            if (this.dmSet == null)
                this.dmSet = this.SetLight.ConvertToDmSet();

            return this.dmSet;
        }

        [DataMember(Name = "s")]
        public DmSetLightSchema SetLight { get; set; }

        /// <summary>
        /// Gets or Sets the current scope name
        /// </summary>
        [DataMember(Name = "sn")]
        public string ScopeName { get; set; }

        /// <summary>
        /// Filters applied on tables
        /// </summary>
        [DataMember(Name = "f")]
        public ICollection<FilterClause> Filters { get; set; }

        /// <summary>
        /// Specify a prefix for naming stored procedure. Default is empty string
        /// </summary>
        [DataMember(Name = "spp")]
        public string StoredProceduresPrefix
        {
            get => this.storedProceduresPrefix;
            set
            {
                if (this.storedProceduresPrefix != value)
                {
                    this.storedProceduresPrefix = value;

                    if (this.GetSet() != null || this.GetSet().Tables != null)
                    {
                        foreach (var tbl in this.GetSet().Tables)
                        {
                            tbl.StoredProceduresPrefix = this.storedProceduresPrefix;
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Specify a suffix for naming stored procedures. Default is empty string
        /// </summary>
        [DataMember(Name = "sps")]
        public string StoredProceduresSuffix
        {
            get => this.storedProceduresSuffix;
            set
            {
                if (this.storedProceduresSuffix != value)
                {
                    this.storedProceduresSuffix = value;

                    if (this.GetSet() != null || this.GetSet().Tables != null)
                    {
                        foreach (var tbl in this.GetSet().Tables)
                        {
                            tbl.StoredProceduresSuffix = this.storedProceduresSuffix;
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Specify a prefix for naming stored procedure. Default is empty string
        /// </summary>
        [DataMember(Name = "tp")]
        public string TriggersPrefix
        {
            get => this.triggersPrefix;
            set
            {
                if (this.triggersPrefix != value)
                {
                    this.triggersPrefix = value;

                    if (this.GetSet() != null || this.GetSet().Tables != null)
                    {
                        foreach (var tbl in this.GetSet().Tables)
                        {
                            tbl.TriggersPrefix = this.triggersPrefix;
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Specify a suffix for naming stored procedures. Default is empty string
        /// </summary>
        [DataMember(Name = "ts")]
        public string TriggersSuffix
        {
            get => this.triggersSuffix;
            set
            {
                if (this.triggersSuffix != value)
                {
                    this.triggersSuffix = value;

                    if (this.GetSet() != null || this.GetSet().Tables != null)
                    {
                        foreach (var tbl in this.GetSet().Tables)
                        {
                            tbl.TriggersSuffix = this.triggersSuffix;
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Specify a prefix for naming tracking tables. Default is empty string
        /// </summary>
        [DataMember(Name = "ttp")]
        public string TrackingTablesPrefix
        {
            get => this.trackingTablesPrefix;
            set
            {
                if (this.trackingTablesPrefix != value)
                {
                    this.trackingTablesPrefix = value;

                    if (this.GetSet() != null || this.GetSet().Tables != null)
                    {
                        foreach (var tbl in this.GetSet().Tables)
                        {
                            tbl.TrackingTablesPrefix = this.trackingTablesPrefix;
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Specify a suffix for naming tracking tables.
        /// </summary>
        [DataMember(Name = "tts")]
        public string TrackingTablesSuffix
        {
            get => this.trackingTablesSuffix;
            set
            {
                if (this.trackingTablesSuffix != value)
                {
                    this.trackingTablesSuffix = value;

                    if (this.GetSet() != null || this.GetSet().Tables != null)
                    {
                        foreach (var tbl in this.GetSet().Tables)
                        {
                            tbl.TrackingTablesSuffix = this.trackingTablesSuffix;
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
            this.dmSet = new DmSet(DMSET_NAME);
            this.ConflictResolutionPolicy = ConflictResolutionPolicy.ServerWins;
            this.Filters = new List<FilterClause>();
            this.ScopeName = SyncOptions.DefaultScopeName;
        }

        public SyncSchema(string[] tables) : this()
        {
            if (tables == null || tables.Length <= 0)
                return;

            foreach (var table in tables)
                this.Add(table);
        }

        public int Count
        {
            get
            {
                if (this.GetSet() == null)
                    return 0;
                if (this.GetSet().Tables == null)
                    return 0;
                return this.GetSet().Tables.Count;
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
            if (this.GetSet() == null || this.GetSet().Tables == null)
                throw new InvalidOperationException($"Can't add new table {table} in Configuration, ScopeSet is null");

            // Potentially user can pass something like [SalesLT].[Product]
            // or SalesLT.Product or Product. ObjectNameParser will handle it
            var parser = ParserName.Parse(table);

            var tableName = parser.ObjectName;
            var schema = parser.SchemaName;

            if (!this.GetSet().Tables.Contains(tableName))
            {
                var dmTable = new DmTable(tableName);
                if (!string.IsNullOrEmpty(schema))
                    dmTable.Schema = schema;

                dmTable.StoredProceduresPrefix = this.StoredProceduresPrefix;
                dmTable.StoredProceduresSuffix = this.StoredProceduresSuffix;
                dmTable.TrackingTablesPrefix = this.TrackingTablesPrefix;
                dmTable.TrackingTablesSuffix = this.TrackingTablesSuffix;

                this.GetSet().Tables.Add(dmTable);
            }
        }

        /// <summary>
        /// Adding table to configuration
        /// </summary>
        public void Add(DmTable item)
        {
            if (this.GetSet() == null || this.GetSet().Tables == null)
                throw new InvalidOperationException("Can't add a dmTable in Configuration, ScopeSet is null");

            item.StoredProceduresPrefix = this.StoredProceduresPrefix;
            item.StoredProceduresSuffix = this.StoredProceduresSuffix;
            item.TrackingTablesPrefix = this.TrackingTablesPrefix;
            item.TrackingTablesSuffix = this.TrackingTablesSuffix;

            this.GetSet().Tables.Add(item);
        }

    }
}
