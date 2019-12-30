using Dotmim.Sync.Builders;
using Dotmim.Sync.Data;
using Dotmim.Sync.Filter;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;

namespace Dotmim.Sync
{

    [DataContract(Name = "s"), Serializable]
    public class SyncSet : IDisposable
    {
        /// <summary>
        /// Gets or Sets the name of the data source (database name)
        /// </summary>
        [DataMember(Name = "n", IsRequired = true, Order = 1)]
        public string DataSourceName { get; set; }

        /// <summary>
        /// Gets or Sets the current scope name
        /// </summary>
        [DataMember(Name = "sn", IsRequired = false, EmitDefaultValue = false, Order = 2)]
        public string ScopeName { get; set; }

        /// <summary>
        /// Gets or sets the locale information used to compare strings within the table.
        /// </summary>
        [DataMember(Name = "ci", IsRequired = false, EmitDefaultValue = false, Order = 3)]
        public string CultureInfoName { get; set; }

        /// <summary>
        /// Gets or sets the Case sensitive rul of the DmSet that the DmSetSurrogate object represents.
        /// </summary>
        [DataMember(Name = "cs", IsRequired = false, EmitDefaultValue = false, Order = 4)]
        public bool CaseSensitive { get; set; }

        /// <summary>
        /// Specify a prefix for naming stored procedure. Default is empty string
        /// </summary>
        [DataMember(Name = "spp", IsRequired = false, EmitDefaultValue = false, Order = 5)]
        public string StoredProceduresPrefix { get; set; }

        /// <summary>
        /// Specify a suffix for naming stored procedures. Default is empty string
        /// </summary>
        [DataMember(Name = "sps", IsRequired = false, EmitDefaultValue = false, Order = 6)]
        public string StoredProceduresSuffix { get; set; }

        /// <summary>
        /// Specify a prefix for naming stored procedure. Default is empty string
        /// </summary>
        [DataMember(Name = "tp", IsRequired = false, EmitDefaultValue = false, Order = 7)]
        public string TriggersPrefix { get; set; }

        /// <summary>
        /// Specify a suffix for naming stored procedures. Default is empty string
        /// </summary>
        [DataMember(Name = "ts", IsRequired = false, EmitDefaultValue = false, Order = 8)]
        public string TriggersSuffix { get; set; }

        /// <summary>
        /// Specify a prefix for naming tracking tables. Default is empty string
        /// </summary>
        [DataMember(Name = "ttp", IsRequired = false, EmitDefaultValue = false, Order = 9)]
        public string TrackingTablesPrefix { get; set; }

        /// <summary>
        /// Specify a suffix for naming tracking tables.
        /// </summary>
        [DataMember(Name = "tts", IsRequired = false, EmitDefaultValue = false, Order = 10)]
        public string TrackingTablesSuffix { get; set; }

        /// <summary>
        /// Gets or Sets an array of DmTableSurrogate objects that comprise 
        /// the dm set that is represented by the DmSetSurrogate object.
        /// </summary>
        [DataMember(Name = "t", IsRequired = false, EmitDefaultValue = false, Order = 11)]
        public SyncTables Tables { get; set; }

        /// <summary>
        /// Gets or Sets an array of every SchemaRelation belong to this Schema
        /// </summary>
        [DataMember(Name = "r", IsRequired = false, EmitDefaultValue = false, Order = 12)]
        public SyncRelations Relations { get; set; }

        /// <summary>
        /// Filters applied on tables
        /// </summary>
        [DataMember(Name = "f", IsRequired = false, EmitDefaultValue = false, Order = 13)]
        public SyncFilters Filters { get; set; }


        /// <summary>
        /// Only used for Serialization
        /// </summary>
        public SyncSet()
        {
            this.Tables = new SyncTables(this);
            this.Relations = new SyncRelations(this);
            this.Filters = new SyncFilters(this);
            this.ScopeName = SyncOptions.DefaultScopeName;
        }

        public SyncSet(string dataSourceName, bool caseSensitive, string cultureInfoName = null, string scopeName = SyncOptions.DefaultScopeName) : this()
        {
            this.DataSourceName = dataSourceName;
            this.CultureInfoName = cultureInfoName;
            this.CaseSensitive = caseSensitive;
            this.ScopeName = scopeName;
        }

        /// <summary>
        /// Create a new Sync Schema with Tables to be read on server side
        /// </summary>
        public SyncSet(string[] tables, string scopeName = SyncOptions.DefaultScopeName) : this()
        {
            this.ScopeName = scopeName;

            if (tables == null || tables.Length <= 0)
                return;

            foreach (var table in tables)
                this.Tables.Add(table);
        }

        /// <summary>
        /// Ensure all tables, filters and relations has the correct reference to this schema
        /// </summary>
        public void EnsureSchema()
        {
            if (this.Tables != null)
                this.Tables.EnsureTables(this);

            if (this.Relations != null)
                this.Relations.EnsureRelations(this);

            if (this.Filters != null)
                this.Filters.EnsureFilters(this);
        }

        /// <summary>
        /// Clone the SyncSet schema (without data)
        /// </summary>
        public SyncSet Clone(bool includeTables = true)
        {
            var clone = new SyncSet();
            clone.CaseSensitive = this.CaseSensitive;
            clone.CultureInfoName = this.CultureInfoName;
            clone.DataSourceName = this.DataSourceName;
            clone.ScopeName = this.ScopeName;
            clone.StoredProceduresPrefix = this.StoredProceduresPrefix;
            clone.StoredProceduresSuffix = this.StoredProceduresSuffix;
            clone.TrackingTablesPrefix = this.TrackingTablesPrefix;
            clone.TrackingTablesSuffix = this.TrackingTablesSuffix;
            clone.TriggersPrefix = this.TriggersPrefix;
            clone.TriggersSuffix = this.TriggersSuffix;

            if (!includeTables)
                return clone;

            foreach (var f in this.Filters)
                clone.Filters.Add(f.Clone());

            foreach (var r in this.Relations)
                clone.Relations.Add(r.Clone());

            foreach (var t in this.Tables)
                clone.Tables.Add(t.Clone());

            // Ensure all elements has the correct ref to its parent
            clone.EnsureSchema();

            return clone;
        }



        /// <summary>
        /// Import a container set in a SyncSet instance
        /// </summary>
        public void ImportContainerSet(ContainerSet containerSet)
        {
            this.DataSourceName = containerSet.DataSourceName;

            foreach (var table in containerSet.Tables)
            {
                var syncTable = this.Tables[table.TableName, table.SchemaName];

                if (syncTable == null)
                    throw new ArgumentNullException($"Table {table.TableName} does not exist in the SyncSet");

                syncTable.Rows.ImportContainerTable(table);
            }

        }

        /// <summary>
        /// Get the rows inside a container.
        /// ContainerSet is a serialization container for rows
        /// </summary>
        public ContainerSet GetContainerSet()
        {
            var containerSet = new ContainerSet(this.DataSourceName);
            foreach (var table in this.Tables)
            {
                var containerTable = new ContainerTable(table)
                {
                    Rows = table.Rows.ExportToContainerTable().ToList()
                };

                if (containerTable.Rows.Count > 0)
                    containerSet.Tables.Add(containerTable);
            }

            return containerSet;
        }


        /// <summary>
        /// Clear the SyncSet
        /// </summary>
        public void Clear() => this.Dispose(true);


        /// <summary>
        /// Dispose the whole SyncSet
        /// </summary>
        public void Dispose()
        {
            this.Dispose(true);
            if (this.Tables != null)
                this.Tables.Schema = null;
            if (this.Relations != null)
                this.Relations.Schema = null;
            if (this.Filters != null)
                this.Filters.Schema = null;

            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool cleanup)
        {
            // Dispose managed ressources
            if (cleanup)
            {
                if (this.Tables != null)
                    this.Tables.Clear();

                if (this.Relations != null)
                    this.Relations.Clear();

                if (this.Filters != null)
                    this.Filters.Clear();
            }

            // Dispose unmanaged ressources
        }


        /// <summary>
        /// Check if Schema has tables
        /// </summary>
        public bool HasTables => this.Tables?.Count > 0;

        /// <summary>
        /// Check if Schema has at least one table with columns
        /// </summary>
        public bool HasColumns => this.Tables?.SelectMany(t => t.Columns).Count() > 0;  // using SelectMany to get columns and not Collection<Column>


        /// <summary>
        /// Gets if at least one table as at least one row
        /// </summary>
        public bool HasRows
        {
            get
            {
                if (!HasTables)
                    return false;

                // Check if any of the tables has rows inside
                return this.Tables.Any(t => t.Rows != null && t.Rows.Count > 0);
            }
        }
    }
}
