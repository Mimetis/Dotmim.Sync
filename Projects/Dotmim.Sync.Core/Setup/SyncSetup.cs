using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;

namespace Dotmim.Sync
{
    [DataContract(Name = "s"), Serializable]
    public class SyncSetup : IEquatable<SyncSetup>
    {

        /// <summary>
        /// Gets or Sets the tables involved in the sync
        /// </summary>
        [DataMember(Name = "tbls", IsRequired = false, EmitDefaultValue = false, Order = 1)]
        public SetupTables Tables { get; set; }

        /// <summary>
        /// Gets or Sets the filters involved in the sync
        /// </summary>
        [DataMember(Name = "fils", IsRequired = false, EmitDefaultValue = false, Order = 2)]
        public SetupFilters Filters { get; set; }

        /// <summary>
        /// Specify a prefix for naming stored procedure. Default is empty string
        /// </summary>
        [DataMember(Name = "spp", IsRequired = false, EmitDefaultValue = false, Order = 3)]
        public string StoredProceduresPrefix { get; set; }

        /// <summary>
        /// Specify a suffix for naming stored procedures. Default is empty string
        /// </summary>
        [DataMember(Name = "sps", IsRequired = false, EmitDefaultValue = false, Order = 4)]
        public string StoredProceduresSuffix { get; set; }

        /// <summary>
        /// Specify a prefix for naming stored procedure. Default is empty string
        /// </summary>
        [DataMember(Name = "tf", IsRequired = false, EmitDefaultValue = false, Order = 5)]
        public string TriggersPrefix { get; set; }

        /// <summary>
        /// Specify a suffix for naming stored procedures. Default is empty string
        /// </summary>
        [DataMember(Name = "ts", IsRequired = false, EmitDefaultValue = false, Order = 6)]
        public string TriggersSuffix { get; set; }

        /// <summary>
        /// Specify a prefix for naming tracking tables. Default is empty string
        /// </summary>
        [DataMember(Name = "ttp", IsRequired = false, EmitDefaultValue = false, Order = 7)]
        public string TrackingTablesPrefix { get; set; }

        /// <summary>
        /// Specify a suffix for naming tracking tables.
        /// </summary>
        [DataMember(Name = "tts", IsRequired = false, EmitDefaultValue = false, Order = 8)]
        public string TrackingTablesSuffix { get; set; }

        ///// <summary>
        ///// Gets or Sets the current Setup version.
        ///// </summary>
        //[DataMember(Name = "v", IsRequired = false, EmitDefaultValue = false, Order = 9)]
        //public string Version { get; set; }

        /// <summary>
        /// Create a list of tables to be added to the sync process
        /// </summary>
        public SyncSetup(IEnumerable<string> tables) : this() => this.Tables.AddRange(tables);

        /// <summary>
        /// ctor
        /// </summary>
        public SyncSetup()
        {
            this.Tables = new SetupTables();
            this.Filters = new SetupFilters();
            //this.Version = SyncVersion.Current.ToString();
        }

        /// <summary>
        /// Check if Setup has tables
        /// </summary>
        public bool HasTables => this.Tables?.Count > 0;

        /// <summary>
        /// Check if Setup has at least one table with columns
        /// </summary>
        public bool HasColumns => this.Tables?.SelectMany(t => t.Columns).Count() > 0;  // using SelectMany to get columns and not Collection<Column>


        /// <summary>
        /// Check if two setups have the same local options
        /// </summary>
        public bool HasSameOptions(SyncSetup otherSetup)
        {
            if (otherSetup == null)
                return false;

            var sc = SyncGlobalization.DataSourceStringComparison;

            if (!string.Equals(this.StoredProceduresPrefix, otherSetup.StoredProceduresPrefix, sc) ||
                !string.Equals(this.StoredProceduresSuffix, otherSetup.StoredProceduresSuffix, sc) ||
                !string.Equals(this.TrackingTablesPrefix, otherSetup.TrackingTablesPrefix, sc) ||
                !string.Equals(this.TrackingTablesSuffix, otherSetup.TrackingTablesSuffix, sc) ||
                !string.Equals(this.TriggersPrefix, otherSetup.TriggersPrefix, sc) ||
                !string.Equals(this.TriggersSuffix, otherSetup.TriggersSuffix, sc)) 
                return false;

            return true;
        }

        /// <summary>
        /// Check if two setups have the same tables / filters structure
        /// </summary>
        public bool HasSameStructure(SyncSetup otherSetup)
        {
            if (otherSetup == null)
                return false;

            // Checking inner lists
            if (!this.Tables.CompareWith(otherSetup.Tables))
                return false;

            if (!this.Filters.CompareWith(otherSetup.Filters))
                return false;

            return true;
        }

        public bool EqualsByProperties(SyncSetup otherSetup)
        {
            if (otherSetup == null)
                return false;

            if (!HasSameOptions(otherSetup))
                return false;

            if (!HasSameStructure(otherSetup))
                return false;

            return true;
        }

        public override string ToString() => $"{this.Tables.Count} tables";

        /// <summary>
        /// Gets a true boolean if other instance is defined as same based on all properties
        /// </summary>
        public bool Equals(SyncSetup other) => this.EqualsByProperties(other);

        /// <summary>
        /// Gets a true boolean if other instance is defined as same based on all properties
        /// </summary>
        public override bool Equals(object obj) => this.EqualsByProperties(obj as SyncSetup);

        public override int GetHashCode() => base.GetHashCode();
    }
}
