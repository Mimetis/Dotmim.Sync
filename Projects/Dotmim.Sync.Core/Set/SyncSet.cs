using Dotmim.Sync.Builders;


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
    public class SyncSet : IDisposable, IEquatable<SyncSet>
    {
        /// <summary>
        /// Gets or Sets the sync set tables
        /// </summary>
        [DataMember(Name = "t", IsRequired = false, EmitDefaultValue = false, Order = 8)]
        public SyncTables Tables { get; set; }

        /// <summary>
        /// Gets or Sets an array of every SchemaRelation belong to this Schema
        /// </summary>
        [DataMember(Name = "r", IsRequired = false, EmitDefaultValue = false, Order = 9)]
        public SyncRelations Relations { get; set; }

        /// <summary>
        /// Filters applied on tables
        /// </summary>
        [DataMember(Name = "f", IsRequired = false, EmitDefaultValue = false, Order = 10)]
        public SyncFilters Filters { get; set; }

        /// <summary>
        /// Create a new SyncSet, empty
        /// </summary>
        public SyncSet()
        {
            this.Tables = new SyncTables(this);
            this.Relations = new SyncRelations(this);
            this.Filters = new SyncFilters(this);
        }


        /// <summary>
        /// Creates a new SyncSet based on a Sync setup (containing tables)
        /// </summary>
        /// <param name="setup"></param>
        public SyncSet(SyncSetup setup) : this()
        {
            // Create the schema
            var schema = new SyncSet();

            foreach (var filter in setup.Filters)
                schema.Filters.Add(filter);

            foreach (var setupTable in setup.Tables)
                this.Tables.Add(new SyncTable(setupTable.TableName, setupTable.SchemaName));
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
        public void ImportContainerSet(ContainerSet containerSet, bool checkType)
        {
            foreach (var table in containerSet.Tables)
            {
                var syncTable = this.Tables[table.TableName, table.SchemaName];

                if (syncTable == null)
                    throw new ArgumentNullException($"Table {table.TableName} does not exist in the SyncSet");

                syncTable.Rows.ImportContainerTable(table, checkType);
            }

        }

        /// <summary>
        /// Get the rows inside a container.
        /// ContainerSet is a serialization container for rows
        /// </summary>
        public ContainerSet GetContainerSet()
        {
            var containerSet = new ContainerSet();
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

        public bool Equals(SyncSet otherSet)
        {
            if (otherSet == null)
                return false;

            if (this.Tables.Count != otherSet.Tables.Count)
                return false;

            //if (this.StoredProceduresPrefix != otherSet.StoredProceduresPrefix ||
            //    this.StoredProceduresSuffix != otherSet.StoredProceduresSuffix ||
            //    this.TrackingTablesPrefix != otherSet.TrackingTablesPrefix ||
            //    this.TrackingTablesSuffix != otherSet.TrackingTablesSuffix ||
            //    this.TriggersPrefix != otherSet.TriggersPrefix ||
            //    this.TriggersSuffix != otherSet.TriggersSuffix)
            //    return false;

            if (this.Relations != null && otherSet.Relations == null || this.Relations == null && otherSet.Filters != null)
                return false;

            if (this.Relations != null && otherSet.Relations != null)
            {
                // we may have the exact same count
                if (this.Relations.Count != otherSet.Relations.Count)
                    return false;

                // Compare relations
                foreach (var currentRelation in this.Relations)
                {
                    var otherRelation = otherSet.Relations.FirstOrDefault(f => f == currentRelation);

                    if (otherRelation == null)
                        return false;

                    if (currentRelation.Keys.Count != otherRelation.Keys.Count)
                        return false;

                    if (!currentRelation.Keys.All(ck => otherRelation.Keys.Any(ok => ok == ck)))
                        return false;
                }

            }

            if (this.Filters != null && otherSet.Filters == null || this.Filters == null && otherSet.Filters != null)
                return false;

            if (this.Filters != null && otherSet.Filters != null)
            {
                if (this.Filters.Count != otherSet.Filters.Count)
                    return false;

                // Compare filters
                foreach (var currentFilter in this.Filters)
                {
                    var otherFilter = otherSet.Filters.FirstOrDefault(f => f == currentFilter);

                    if (otherFilter == null)
                        return false;

                    // Parameters
                    if (currentFilter.Parameters.Count != otherFilter.Parameters.Count)
                        return false;

                    foreach (var currentParameter in currentFilter.Parameters)
                    {
                        var otherParameter = otherFilter.Parameters.FirstOrDefault(op => op == currentParameter);

                        if (otherParameter == null)
                            return false;

                        // check additionals properties that are not check in the base.Equals() method
                        if (otherParameter.AllowNull != currentParameter.AllowNull ||
                            otherParameter.DbType != currentParameter.DbType ||
                            otherParameter.DefaultValue != currentParameter.DefaultValue ||
                            otherParameter.MaxLength != currentParameter.MaxLength)
                            return false;
                    }


                    // Custom Wheres
                    if (currentFilter.CustomWheres.Count != otherFilter.CustomWheres.Count)
                        return false;

                    // Compare all custom wheres and check they are equals
                    if (!currentFilter.CustomWheres.All(cw => otherFilter.CustomWheres.Any(ow => ow.Equals(cw, SyncGlobalization.DataSourceStringComparison))))
                        return false;

                    // Wheres
                    if (currentFilter.Wheres.Count != otherFilter.Wheres.Count)
                        return false;

                    // Compare all wheres and check they are equals
                    if (!currentFilter.Wheres.All(cw => otherFilter.Wheres.Any(ow => cw == ow)))
                        return false;

                    // Joins
                    if (currentFilter.Joins.Count != otherFilter.Joins.Count)
                        return false;

                    // Compare all Joins and check they are equals
                    if (!currentFilter.Joins.All(cw => otherFilter.Joins.Any(ow => cw == ow)))
                        return false;
                }
            }


            foreach(var currentTable in this.Tables)
            {
                var otherTable = otherSet.Tables.FirstOrDefault(t => t == currentTable);

                if (otherTable == null)
                    return false;

                // check additionals properties that are not check in the base.Equals() method

                if (currentTable.Columns != null && otherTable.Columns == null || currentTable.Columns == null && otherTable.Columns != null)
                    return false;

                if (currentTable.Columns.Count != otherTable.Columns.Count)
                    return false;

                // we just check column name, should we check ALL properties as well ?
                if (!currentTable.Columns.All(cc => otherTable.Columns.Any(oc => oc == cc)))
                    return false;

            }

            return true;
        }

        public override bool Equals(object obj) => this.Equals(obj as SyncSet);

        public override int GetHashCode() => base.GetHashCode();

        public override string ToString() => $"{this.Tables.Count} tables";

        public static bool operator ==(SyncSet left, SyncSet right) => EqualityComparer<SyncSet>.Default.Equals(left, right);

        public static bool operator !=(SyncSet left, SyncSet right) => !(left == right);

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
