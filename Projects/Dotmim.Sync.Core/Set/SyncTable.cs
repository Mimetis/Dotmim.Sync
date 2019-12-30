using Dotmim.Sync.Builders;
using Dotmim.Sync.Data;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Serialization;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;

namespace Dotmim.Sync
{
    /// <summary>
    /// Represents a table schema
    /// </summary>
    [DataContract(Name = "st"), Serializable]
    public class SyncTable : IDisposable
    {
        [NonSerialized]
        private SyncRows rows;

        /// <summary>
        /// Gets or sets the name of the table that the DmTableSurrogate object represents.
        /// </summary>
        [DataMember(Name = "n", IsRequired = true, Order = 1)]
        public string TableName { get; set; }

        /// <summary>
        /// Get or Set the schema used for the DmTableSurrogate
        /// </summary>
        [DataMember(Name = "s", IsRequired = false, EmitDefaultValue = false, Order = 2)]
        public string SchemaName { get; set; }

        /// <summary>
        /// Gets or Sets the original provider (SqlServer, MySql, Sqlite, Oracle, PostgreSQL)
        /// </summary>
        [DataMember(Name = "op", IsRequired = false, EmitDefaultValue = false, Order = 3)]
        public string OriginalProvider { get; set; }

        /// <summary>
        /// Gets or Sets the Sync direction (may be Bidirectional, DownloadOnly, UploadOnly) 
        /// Default is Bidirectional
        /// </summary>
        [DataMember(Name = "sd", IsRequired = false, EmitDefaultValue = false, Order = 4)]
        public SyncDirection SyncDirection { get; set; }

        /// <summary>
        /// Gets an array of DmColumnSurrogate objects that comprise the table that is represented by the DmTableSurrogate object.
        /// </summary>
        [DataMember(Name = "c", IsRequired = false, EmitDefaultValue = false, Order = 5)]
        public SyncColumns Columns { get; set; }

        /// <summary>
        /// Gets an array of DmColumnSurrogate objects that represent the PrimaryKeys.
        /// </summary>
        [DataMember(Name = "pk", IsRequired = false, EmitDefaultValue = false, Order = 6)]
        public Collection<string> PrimaryKeys { get; set; } = new Collection<string>();


        /// <summary>
        /// Gets the ShemaTable's rows
        /// </summary>
        [IgnoreDataMember]
        public SyncRows Rows
        {
            // Use of field property because of attribute [NonSerialized] necessary for binaryformatter
            get => rows;
            private set => rows = value;
        }

        /// <summary>
        /// Gets the ShemaTable's SyncSchema
        /// </summary>
        [IgnoreDataMember]
        public SyncSet Schema { get; set; }

        public SyncTable()
        {
            this.Rows = new SyncRows(this);
            this.Columns = new SyncColumns(this);
        }

        /// <summary>
        /// Create a new schema name with the given name
        /// </summary>
        public SyncTable(string tableName) : this(tableName, string.Empty) { }

        /// <summary>
        /// Create a new schema name with the given name
        /// </summary>
        public SyncTable(string tableName, string schemaName) : this()
        {
            this.TableName = tableName;
            this.SchemaName = schemaName;
        }

        /// <summary>
        /// Ensure table as the correct schema (since the property is not serialized
        /// </summary>
        public void EnsureTable(SyncSet schema)
        {
            this.Schema = schema;

            if (this.Columns != null)
                this.Columns.EnsureColumns(this);

            if (this.Rows != null)
                this.Rows.EnsureRows(this);
        }

        /// <summary>
        /// Clear the Table's rows
        /// </summary>
        public void Clear()
        {
            if (this.Rows == null)
                return;

            foreach (var row in this.Rows)
                row.Table = null;

            this.Rows.Clear();
        }


        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool cleanup)
        {
            // Dispose managed ressources
            if (cleanup)
            {
                if (this.Columns != null)
                {
                    foreach (var column in this.Columns)
                        column.Table = null;

                    this.Columns.Clear();
                    this.Columns.Table = null;
                    this.Columns = null;
                }

                if (this.Rows != null)
                {
                    // delete reference to the current Table
                    this.Rows.Table = null;
                    this.Rows = null;
                }
            }

            // Dispose unmanaged ressources
        }

        /// <summary>
        /// Clone the table structure (without rows)
        /// </summary>
        public SyncTable Clone()
        {
            var clone = new SyncTable();
            clone.OriginalProvider = this.OriginalProvider;
            clone.SchemaName = this.SchemaName;
            clone.SyncDirection = this.SyncDirection;
            clone.TableName = this.TableName;

            foreach (var c in this.Columns)
                clone.Columns.Add(c.Clone());

            foreach (var pkey in this.PrimaryKeys)
                clone.PrimaryKeys.Add(pkey);

            return clone;
        }

        ///// <summary>
        ///// Initializes a new instance of the DmTableSurrogate class.
        ///// </summary>
        //public SyncTable(DmTable dt)
        //{
        //    if (dt == null)
        //        throw new ArgumentNullException("dt", "DmTable");

        //    this.TableName = dt.TableName;
        //    this.SchemaName = dt.Schema;
        //    this.OriginalProvider = dt.OriginalProvider;
        //    this.SyncDirection = dt.SyncDirection;

        //    for (int i = 0; i < dt.Columns.Count; i++)
        //        this.Columns.Add(new SyncColumn(dt.Columns[i]));

        //    // Primary Keys
        //    if (dt.PrimaryKey != null && dt.PrimaryKey.Columns != null && dt.PrimaryKey.Columns.Length > 0)
        //    {
        //        for (int i = 0; i < dt.PrimaryKey.Columns.Length; i++)
        //            this.PrimaryKeys.Add(dt.PrimaryKey.Columns[i].ColumnName);
        //    }

        //}

        /// <summary>
        /// Copies the table schema from a DmTableSurrogate object into a DmTable object.
        /// </summary>
        //public void ReadSchemaIntoDmTable(DmTable dt)
        //{
        //    if (dt == null)
        //        throw new ArgumentNullException("dt", "DmTable");

        //    dt.TableName = this.TableName;
        //    dt.Schema = this.SchemaName;
        //    dt.OriginalProvider = this.OriginalProvider;
        //    dt.SyncDirection = this.SyncDirection;
        //    dt.CaseSensitive = this.Schema == null ? true : this.Schema.CaseSensitive;

        //    var cultureInfo = CultureInfo.CurrentCulture;

        //    if (this.Schema != null && !String.IsNullOrEmpty(this.Schema.CultureInfoName))
        //        cultureInfo = new CultureInfo(this.Schema.CultureInfoName);

        //    dt.Culture = cultureInfo;

        //    var orderedColumns = this.GetMutableColumnsWithPrimaryKeys();

        //    for (int i = 0; i < orderedColumns.Count; i++)
        //    {
        //        var dmColumn = orderedColumns[i].ConvertToDmColumn();
        //        dt.Columns.Add(dmColumn);
        //    }

        //    if (this.PrimaryKeys != null && this.PrimaryKeys.Count > 0)
        //    {
        //        DmColumn[] keyColumns = new DmColumn[this.PrimaryKeys.Count];

        //        for (int i = 0; i < this.PrimaryKeys.Count; i++)
        //        {
        //            string columnName = this.PrimaryKeys[i];
        //            keyColumns[i] = dt.Columns.First(c => dt.IsEqual(c.ColumnName, columnName));
        //        }
        //        var key = new DmKey(keyColumns);

        //        dt.PrimaryKey = key;
        //    }
        //}

        /// <summary>
        /// Convert this surrogate to a DmTable
        /// </summary>
        //public DmTable ConvertToDmTable()
        //{
        //    var dmTable = new DmTable(this.TableName);
        //    this.ReadSchemaIntoDmTable(dmTable);

        //    return dmTable;
        //}


        /// <summary>
        /// Create a new row
        /// </summary>
        public SyncRow NewRow(DataRowState state = DataRowState.Unchanged) => new SyncRow(this, state);



        /// <summary>
        /// Gets the collection of child relations for this SchemaTable.
        /// </summary>
        public IEnumerable<SyncRelation> GetChildRelations()
        {
            if (this.Schema == null)
                return Enumerable.Empty<SyncRelation>();

            var childRelations = this.Schema.Relations.Where(r =>
            {
                if (r.ParentKeys.Count() <= 0)
                    return false;

                var parentTable = r.GetParentTable();

                return parentTable == this;
            });

            return childRelations;
        }

        /// <summary>
        /// Gets the collection of parent relations for this SchemaTable.
        /// </summary>
        public IEnumerable<SyncRelation> GetParentRelations()
        {
            if (this.Schema == null)
                return Enumerable.Empty<SyncRelation>();

            var childRelations = this.Schema.Relations.Where(r =>
            {
                if (r.ChildKeys.Count() <= 0)
                    return false;

                var childTable = r.GetChildTable();

                return childTable == this;
            });

            return childRelations;
        }

        /// <summary>
        /// Get all columns that can be updated
        /// </summary>
        public IEnumerable<SyncColumn> GetMutableColumns(bool includeAutoIncrement = true)
        {
            foreach (var column in this.Columns.OrderBy(c => c.Ordinal))
            {
                var isPrimaryKey = this.PrimaryKeys.Any(pkey => this.Schema.StringEquals(column.ColumnName, pkey));

                if (!isPrimaryKey && !column.IsCompute && !column.IsReadOnly)
                {
                    if (includeAutoIncrement || (!includeAutoIncrement && !column.IsAutoIncrement))
                        yield return column;
                }
            }
        }

        /// <summary>
        /// Get all columns that can be queried
        /// </summary>
        public IEnumerable<SyncColumn> GetMutableColumnsWithPrimaryKeys()
        {
            foreach (var column in this.Columns.OrderBy(c => c.Ordinal))
            {
                if (!column.IsCompute && !column.IsReadOnly)
                    yield return column;
            }
        }

        /// <summary>
        /// Get all columns that are Primary keys, based on the names we have in PrimariKeys property
        /// </summary>
        /// <returns></returns>
        public IEnumerable<SyncColumn> GetPrimaryKeysColumns()
        {
            foreach (var column in this.Columns.OrderBy(c => c.Ordinal))
            {
                var isPrimaryKey = this.PrimaryKeys.Any(pkey => this.Schema.StringEquals(column.ColumnName, pkey));

                if (isPrimaryKey)
                    yield return column;
            }
        }

        /// <summary>
        /// Check if a column name is a primary key
        /// </summary>
        public bool IsPrimaryKey(string columnName)
        {
            return this.PrimaryKeys.Any(pkey => this.Schema.StringEquals(columnName, pkey));
        }

        /// <summary>
        /// Gets a value returning if the SchemaTable contains an auto increment column
        /// </summary>
        public bool HasAutoIncrementColumns => this.Columns.Any(c => c.IsAutoIncrement);

        public override string ToString()
        {
            if (!string.IsNullOrEmpty(this.SchemaName))
                return $"{this.SchemaName}.{this.TableName}";
            else
                return this.TableName;
        }

    }




}
