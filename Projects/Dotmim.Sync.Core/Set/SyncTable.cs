using Dotmim.Sync.Builders;

using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Serialization;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
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
    public class SyncTable : SyncNamedItem<SyncTable>, IDisposable
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
        /// Gets or Sets the table columns
        /// </summary>
        [DataMember(Name = "c", IsRequired = false, EmitDefaultValue = false, Order = 5)]
        public SyncColumns Columns { get; set; }

        /// <summary>
        /// Gets or Sets the table primary keys
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
        /// Create a new sync table with the given name
        /// </summary>
        public SyncTable(string tableName) : this(tableName, string.Empty) { }

        /// <summary>
        /// Create a new sync table with the given name
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
        public void Clear() => this.Dispose(true);


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
                if (this.Rows != null)
                    this.Rows.Clear();

                if (this.Columns != null)
                    this.Columns.Clear();

                this.Schema = null;
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

        /// <summary>
        /// Create a new row
        /// </summary>
        public SyncRow NewRow(DataRowState state = DataRowState.Unchanged)
        {
            var row = new SyncRow(this.Columns.Count)
            {
                RowState = state,
                Table = this
            };

            return row;
        }

        public IEnumerable<SyncRelation> GetRelations()
        {
            if (this.Schema == null)
                return Enumerable.Empty<SyncRelation>();

            return this.Schema.Relations.Where(r => r.GetTable().EqualsByName(this));
        }


        /// <summary>
        /// Gets the full name of the table, based on schema name + "." + table name (if schema name exists)
        /// </summary>
        /// <returns></returns>
        public string GetFullName()
            => string.IsNullOrEmpty(this.SchemaName) ? this.TableName : $"{this.SchemaName}.{this.TableName}";


        /// <summary>
        /// Get all columns that can be updated
        /// </summary>
        public IEnumerable<SyncColumn> GetMutableColumns(bool includeAutoIncrement = true, bool includePrimaryKeys = false)
        {
            foreach (var column in this.Columns.OrderBy(c => c.Ordinal))
            {
                if (!column.IsCompute && !column.IsReadOnly)
                {
                    var isPrimaryKey = this.PrimaryKeys.Any(pkey => column.ColumnName.Equals(pkey, SyncGlobalization.DataSourceStringComparison));

                    if (includePrimaryKeys && isPrimaryKey)
                        yield return column;
                    else if (!isPrimaryKey && (includeAutoIncrement || (!includeAutoIncrement && !column.IsAutoIncrement)))
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
                var isPrimaryKey = this.PrimaryKeys.Any(pkey => column.ColumnName.Equals(pkey, SyncGlobalization.DataSourceStringComparison));

                if (isPrimaryKey)
                    yield return column;
            }
        }

        /// <summary>
        /// Get all filters for a selected sync table
        /// </summary>
        public SyncFilter GetFilter()
        {
            if (this.Schema.Filters == null || this.Schema.Filters.Count <= 0)
                return null;

            return this.Schema.Filters.FirstOrDefault(sf =>
            {
                var sc = SyncGlobalization.DataSourceStringComparison;

                var sn = sf.SchemaName == null ? string.Empty : sf.SchemaName;
                var otherSn = this.SchemaName == null ? string.Empty : this.SchemaName;

                return this.TableName.Equals(sf.TableName, sc) && sn.Equals(otherSn, sc);
            });
        }

        public void Load(DbDataReader reader)
        {
            var readerFieldCount = reader.FieldCount;

            if (readerFieldCount == 0 || !reader.HasRows)
                return;

            if (this.Columns.Count == 0)
            {
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    var columnName = reader.GetName(i);
                    var columnType = reader.GetFieldType(i);
                    this.Columns.Add(columnName, columnType);
                }
            }

            while (reader.Read())
            {
                var row = this.NewRow();

                for (var i = 0; i < reader.FieldCount; i++)
                {
                    var columnName = reader.GetName(i);
                    var columnValueObject = reader.GetValue(i);
                    //var columnValue = columnValueObject == DBNull.Value ? null : columnValueObject;

                    row[columnName] = columnValueObject;
                }

                this.Rows.Add(row);
            }
        }

        /// <summary>
        /// Check if a column name is a primary key
        /// </summary>
        public bool IsPrimaryKey(string columnName) => this.PrimaryKeys.Any(pkey => columnName.Equals(pkey, SyncGlobalization.DataSourceStringComparison));

        /// <summary>
        /// Gets a value returning if the SchemaTable contains an auto increment column
        /// </summary>
        public bool HasAutoIncrementColumns => this.Columns.Any(c => c.IsAutoIncrement);

        /// <summary>
        /// Gets a value indicating if the synctable has rows
        /// </summary>
        public bool HasRows => this.Rows != null && this.Rows.Count > 0;

        public override string ToString()
        {
            if (!string.IsNullOrEmpty(this.SchemaName))
                return $"{this.SchemaName}.{this.TableName}";
            else
                return this.TableName;
        }

        /// <summary>
        /// Get all comparable fields to determine if two instances are identifed as same by name
        /// </summary>
        public override IEnumerable<string> GetAllNamesProperties()
        {
            yield return this.TableName;
            yield return this.SchemaName;
        }

        public override bool EqualsByProperties(SyncTable other)
        {
            if (other == null)
                return false;

            var sc = SyncGlobalization.DataSourceStringComparison;

            if (!this.EqualsByName(other))
                return false;

            // checking properties
            if (this.SyncDirection != other.SyncDirection)
                return false;

            if (!string.Equals(this.OriginalProvider, other.OriginalProvider, sc))
                return false;

            // Check list
            if (!this.Columns.CompareWith(other.Columns))
                return false;

            if (!this.PrimaryKeys.CompareWith(other.PrimaryKeys))
                return false;


            return true;

        }
    }
}
