using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;
using System.Runtime.Serialization;

namespace Dotmim.Sync
{
    /// <summary>
    /// Sync filters collection.
    /// </summary>
    [CollectionDataContract(Name = "filters", ItemName = "filt"), Serializable]
    public class SyncFilters : ICollection<SyncFilter>, IList<SyncFilter>
    {
        /// <summary>
        /// Gets or sets exposing the InnerCollection for serialization purpose.
        /// </summary>
        [DataMember(Name = "c", IsRequired = true, Order = 1)]
        public Collection<SyncFilter> InnerCollection { get; set; } = [];

        /// <summary>
        /// Gets filter's schema.
        /// </summary>
        [IgnoreDataMember]
        public SyncSet Schema { get; internal set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="SyncFilters"/> class.
        /// Create a default collection for SerializersFactory.
        /// </summary>
        public SyncFilters()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SyncFilters"/> class.
        /// Create a new collection of tables for a SyncSchema.
        /// </summary>
        public SyncFilters(SyncSet schema) => this.Schema = schema;

        /// <summary>
        /// Since we don't serializer the reference to the schema, this method will reaffect the correct schema.
        /// </summary>
        public void EnsureFilters(SyncSet schema)
        {
            this.Schema = schema;

            if (this.InnerCollection != null)
            {
                foreach (var item in this)
                    item.EnsureFilter(schema);
            }
        }

        /// <summary>
        /// Add a new filter.
        /// </summary>
        public void Add(SyncFilter item)
        {
            item.Schema = this.Schema;
            this.InnerCollection.Add(item);
        }

        /// <summary>
        /// Add a new filter.
        /// </summary>
        public void Add(SetupFilter setupFilter)
        {
            var item = new SyncFilter(setupFilter.TableName, setupFilter.SchemaName)
            {
                Schema = this.Schema,
            };

            foreach (var s in setupFilter.Parameters)
                item.Parameters.Add(new SyncFilterParameter { Name = s.Name, SchemaName = s.SchemaName, TableName = s.TableName, DbType = s.DbType, DefaultValue = s.DefaultValue, AllowNull = s.AllowNull, MaxLength = s.MaxLength });

            foreach (var s in setupFilter.Wheres)
                item.Wheres.Add(new SyncFilterWhereSideItem { ColumnName = s.ColumnName, TableName = s.TableName, SchemaName = s.SchemaName, ParameterName = s.ParameterName });

            foreach (var s in setupFilter.Joins)
            {
                item.Joins.Add(new SyncFilterJoin
                {
                    TableName = s.TableName,
                    JoinEnum = s.JoinEnum,
                    LeftTableName = s.LeftTableName,
                    LeftColumnName = s.LeftColumnName,
                    RightTableName = s.RightTableName,
                    RightColumnName = s.RightColumnName,
                    TableSchemaName = s.TableSchemaName,
                    LeftTableSchemaName = s.LeftTableSchemaName,
                    RightTableSchemaName = s.RightTableSchemaName,
                });
            }

            foreach (var s in setupFilter.CustomWheres)
                item.CustomWheres.Add(s);

            this.InnerCollection.Add(item);
        }

        /// <summary>
        /// Add a new filter.
        /// </summary>
        public void Add(string tableName, string columnName, string schemaName = null)
        {
            var item = new SyncFilter(tableName, schemaName)
            {
                Schema = this.Schema,
            };

            // Add a column as parameter. This column will be automaticaly added in the tracking table
            item.Parameters.Add(new SyncFilterParameter { Name = columnName, TableName = tableName, SchemaName = schemaName, AllowNull = true });

            // add the side where expression, allowing to be null
            item.Wheres.Add(new SyncFilterWhereSideItem { ColumnName = columnName, TableName = tableName, SchemaName = schemaName });

            this.InnerCollection.Add(item);
        }

        /// <summary>
        /// Clear.
        /// </summary>
        public void Clear()
        {
            foreach (var item in this.InnerCollection)
                item.Clear();

            this.InnerCollection.Clear();
        }

        /// <summary>
        /// Gets the count of filters.
        /// </summary>
        public int Count => this.InnerCollection.Count;

        /// <summary>
        /// Gets a value indicating whether the collection is read-only.
        /// </summary>
        public bool IsReadOnly => false;

        /// <summary>
        /// Gets or sets the filter at the specified index.
        /// </summary>
        public SyncFilter this[int index] { get => this.InnerCollection[index]; set => this.InnerCollection[index] = value; }

        /// <summary>
        /// Insert a filter at the specified index.
        /// </summary>
        public void Insert(int index, SyncFilter item) => this.InnerCollection.Insert(index, item);

        /// <summary>
        /// Remove a filter.
        /// </summary>
        public bool Remove(SyncFilter item) => this.InnerCollection.Remove(item);

        /// <summary>
        /// Check if the collection contains a filter.
        /// </summary>
        public bool Contains(SyncFilter item) => this.InnerCollection.Contains(item);

        /// <summary>
        /// Copy the collection to an array.
        /// </summary>
        public void CopyTo(SyncFilter[] array, int arrayIndex) => this.InnerCollection.CopyTo(array, arrayIndex);

        /// <summary>
        /// Get the index of a filter.
        /// </summary>
        public int IndexOf(SyncFilter item) => this.InnerCollection.IndexOf(item);

        /// <summary>
        /// Remove a filter at the specified index.
        /// </summary>
        public void RemoveAt(int index) => this.InnerCollection.RemoveAt(index);

        /// <summary>
        /// Gets the enumerator.
        /// </summary>
        IEnumerator IEnumerable.GetEnumerator() => this.InnerCollection.GetEnumerator();

        /// <summary>
        /// Gets the enumerator.
        /// </summary>
        public IEnumerator<SyncFilter> GetEnumerator() => this.InnerCollection.GetEnumerator();

        /// <summary>
        /// Returns a string that represents the current object.
        /// </summary>
        public override string ToString() => this.InnerCollection.Count.ToString(CultureInfo.InvariantCulture);
    }
}