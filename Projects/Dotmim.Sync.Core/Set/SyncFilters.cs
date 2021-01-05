using Dotmim.Sync.Builders;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;

namespace Dotmim.Sync
{
    [CollectionDataContract(Name = "filters", ItemName = "filt"), Serializable]
    public class SyncFilters : ICollection<SyncFilter>, IList<SyncFilter>
    {
        /// <summary>
        /// Exposing the InnerCollection for serialization purpose
        /// </summary>
        [DataMember(Name = "c", IsRequired = true, Order = 1)]
        public Collection<SyncFilter> InnerCollection { get; set; } = new Collection<SyncFilter>();

        /// <summary>
        /// Filter's schema
        /// </summary>
        [IgnoreDataMember]
        public SyncSet Schema { get; internal set; }

        /// <summary>
        /// Create a default collection for Serializers
        /// </summary>
        public SyncFilters()
        {
        }

        /// <summary>
        /// Create a new collection of tables for a SyncSchema
        /// </summary>
        public SyncFilters(SyncSet schema) => this.Schema = schema;

        /// <summary>
        /// Since we don't serializer the reference to the schema, this method will reaffect the correct schema
        /// </summary>
        public void EnsureFilters(SyncSet schema)
        {
            this.Schema = schema;

            if (InnerCollection != null)
                foreach (var item in this)
                    item.EnsureFilter(schema);
        }

        /// <summary>
        /// Add a new filter 
        /// </summary>
        public void Add(SyncFilter item)
        {
            item.Schema = Schema;
            InnerCollection.Add(item);
        }

        /// <summary>
        /// Add a new filter 
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
                item.Joins.Add(new SyncFilterJoin
                {
                    TableName = s.TableName,
                    JoinEnum = s.JoinEnum,
                    LeftTableName = s.LeftTableName,
                    LeftColumnName = s.LeftColumnName,
                    RightTableName = s.RightTableName,
                    RightColumnName = s.RightColumnName,
                });

            foreach (var s in setupFilter.CustomWheres)
                item.CustomWheres.Add(s);

            InnerCollection.Add(item);
        }

        /// <summary>
        /// Add a new filter 
        /// </summary>
        public void Add(string tableName, string columnName, string schemaName = null)
        {
            var item = new SyncFilter(tableName, schemaName)
            {
                Schema = Schema
            };

            // Add a column as parameter. This column will be automaticaly added in the tracking table
            item.Parameters.Add(new SyncFilterParameter { Name = columnName, TableName = tableName, SchemaName = schemaName, AllowNull = true });

            // add the side where expression, allowing to be null
            item.Wheres.Add(new SyncFilterWhereSideItem { ColumnName = columnName, TableName = tableName, SchemaName = schemaName });

            InnerCollection.Add(item);
        }

        /// <summary>
        /// Clear
        /// </summary>
        public void Clear()
        {
            foreach (var item in InnerCollection)
                item.Clear();

            InnerCollection.Clear();
        }

        public SyncFilter this[int index] => InnerCollection[index];
        public int Count => InnerCollection.Count;
        public bool IsReadOnly => false;
        SyncFilter IList<SyncFilter>.this[int index] { get => InnerCollection[index]; set => InnerCollection[index] = value; }
        public void Insert(int index, SyncFilter item) => InnerCollection.Insert(index, item);
        public bool Remove(SyncFilter item) => InnerCollection.Remove(item);
        public bool Contains(SyncFilter item) => InnerCollection.Contains(item);
        public void CopyTo(SyncFilter[] array, int arrayIndex) => InnerCollection.CopyTo(array, arrayIndex);
        public int IndexOf(SyncFilter item) => InnerCollection.IndexOf(item);
        public void RemoveAt(int index) => InnerCollection.RemoveAt(index);
        IEnumerator IEnumerable.GetEnumerator() => InnerCollection.GetEnumerator();
        public IEnumerator<SyncFilter> GetEnumerator() => InnerCollection.GetEnumerator();
        public override string ToString() => this.InnerCollection.Count.ToString();
    }

}
