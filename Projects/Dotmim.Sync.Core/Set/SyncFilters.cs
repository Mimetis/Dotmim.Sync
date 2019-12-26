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
        [DataMember(Name = "c", IsRequired = true)]
        public Collection<SyncFilter> InnerCollection { get; set; } = new Collection<SyncFilter>();

        /// <summary>
        /// Filter's schema
        /// </summary>
        [IgnoreDataMember]
        public SyncSet Schema { get; private set; }

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
            foreach (var item in this)
                item.EnsureFilter(schema);
        }

        /// <summary>
        /// Get all filters not marked as Virtual
        /// </summary>
        /// <returns></returns>
        public IEnumerable<SyncFilter> GetColumnFilters()
        {
            return this.Where(f => !f.IsVirtual);
        }

        /// <summary>
        /// Add a new table to the Schema table collection
        /// </summary>
        public void Add(SyncFilter item)
        {
            item.Schema = Schema;
            InnerCollection.Add(item);
        }
        public SyncFilter this[int index] => InnerCollection[index];
        public int Count => InnerCollection.Count;
        public bool IsReadOnly => false;
        SyncFilter IList<SyncFilter>.this[int index] { get => InnerCollection[index]; set => InnerCollection[index] = value; }
        public void Insert(int index, SyncFilter item) => InnerCollection.Insert(index, item);
        public bool Remove(SyncFilter item) => InnerCollection.Remove(item);
        public void Clear() => InnerCollection.Clear();
        public bool Contains(SyncFilter item) => InnerCollection.Contains(item);
        public void CopyTo(SyncFilter[] array, int arrayIndex) => InnerCollection.CopyTo(array, arrayIndex);
        public int IndexOf(SyncFilter item) => InnerCollection.IndexOf(item);
        public void RemoveAt(int index) => InnerCollection.RemoveAt(index);
        IEnumerator IEnumerable.GetEnumerator() => InnerCollection.GetEnumerator();
        public IEnumerator<SyncFilter> GetEnumerator() => InnerCollection.GetEnumerator();
        public override string ToString() => this.InnerCollection.Count.ToString();
    }

}
