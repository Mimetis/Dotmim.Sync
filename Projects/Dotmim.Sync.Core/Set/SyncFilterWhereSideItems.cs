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
    [CollectionDataContract(Name = "filterswhereside", ItemName = "whereside"), Serializable]
    public class SyncFilterWhereSideItems : ICollection<SyncFilterWhereSideItem>, IList<SyncFilterWhereSideItem>
    {
        /// <summary>
        /// Exposing the InnerCollection for serialization purpose
        /// </summary>
        [DataMember(Name = "c", IsRequired = true, Order = 1)]
        public Collection<SyncFilterWhereSideItem> InnerCollection { get; set; } = new Collection<SyncFilterWhereSideItem>();

        /// <summary>
        /// Filter's schema
        /// </summary>
        [IgnoreDataMember]
        public SyncSet Schema { get; internal set; }

        /// <summary>
        /// Create a default collection for SerializersFactory
        /// </summary>
        public SyncFilterWhereSideItems()
        {
        }

        /// <summary>
        /// Create a new collection of tables for a SyncSchema
        /// </summary>
        public SyncFilterWhereSideItems(SyncSet schema) => this.Schema = schema;

        /// <summary>
        /// Since we don't serializer the reference to the schema, this method will reaffect the correct schema
        /// </summary>
        public void EnsureFilters(SyncSet schema)
        {
            this.Schema = schema;

            foreach (var item in this)
                item.EnsureFilterWhereSideItem(schema);
        }

        /// <summary>
        /// Add a new filter 
        /// </summary>
        public void Add(SyncFilterWhereSideItem item)
        {
            item.Schema = Schema;
            InnerCollection.Add(item);
        }


        /// <summary>
        /// Clear
        /// </summary>
        public void Clear() => InnerCollection.Clear();

        public SyncFilterWhereSideItem this[int index] => InnerCollection[index];
        public int Count => InnerCollection.Count;
        public bool IsReadOnly => false;
        SyncFilterWhereSideItem IList<SyncFilterWhereSideItem>.this[int index] { get => InnerCollection[index]; set => InnerCollection[index] = value; }
        public void Insert(int index, SyncFilterWhereSideItem item) => InnerCollection.Insert(index, item);
        public bool Remove(SyncFilterWhereSideItem item) => InnerCollection.Remove(item);
        public bool Contains(SyncFilterWhereSideItem item) => InnerCollection.Contains(item);
        public void CopyTo(SyncFilterWhereSideItem[] array, int arrayIndex) => InnerCollection.CopyTo(array, arrayIndex);
        public int IndexOf(SyncFilterWhereSideItem item) => InnerCollection.IndexOf(item);
        public void RemoveAt(int index) => InnerCollection.RemoveAt(index);
        IEnumerator IEnumerable.GetEnumerator() => InnerCollection.GetEnumerator();
        public IEnumerator<SyncFilterWhereSideItem> GetEnumerator() => InnerCollection.GetEnumerator();
        public override string ToString() => this.InnerCollection.Count.ToString();
    }

}
