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
    public class SyncRelations : ICollection<SyncRelation>, IList<SyncRelation>
    {
        /// <summary>
        /// Exposing the InnerCollection for serialization purpose
        /// </summary>
        [DataMember(Name = "c", IsRequired = true)]
        public Collection<SyncRelation> InnerCollection { get; set; } = new Collection<SyncRelation>();

        /// <summary>
        /// Relation's schema
        /// </summary>
        [IgnoreDataMember]
        public SyncSet Schema { get; internal set; }

        /// <summary>
        /// Create a default collection for Serializers
        /// </summary>
        public SyncRelations()
        {
        }

        /// <summary>
        /// Create a new collection of tables for a SyncSchema
        /// </summary>
        public SyncRelations(SyncSet schema) => this.Schema = schema;

        /// <summary>
        /// Since we don't serializer the reference to the schema, this method will reaffect the correct schema
        /// </summary>
        public void EnsureRelations(SyncSet schema)
        {
            this.Schema = schema;

            if (InnerCollection != null)
                foreach (var item in this)
                    item.EnsureRelation(schema);
        }

        /// <summary>
        /// Add a new table to the Schema table collection
        /// </summary>
        public void Add(SyncRelation item)
        {
            item.Schema = Schema;
            InnerCollection.Add(item);
        }

        /// <summary>
        /// Clear all the relations
        /// </summary>
        public void Clear()
        {
            foreach (var item in InnerCollection)
                item.Clear();

            InnerCollection.Clear();
        }

        public SyncRelation this[int index] => InnerCollection[index];
        public int Count => InnerCollection.Count;
        public bool IsReadOnly => false;
        public bool Remove(SyncRelation item) => InnerCollection.Remove(item);
        public bool Contains(SyncRelation item) => InnerCollection.Contains(item);
        public void CopyTo(SyncRelation[] array, int arrayIndex) => InnerCollection.CopyTo(array, arrayIndex);
        public int IndexOf(SyncRelation item) => InnerCollection.IndexOf(item);
        public void RemoveAt(int index) => InnerCollection.RemoveAt(index);
        IEnumerator IEnumerable.GetEnumerator() => InnerCollection.GetEnumerator();
        public IEnumerator<SyncRelation> GetEnumerator() => InnerCollection.GetEnumerator();
        SyncRelation IList<SyncRelation>.this[int index] { get => InnerCollection[index]; set => InnerCollection[index] = value; }
        public void Insert(int index, SyncRelation item) => InnerCollection.Insert(index, item);
        public override string ToString() => this.InnerCollection.Count.ToString();

    }

}
