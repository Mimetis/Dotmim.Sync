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
    [CollectionDataContract(Name = "filtersjoins", ItemName = "join"), Serializable]
    public class SyncFilterJoins : ICollection<SyncFilterJoin>, IList<SyncFilterJoin>
    {
        /// <summary>
        /// Exposing the InnerCollection for serialization purpose
        /// </summary>
        [DataMember(Name = "c", IsRequired = true)]
        public Collection<SyncFilterJoin> InnerCollection { get; set; } = new Collection<SyncFilterJoin>();

        /// <summary>
        /// Filter's schema
        /// </summary>
        [IgnoreDataMember]
        public SyncSet Schema { get; internal set; }

        /// <summary>
        /// Create a default collection for Serializers
        /// </summary>
        public SyncFilterJoins()
        {
        }

        /// <summary>
        /// Create a new collection of tables for a SyncSchema
        /// </summary>
        public SyncFilterJoins(SyncSet schema) => this.Schema = schema;

        /// <summary>
        /// Since we don't serializer the reference to the schema, this method will reaffect the correct schema
        /// </summary>
        public void EnsureFilters(SyncSet schema)
        {
            this.Schema = schema;

            foreach (var item in this)
                item.EnsureFilterJoin(schema);
        }

        /// <summary>
        /// Add a new filter 
        /// </summary>
        public void Add(SyncFilterJoin item)
        {
            item.Schema = Schema;
            InnerCollection.Add(item);
        }


        ///// <summary>
        ///// Get a table by its name
        ///// </summary>
        //public SyncFilterJoin this[string name]
        //{
        //    get
        //    {
        //        if (string.IsNullOrEmpty(name))
        //            throw new ArgumentNullException("name");

        //        var parser = ParserName.Parse(name);
        //        var objectName = parser.ObjectName;

        //        // Create a tmp synctable to benefit the SyncTable.Equals() method
        //        return InnerCollection.FirstOrDefault(st => string.Equals(st.Name, name, SyncGlobalization.DataSourceStringComparison));
        //    }
        //}


        /// <summary>
        /// Clear
        /// </summary>
        public void Clear()
        {
            InnerCollection.Clear();
        }

        public SyncFilterJoin this[int index] => InnerCollection[index];
        public int Count => InnerCollection.Count;
        public bool IsReadOnly => false;
        SyncFilterJoin IList<SyncFilterJoin>.this[int index] { get => InnerCollection[index]; set => InnerCollection[index] = value; }
        public void Insert(int index, SyncFilterJoin item) => InnerCollection.Insert(index, item);
        public bool Remove(SyncFilterJoin item) => InnerCollection.Remove(item);
        public bool Contains(SyncFilterJoin item) => InnerCollection.Contains(item);
        public void CopyTo(SyncFilterJoin[] array, int arrayIndex) => InnerCollection.CopyTo(array, arrayIndex);
        public int IndexOf(SyncFilterJoin item) => InnerCollection.IndexOf(item);
        public void RemoveAt(int index) => InnerCollection.RemoveAt(index);
        IEnumerator IEnumerable.GetEnumerator() => InnerCollection.GetEnumerator();
        public IEnumerator<SyncFilterJoin> GetEnumerator() => InnerCollection.GetEnumerator();
        public override string ToString() => this.InnerCollection.Count.ToString();
    }

}
