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
    [CollectionDataContract(Name = "filtersparameters", ItemName = "param"), Serializable]
    public class SyncFilterParameters : ICollection<SyncFilterParameter>, IList<SyncFilterParameter>
    {
        /// <summary>
        /// Exposing the InnerCollection for serialization purpose
        /// </summary>
        [DataMember(Name = "c", IsRequired = true, Order = 1)]
        public Collection<SyncFilterParameter> InnerCollection { get; set; } = new Collection<SyncFilterParameter>();

        /// <summary>
        /// Filter's schema
        /// </summary>
        [IgnoreDataMember]
        public SyncSet Schema { get; internal set; }

        /// <summary>
        /// Create a default collection for Serializers
        /// </summary>
        public SyncFilterParameters()
        {
        }

        /// <summary>
        /// Create a new collection of tables for a SyncSchema
        /// </summary>
        public SyncFilterParameters(SyncSet schema) => this.Schema = schema;

        /// <summary>
        /// Since we don't serializer the reference to the schema, this method will reaffect the correct schema
        /// </summary>
        public void EnsureFilters(SyncSet schema)
        {
            this.Schema = schema;

            foreach (var item in this)
                item.EnsureFilterParameter(schema);
        }

        /// <summary>
        /// Add a new filter 
        /// </summary>
        public void Add(SyncFilterParameter item)
        {
            item.Schema = Schema;
            InnerCollection.Add(item);
        }


        /// <summary>
        /// Get a table by its name
        /// </summary>
        public SyncFilterParameter this[string name]
        {
            get
            {
                if (string.IsNullOrEmpty(name))
                    throw new ArgumentNullException("name");

                var parser = ParserName.Parse(name);
                var objectName = parser.ObjectName;

                // Create a tmp synctable to benefit the SyncTable.Equals() method
                return InnerCollection.FirstOrDefault(st => string.Equals(st.Name, name, SyncGlobalization.DataSourceStringComparison));
            }
        }


        /// <summary>
        /// Clear
        /// </summary>
        public void Clear()
        {
            InnerCollection.Clear();
        }

        public SyncFilterParameter this[int index] => InnerCollection[index];
        public int Count => InnerCollection.Count;
        public bool IsReadOnly => false;
        SyncFilterParameter IList<SyncFilterParameter>.this[int index] { get => InnerCollection[index]; set => InnerCollection[index] = value; }
        public void Insert(int index, SyncFilterParameter item) => InnerCollection.Insert(index, item);
        public bool Remove(SyncFilterParameter item) => InnerCollection.Remove(item);
        public bool Contains(SyncFilterParameter item) => InnerCollection.Contains(item);
        public void CopyTo(SyncFilterParameter[] array, int arrayIndex) => InnerCollection.CopyTo(array, arrayIndex);
        public int IndexOf(SyncFilterParameter item) => InnerCollection.IndexOf(item);
        public void RemoveAt(int index) => InnerCollection.RemoveAt(index);
        IEnumerator IEnumerable.GetEnumerator() => InnerCollection.GetEnumerator();
        public IEnumerator<SyncFilterParameter> GetEnumerator() => InnerCollection.GetEnumerator();
        public override string ToString() => this.InnerCollection.Count.ToString();
    }

}
