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
    [CollectionDataContract(Name = "params", ItemName = "param"), Serializable]
    public class SyncParameters : ICollection<SyncParameter>, IList<SyncParameter>
    {
        /// <summary>
        /// Gets or Sets the InnerCollection (Exposed as Public for serialization purpose)
        /// </summary>
        [DataMember(Name = "c", IsRequired = true)]
        public Collection<SyncParameter> InnerCollection { get; set; } = new Collection<SyncParameter>();

        /// <summary>
        /// Create a default collection for Serializers
        /// </summary>
        public SyncParameters()
        {
        }

        /// <summary>
        /// Add a new sync parameter 
        /// </summary>
        public void Add<T>(string name, T value) => this.Add(new SyncParameter(name, value));


        /// <summary>
        /// Add a new sync parameter 
        /// </summary>
        public void Add(SyncParameter item)
        {
            if (this.Any(p => p.Name.Equals(item.Name, SyncGlobalization.DataSourceStringComparison)))
                throw new SyncParameterAlreadyExistsException(item.Name);

            InnerCollection.Add(item);
        }

        /// <summary>
        /// Add an array of parameters
        /// </summary>
        public void AddRange(IEnumerable<SyncParameter> parameters)
        {
            foreach(var p in parameters)
                Add(p);
        }

        /// <summary>
        /// Get a parameters by its name
        /// </summary>
        public SyncParameter this[string name]
        {
            get
            {
                if (string.IsNullOrEmpty(name))
                    throw new ArgumentNullException("name");

                return InnerCollection.FirstOrDefault(p => string.Equals(p.Name, name, SyncGlobalization.DataSourceStringComparison));
            }
        }


        /// <summary>
        /// Clear
        /// </summary>
        public void Clear()
        {
            InnerCollection.Clear();
        }

        public SyncParameter this[int index] => InnerCollection[index];
        public int Count => InnerCollection.Count;
        public bool IsReadOnly => false;
        SyncParameter IList<SyncParameter>.this[int index] { get => InnerCollection[index]; set => InnerCollection[index] = value; }
        public void Insert(int index, SyncParameter item) => InnerCollection.Insert(index, item);
        public bool Remove(SyncParameter item) => InnerCollection.Remove(item);
        public bool Remove(string name) => InnerCollection.Remove(this[name]);
        public bool Contains(SyncParameter item) => InnerCollection.Contains(item);
        public bool Contains(string name) => this[name] != null;
        public void CopyTo(SyncParameter[] array, int arrayIndex) => InnerCollection.CopyTo(array, arrayIndex);
        public int IndexOf(SyncParameter item) => InnerCollection.IndexOf(item);
        public void RemoveAt(int index) => InnerCollection.RemoveAt(index);
        IEnumerator IEnumerable.GetEnumerator() => InnerCollection.GetEnumerator();
        public IEnumerator<SyncParameter> GetEnumerator() => InnerCollection.GetEnumerator();
        public override string ToString() => this.InnerCollection.Count.ToString();
    }

}
