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
    public class SetupFilters : ICollection<SetupFilter>, IList<SetupFilter>
    {
        // reference to setup's parent
        private readonly SyncSetup setup;

        private Collection<SetupFilter> innerCollection = new Collection<SetupFilter>();

        /// <summary>
        /// Create a default collection for Serializers
        /// </summary>
        public SetupFilters()
        {
        }

        /// <summary>
        /// Add a new filter 
        /// </summary>
        public void Add(SetupFilter item)
        {
            if (innerCollection.Any(st => item == st))
                throw new Exception($"Filter on column {item.ColumnName} already exists in the collection");

            this.innerCollection.Add(item);
        }

        /// <summary>
        /// Add a new filter 
        /// </summary>
        public SetupFilters Add(string tableName, string columnName, string schemaName = null)
        {
            var item = new SetupFilter(tableName, columnName, schemaName);
            this.Add(item);
            return this;
        }


        public void Clear() => this.innerCollection.Clear();
        public SetupFilter this[int index] => innerCollection[index];
        public int Count => innerCollection.Count;
        public bool IsReadOnly => false;
        SetupFilter IList<SetupFilter>.this[int index] { get => innerCollection[index]; set => innerCollection[index] = value; }
        public void Insert(int index, SetupFilter item) => innerCollection.Insert(index, item);
        public bool Remove(SetupFilter item) => innerCollection.Remove(item);
        public bool Contains(SetupFilter item) => innerCollection.Any(f => f == item);
        public void CopyTo(SetupFilter[] array, int arrayIndex) => innerCollection.CopyTo(array, arrayIndex);
        public int IndexOf(SetupFilter item) => innerCollection.IndexOf(item);
        public void RemoveAt(int index) => innerCollection.RemoveAt(index);
        IEnumerator IEnumerable.GetEnumerator() => innerCollection.GetEnumerator();
        public IEnumerator<SetupFilter> GetEnumerator() => innerCollection.GetEnumerator();
        public override string ToString() => this.innerCollection.Count.ToString();
    }

}
