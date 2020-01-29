using Dotmim.Sync.Builders;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;

namespace Dotmim.Sync
{
    public class SetupFilters : ICollection<SetupFilter>, IList<SetupFilter>
    {
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
                throw new FilterAlreadyExistsException(item.TableName);

            this.innerCollection.Add(item);
        }

        /// <summary>
        /// Add a new filter 
        /// </summary>
        public SetupFilters Add(string tableName, string columnName, string schemaName = null)
        {
            // Create a filter on the table
            var item = new SetupFilter(tableName, schemaName);

            // Add a column as parameter. This column will be automaticaly added in the tracking table
            item.AddParameter(columnName, tableName, schemaName, true);

            // add the side where expression, allowing to be null
            item.AddWhere(columnName, tableName, schemaName);

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
