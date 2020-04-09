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
    [CollectionDataContract(Name = "fils", ItemName = "fil"), Serializable]
    public class SetupFilters : ICollection<SetupFilter>, IList<SetupFilter>
    {
        /// <summary>
        /// Exposing the InnerCollection for serialization purpose
        /// </summary>
        [DataMember(Name = "c", IsRequired = true, Order = 1)]
        public Collection<SetupFilter> InnerCollection = new Collection<SetupFilter>();

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
            if (InnerCollection.Any(st => item == st))
                throw new FilterAlreadyExistsException(item.TableName);

            this.InnerCollection.Add(item);
        }

        /// <summary>
        /// Add a new filter 
        /// </summary>
        public SetupFilters Add(string tableName, string columnName, string schemaName = null, bool allowNull = false)
        {
            // Create a filter on the table
            var item = new SetupFilter(tableName, schemaName);

            // Add a column as parameter. This column will be automaticaly added in the tracking table
            item.AddParameter(columnName, tableName, schemaName, allowNull);

            // add the side where expression, allowing to be null
            item.AddWhere(columnName, tableName, columnName, schemaName);

            this.Add(item);
            return this;
        }


        public void Clear() => this.InnerCollection.Clear();
        public SetupFilter this[int index] => InnerCollection[index];
        public int Count => InnerCollection.Count;
        public bool IsReadOnly => false;
        SetupFilter IList<SetupFilter>.this[int index] { get => InnerCollection[index]; set => InnerCollection[index] = value; }
        public void Insert(int index, SetupFilter item) => InnerCollection.Insert(index, item);
        public bool Remove(SetupFilter item) => InnerCollection.Remove(item);
        public bool Contains(SetupFilter item) => InnerCollection.Any(f => f == item);
        public void CopyTo(SetupFilter[] array, int arrayIndex) => InnerCollection.CopyTo(array, arrayIndex);
        public int IndexOf(SetupFilter item) => InnerCollection.IndexOf(item);
        public void RemoveAt(int index) => InnerCollection.RemoveAt(index);
        IEnumerator IEnumerable.GetEnumerator() => InnerCollection.GetEnumerator();
        public IEnumerator<SetupFilter> GetEnumerator() => InnerCollection.GetEnumerator();
        public override string ToString() => this.InnerCollection.Count.ToString();
    }

}
