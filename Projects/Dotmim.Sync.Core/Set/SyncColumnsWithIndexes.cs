//using Dotmim.Sync.Builders;
//using System;
//using System.Collections;
//using System.Collections.Generic;
//using System.Collections.ObjectModel;
//using System.Linq;
//using System.Runtime.Serialization;
//using System.Text;

//namespace Dotmim.Sync
//{
//    [CollectionDataContract(Name = "cols", ItemName = "col"), Serializable]
//    public class SyncColumns : ICollection<SyncColumn>, IList<SyncColumn>
//    {
//        /// <summary>
//        /// Exposing the InnerCollection for serialization purpose
//        /// </summary>
//        [DataMember(Name = "c", IsRequired = true, Order = 1)]
//        public Collection<SyncColumn> InnerCollection { get; set; } = new Collection<SyncColumn>();


//        [DataMember(Name = "i", IsRequired = true, Order = 2)]
//        public Dictionary<string, int> InnerIndexes { get; set; } = new Dictionary<string, int>();

//        /// <summary>
//        /// Column's schema
//        /// </summary>
//        [IgnoreDataMember]
//        public SyncTable Table { get; internal set; }

//        /// <summary>
//        /// Create a default collection for Serializers
//        /// </summary>
//        public SyncColumns()
//        {
//        }

//        /// <summary>
//        /// Create a new collection of tables for a SyncSchema
//        /// </summary>
//        public SyncColumns(SyncTable table) => this.Table = table;

//        /// <summary>
//        /// Since we don't serializer the reference to the schema, this method will reaffect the correct schema
//        /// </summary>
//        public void EnsureColumns(SyncTable table)
//        {
//            this.Table = table;
//        }

//        /// <summary>
//        /// Get a Column by its name
//        /// </summary>
//        public SyncColumn this[string columnName]
//        {
//            get
//            {
//                var col = InnerCollection[InnerIndexes[columnName.ToLowerInvariant()]];
//                //var col = InnerCollection.FirstOrDefault(c => string.Equals(c.ColumnName, columnName, SyncGlobalization.DataSourceStringComparison));
//                return col;
//            }
//        }


//        /// <summary>
//        /// Add a new Column to the Schema Column collection
//        /// </summary>
//        public void Add(SyncColumn item)
//        {
//            InnerCollection.Add(item);
//            AffectOrder();
//        }

//        public void Add(string columnName, Type type = null)
//        {
//            var item = new SyncColumn(columnName, type);
//            InnerCollection.Add(item);
//            AffectOrder();

//        }



//        /// <summary>
//        /// Add a collection of columns
//        /// </summary>
//        public void AddRange(SyncColumn[] addedColumns)
//        {
//            foreach (var item in addedColumns)
//                InnerCollection.Add(item);

//            AffectOrder();

//        }


//        /// <summary>
//        /// Reorganize columns order
//        /// </summary>
//        public void Reorder(SyncColumn column, int newPosition)
//        {
//            if (newPosition < 0 || newPosition > this.InnerCollection.Count - 1)
//                throw new Exception($"InvalidOrdinal(ordinal, {newPosition}");

//            // Remove column from collection
//            this.InnerCollection.Remove(column);

//            // Add at the end or insert in new positions
//            if (newPosition > this.InnerCollection.Count - 1)
//                this.InnerCollection.Add(column);
//            else
//                this.InnerCollection.Insert(newPosition, column);

//            AffectOrder();
//        }
//        private void AffectOrder()
//        {
//            this.InnerIndexes.Clear();
//            // now reordered correctly, affect new Ordinal property
//            for (int i = 0; i < this.InnerCollection.Count; i++)
//            {
//                var c = this.InnerCollection[i];
//                c.Ordinal = i;
//                this.InnerIndexes[c.ColumnName.ToLowerInvariant()] = i;
//            }

//        }

//        /// <summary>
//        /// Clear all the relations
//        /// </summary>
//        public void Clear()
//        {
//            this.InnerCollection.Clear();
//            this.InnerIndexes.Clear();
//        }

//        public SyncColumn this[int index] => InnerCollection[index];
//        public int Count => InnerCollection.Count;
//        public bool IsReadOnly => false;
//        SyncColumn IList<SyncColumn>.this[int index]
//        {
//            get => this.InnerCollection[index];
//            set
//            {
//                if (value == null)
//                    throw new Exception("Can't be null");

//                this.InnerCollection[index] = value;
//                this.InnerIndexes[value.ColumnName.ToLowerInvariant()] = index;
//            }
//        }
//        public bool Remove(SyncColumn item)
//        {
//            var isDeleted = InnerCollection.Remove(item);

//            if (isDeleted)
//                InnerIndexes.Remove(item.ColumnName.ToLowerInvariant());

//            return isDeleted;
//        }

//        public bool Contains(SyncColumn item) => InnerCollection.Contains(item);
//        public void CopyTo(SyncColumn[] array, int arrayIndex) => InnerCollection.CopyTo(array, arrayIndex);
//        public int IndexOf(SyncColumn item) => InnerCollection.IndexOf(item);
//        public void RemoveAt(int index)
//        {
//            InnerIndexes.Remove(InnerCollection[index].ColumnName.ToLowerInvariant());
//            InnerCollection.RemoveAt(index);
//        }
//        IEnumerator IEnumerable.GetEnumerator() => InnerCollection.GetEnumerator();
//        public IEnumerator<SyncColumn> GetEnumerator() => InnerCollection.GetEnumerator();
//        public override string ToString() => this.InnerCollection.Count.ToString();
//        public void Insert(int index, SyncColumn item)
//        {
//            this.InnerIndexes[item.ColumnName.ToLowerInvariant()] = index;
//            this.InnerCollection.Insert(index, item);
//        }
//    }

//}
