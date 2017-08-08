//using System;
//using System.Collections;
//using System.Collections.Generic;
//using System.Collections.ObjectModel;
//using System.Globalization;
//using System.Text;

//namespace Dotmim.Sync.Data
//{
//    public class DmRelationCollection : IEnumerable<DmRelation>
//    {
//        private readonly Collection<DmRelation> _relations;

//        // dmTable owner of the relation (should be eventually a foreign key relation hosted by the child dmTable)
//        private DmSet dmSet;

//        public DmRelationCollection(DmSet dmSet)
//        {
//            this.dmSet = dmSet;
//        }

//        public int Count => _relations.Count;

//        public bool IsReadOnly => true;

//        internal void Add(DmRelation relation)
//        {
//            if (relation == null)
//                throw new ArgumentNullException(nameof(relation));

//            relation.CheckState();

//            if (relation.DmSet == this.dmSet)
//                throw new Exception("RelationAlreadyInTheDataSet");

//            if (relation.DmSet != null)
//                throw new Exception("RelationAlreadyInOtherDataSet");

//            if (relation.ChildTable.Culture != relation.ParentTable.Culture ||
//                relation.ChildTable.CaseSensitive != relation.ParentTable.CaseSensitive)
//                throw new Exception("CaseLocaleMismatch");

//        }

       

//        //public void AddRange(DmRelation[] relations)
//        //{
//        //    if (relations != null)
//        //    {
//        //        foreach (DmRelation relation in relations)
//        //            if (relation != null)
//        //                Add(relation);
//        //    }
//        //}

//        //public DmRelation Add(string name, DmColumn[] parentColumns, DmColumn[] childColumns)
//        //{
//        //    var relation = new DmRelation(name, parentColumns, childColumns);
//        //    Add(relation);
//        //    return relation;
//        //}

//        //public DmRelation Add(string name, DmColumn parentColumn, DmColumn childColumn)
//        //{
//        //    var relation = new DmRelation(name, parentColumn, childColumn);
//        //    Add(relation);
//        //    return relation;
//        //}


//        /// <summary>
//        /// Gets the relation specified by index.
//        /// </summary>
//        public DmRelation this[int index]
//        {
//            get
//            {
//                if (index >= 0 && index < _relations.Count)
//                    return _relations[index];
//                throw new ArgumentOutOfRangeException("index out of range");
//            }
//        }

//        /// <summary>
//        /// Gets the relation specified by name.
//        /// </summary>
//        public DmRelation this[string name]
//        {
//            get
//            {

//                foreach (var r in _relations)
//                {
//                    if (this.dmSet.IsEqual(name, r.RelationName))
//                        return r;
//                }
//                return null;
//            }
//        }

//        public void Clear()
//        {
//            this._relations.Clear();
//        }

//        public bool Contains(DmRelation item)
//        {
//            throw new NotImplementedException();
//        }

//        public void CopyTo(DmRelation[] array, int arrayIndex)
//        {
//            throw new NotImplementedException();
//        }

//        public bool Remove(DmRelation item)
//        {
//            return this._relations.Remove(item);
//        }

//        public IEnumerator<DmRelation> GetEnumerator()
//        {
//            return _relations.GetEnumerator();
//        }

//        IEnumerator IEnumerable.GetEnumerator()
//        {
//            return _relations.GetEnumerator();
//        }

       
//    }
//}
