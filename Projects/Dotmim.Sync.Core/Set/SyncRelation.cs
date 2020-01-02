using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;

namespace Dotmim.Sync
{
    [DataContract(Name = "sr"), Serializable]
    public class SyncRelation : IDisposable, IEquatable<SyncRelation>
    {

        /// <summary>
        /// Gets or Sets the relation name 
        /// </summary>
        [DataMember(Name = "n", IsRequired = true, Order = 1)]
        public string RelationName { get; set; }

        /// <summary>
        /// Gets or Sets a list of columns that represent the parent key.
        /// </summary>
        [DataMember(Name = "pks", IsRequired = true, Order = 2)]
        public IList<SyncColumnIdentifier> ParentKeys { get; set; } = new List<SyncColumnIdentifier>();

        /// <summary>
        /// Gets or Sets a list of columns that represent the parent key.
        /// </summary>
        [DataMember(Name = "cks", IsRequired = true, Order = 3)]
        public IList<SyncColumnIdentifier> ChildKeys { get; set; } = new List<SyncColumnIdentifier>();

        /// <summary>
        /// Gets the ShemaFilter's SyncSchema
        /// </summary>
        [IgnoreDataMember]
        public SyncSet Schema { get; set; }

        public SyncRelation() { }

        public SyncRelation(string relationName, SyncSet schema=null)
        {
            this.RelationName = relationName;
            this.Schema = schema;
        }

        public SyncRelation(string relationName, IList<SyncColumnIdentifier> parentColumns, IList<SyncColumnIdentifier>  childColumns, SyncSet schema=null)
        {
            this.RelationName = relationName;
            this.ParentKeys = parentColumns;
            this.ChildKeys = childColumns;
            this.Schema = schema;
        }


        public SyncRelation Clone()
        {
            var clone = new SyncRelation();
            clone.RelationName = this.RelationName;

            foreach (var pk in this.ParentKeys)
                clone.ParentKeys.Add(pk.Clone());

            foreach (var ck in this.ChildKeys)
                clone.ChildKeys.Add(ck.Clone());

            return clone;
        }

        /// <summary>
        /// Clear 
        /// </summary>
        public void Clear() => this.Dispose(true);

        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool cleanup)
        {
            // Dispose managed ressources
            if (cleanup)
            {
                // clean rows
                this.ChildKeys.Clear() ;
                this.ParentKeys.Clear();
                this.Schema = null;
            }

            // Dispose unmanaged ressources
        }

        /// <summary>
        /// Ensure this relation has correct Schema reference
        /// </summary>
        public void EnsureRelation(SyncSet schema) => this.Schema = schema;


        /// <summary>
        /// Get parent table
        /// </summary>
        public SyncTable GetParentTable()
        {
            if (this.Schema == null || this.ParentKeys.Count() <= 0)
                return null;

            var id = this.ParentKeys.First();

            return this.Schema.Tables[id.TableName, id.SchemaName];
        }

        /// <summary>
        /// Get child table
        /// </summary>
        public SyncTable GetChildTable()
        {
            if (this.Schema == null || this.ChildKeys.Count() <= 0)
                return null;

            var id = this.ChildKeys.First();

            return this.Schema.Tables[id.TableName, id.SchemaName];
        }

        public override bool Equals(object obj)
        {
            return this.Equals(obj as SyncRelation);
        }

        public bool Equals(SyncRelation other)
        {
            return other != null &&
                   this.RelationName.Equals(other.RelationName, SyncGlobalization.DataSourceStringComparison);
        }

        public override int GetHashCode()
        {
            return 459874666 + EqualityComparer<string>.Default.GetHashCode(this.RelationName);
        }

        public static bool operator ==(SyncRelation left, SyncRelation right)
        {
            return EqualityComparer<SyncRelation>.Default.Equals(left, right);
        }

        public static bool operator !=(SyncRelation left, SyncRelation right)
        {
            return !(left == right);
        }
    }
}
