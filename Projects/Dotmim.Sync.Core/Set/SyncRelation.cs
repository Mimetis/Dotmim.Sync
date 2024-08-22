using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;

namespace Dotmim.Sync
{
    /// <summary>
    /// Represents a relation between two tables.
    /// </summary>
    [DataContract(Name = "sr"), Serializable]
    public class SyncRelation : SyncNamedItem<SyncRelation>
    {

        /// <summary>
        /// Gets or Sets the relation name.
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
        public IList<SyncColumnIdentifier> Keys { get; set; } = new List<SyncColumnIdentifier>();

        /// <summary>
        /// Gets or sets the ShemaFilter's SyncSchema.
        /// </summary>
        [IgnoreDataMember]
        public SyncSet Schema { get; set; }

        /// <inheritdoc cref="SyncRelation"/>
        public SyncRelation() { }

        /// <inheritdoc cref="SyncRelation"/>
        public SyncRelation(string relationName, SyncSet schema = null)
        {
            this.RelationName = relationName;
            this.Schema = schema;
        }

        /// <inheritdoc cref="SyncRelation"/>
        public SyncRelation(string relationName, IList<SyncColumnIdentifier> columns, IList<SyncColumnIdentifier> parentColumns, SyncSet schema = null)
        {
            this.RelationName = relationName;
            this.ParentKeys = parentColumns;
            this.Keys = columns;
            this.Schema = schema;
        }

        /// <summary>
        /// Return a clone of this relation.
        /// </summary>
        public SyncRelation Clone()
        {
            var clone = new SyncRelation();
            clone.RelationName = this.RelationName;

            foreach (var pk in this.ParentKeys)
                clone.ParentKeys.Add(pk.Clone());

            foreach (var ck in this.Keys)
                clone.Keys.Add(ck.Clone());

            return clone;
        }

        /// <summary>
        /// Clear.
        /// </summary>
        public void Clear()
        {
            // clean rows
            this.Keys.Clear();
            this.ParentKeys.Clear();
            this.Schema = null;
        }

        /// <summary>
        /// Ensure this relation has correct Schema reference.
        /// </summary>
        public void EnsureRelation(SyncSet schema) => this.Schema = schema;

        /// <summary>
        /// Get parent table.
        /// </summary>
        public SyncTable GetParentTable()
        {
            if (this.Schema == null || this.ParentKeys.Count <= 0)
                return null;

            var id = this.ParentKeys.First();

            return this.Schema.Tables[id.TableName, id.SchemaName];
        }

        /// <summary>
        /// Get child table.
        /// </summary>
        public SyncTable GetTable()
        {
            if (this.Schema == null || this.Keys.Count <= 0)
                return null;

            var id = this.Keys.First();

            return this.Schema.Tables[id.TableName, id.SchemaName];
        }

        /// <inheritdoc cref="SyncNamedItem{T}.GetAllNamesProperties"/>
        public override IEnumerable<string> GetAllNamesProperties()
        {
            yield return this.RelationName;
        }

        /// <inheritdoc cref="SyncNamedItem{T}.EqualsByProperties(T)"/>
        public override bool EqualsByProperties(SyncRelation otherInstance)
        {
            if (otherInstance == null)
                return false;

            if (!this.EqualsByName(otherInstance))
                return false;

            // Check list
            if (!this.Keys.CompareWith(otherInstance.Keys))
                return false;

            if (!this.ParentKeys.CompareWith(otherInstance.ParentKeys))
                return false;

            return true;
        }
    }
}