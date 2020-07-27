using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;

namespace Dotmim.Sync
{
    /// <summary>
    /// Based object that can be compared by name or by properties
    /// </summary>
    /// <typeparam name="T"></typeparam>
    [DataContract(Name = "sni"), Serializable]
    public abstract class SyncNamedItem<T> : IEquatable<T> where T : class
    {
        /// <summary>
        /// Get all comparable names properties to determine if two instances are identifed as "same" based on their name properties
        /// </summary>
        public abstract IEnumerable<string> GetAllNamesProperties();

        /// <summary>
        /// Gets a true boolean if other instance has the same name, defined by properties marked as names
        /// </summary>
        public virtual bool EqualsByName(T otherInstance)
        {
            // If one instance is null, should returns false, always
            if (otherInstance == null)
                return false;

            var namedOhterInstance = otherInstance as SyncNamedItem<T>;

            if (namedOhterInstance == null)
                return false;

            var sc = SyncGlobalization.DataSourceStringComparison;

            var props1 = this.GetAllNamesProperties().GetEnumerator();
            var props2 = namedOhterInstance.GetAllNamesProperties().GetEnumerator();


            while (props1.MoveNext())
            {
                props2.MoveNext();

                var prop1 = props1.Current;
                var prop2 = props2.Current;

                if (string.IsNullOrEmpty(prop1) && !string.IsNullOrEmpty(prop2))
                    return false;

                if (!string.IsNullOrEmpty(prop1) && string.IsNullOrEmpty(prop2))
                    return false;

                if (string.IsNullOrEmpty(prop1) && string.IsNullOrEmpty(prop2))
                    continue;

                if (!prop1.Equals(prop2, sc))
                    return false;

            }

            return true;
        }

        /// <summary>
        /// Gets a true boolean if other instance is defined as same based on all properties
        /// By default, if not overriden, check the names properties
        /// </summary>
        public virtual bool EqualsByProperties(T otherInstance) => this.EqualsByName(otherInstance);

        /// <summary>
        /// Gets a true boolean if other instance is defined as same based on all properties
        /// </summary>
        public bool Equals(T other) => this.EqualsByProperties(other);

        /// <summary>
        /// Gets a true boolean if other instance is defined as same based on all properties
        /// </summary>
        public override bool Equals(object obj) => this.EqualsByProperties(obj as T);

        public override int GetHashCode() => base.GetHashCode();

    }
}
