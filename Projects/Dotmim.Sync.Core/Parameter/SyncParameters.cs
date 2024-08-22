using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;

namespace Dotmim.Sync
{
    /// <summary>
    /// Parameters used to filter rows.
    /// </summary>
    [CollectionDataContract(Name = "params", ItemName = "param"), Serializable]
    public class SyncParameters : ICollection<SyncParameter>, IList<SyncParameter>
    {
        private static string defaultScopeHash;

        /// <summary>
        /// Gets or Sets the InnerCollection (Exposed as Public for serialization purpose).
        /// </summary>
        [DataMember(Name = "c", IsRequired = true)]
        public Collection<SyncParameter> InnerCollection { get; set; } = new Collection<SyncParameter>();

        /// <summary>
        /// Initializes a new instance of the <see cref="SyncParameters"/> class.
        /// Create a default collection for SerializersFactory.
        /// </summary>
        public SyncParameters()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SyncParameters"/> class.
        /// </summary>
        public SyncParameters(params (string Name, object Value)[] parameters) => this.AddRange(parameters.Select(p => new SyncParameter(p.Name, p.Value)));

        /// <summary>
        /// Initializes a new instance of the <see cref="SyncParameters"/> class.
        /// </summary>
        public SyncParameters(params SyncParameter[] parameters) => this.AddRange(parameters);

        /// <summary>
        /// Add a new sync parameter.
        /// </summary>
        public void Add<T>(string name, T value) => this.Add(new SyncParameter(name, value));

        /// <summary>
        /// Add a new sync parameter.
        /// </summary>
        public void Add(SyncParameter item)
        {
            if (item == null)
                return;

            if (this.Any(p => p.Name.Equals(item.Name, SyncGlobalization.DataSourceStringComparison)))
                throw new SyncParameterAlreadyExistsException(item.Name);

            this.InnerCollection.Add(item);
        }

        /// <summary>
        /// Add an array of parameters.
        /// </summary>
        public void AddRange(IEnumerable<SyncParameter> parameters)
        {
            if (parameters == null)
                return;

            foreach (var p in parameters)
                this.Add(p);
        }

        /// <summary>
        /// Get a parameters by its name.
        /// </summary>
        public SyncParameter this[string name]
        {
            get
            {
                if (string.IsNullOrEmpty(name))
                    throw new ArgumentNullException(nameof(name));

                return this.InnerCollection.FirstOrDefault(p => string.Equals(p.Name, name, SyncGlobalization.DataSourceStringComparison));
            }
        }

        /// <summary>
        /// Clear.
        /// </summary>
        public void Clear() => this.InnerCollection.Clear();

        /// <summary>
        /// Get a hash code to identify the parameters uniquely.
        /// </summary>
        public string GetHash()
        {
            var flatParameters = string.Concat(this.OrderBy(p => p.Name).Select(p => $"{p.Name}.{p.Value}"));
            var b = Encoding.UTF8.GetBytes(flatParameters);
            var hash1 = HashAlgorithm.SHA256.Create(b);
            var hash1String = Convert.ToBase64String(hash1);
            return hash1String;
        }

        /// <summary>
        /// Gets get default hash code for the default scope.
        /// </summary>
        [IgnoreDataMember]
        public static string DefaultScopeHash
        {
            get
            {
                if (string.IsNullOrEmpty(defaultScopeHash))
                {
                    var b = Encoding.UTF8.GetBytes(SyncOptions.DefaultScopeName);
                    var hash1 = HashAlgorithm.SHA256.Create(b);
                    defaultScopeHash = Convert.ToBase64String(hash1);
                }

                return defaultScopeHash;
            }
        }

        /// <summary>
        /// Gets the count of parameters.
        /// </summary>
        public int Count => this.InnerCollection.Count;

        /// <summary>
        /// Gets a value indicating whether gets if the collection is readonly.
        /// </summary>
        public bool IsReadOnly => false;

        /// <summary>
        /// Gets the index of a parameter.
        /// </summary>
        SyncParameter IList<SyncParameter>.this[int index] { get => this.InnerCollection[index]; set => this.InnerCollection[index] = value; }

        /// <summary>
        /// Gets the Sync parameter by its index.
        /// </summary>
        public SyncParameter this[int index] { get => this.InnerCollection[index]; set => this.InnerCollection[index] = value; }

        /// <summary>
        /// Insert a parameter at a specific index.
        /// </summary>
        public void Insert(int index, SyncParameter item) => this.InnerCollection.Insert(index, item);

        /// <summary>
        /// Remove a parameter.
        /// </summary>
        public bool Remove(SyncParameter item) => this.InnerCollection.Remove(item);

        /// <summary>
        /// Remove a parameter by its name.
        /// </summary>
        public bool Remove(string name) => this.InnerCollection.Remove(this[name]);

        /// <summary>
        /// Check if the collection contains a parameter.
        /// </summary>
        public bool Contains(SyncParameter item) => this.InnerCollection.Contains(item);

        /// <summary>
        /// Check if the collection contains a parameter by its name.
        /// </summary>
        public bool Contains(string name) => this[name] != null;

        /// <summary>
        /// Copy the parameters to an array.
        /// </summary>
        public void CopyTo(SyncParameter[] array, int arrayIndex) => this.InnerCollection.CopyTo(array, arrayIndex);

        /// <summary>
        /// Gets the index of a parameter.
        /// </summary>
        public int IndexOf(SyncParameter item) => this.InnerCollection.IndexOf(item);

        /// <summary>
        /// Remove a parameter at a specific index.
        /// </summary>
        public void RemoveAt(int index) => this.InnerCollection.RemoveAt(index);

        /// <summary>
        /// Gets the enumerator.
        /// </summary>
        IEnumerator IEnumerable.GetEnumerator() => this.InnerCollection.GetEnumerator();

        /// <summary>
        /// Gets the enumerator.
        /// </summary>
        public IEnumerator<SyncParameter> GetEnumerator() => this.InnerCollection.GetEnumerator();

        /// <summary>
        /// Gets the string representation of the parameters, using the count property.
        /// </summary>
        public override string ToString() => this.InnerCollection.Count.ToString(CultureInfo.InvariantCulture);
    }
}