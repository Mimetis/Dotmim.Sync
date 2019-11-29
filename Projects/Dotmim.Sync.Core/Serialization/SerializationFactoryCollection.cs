using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Serialization
{
    public class SerializationFactoryCollection : IDictionary<string, ISerializerFactory>
    {
        private const string DEFAULT_KEY = "json";
        private static ISerializerFactory DEFAULT_FACTORY = new JsonConverterFactory();

        private Dictionary<string, ISerializerFactory> serializers = new Dictionary<string, ISerializerFactory>();

        public ISerializerFactory this[string key]
        {
            get => serializers[key];
            set => serializers[key] = value;
        }

        public ISerializerFactory CurrentSerializerFactory { get; private set; }
        public string CurrentKey { get; private set; }

        public ICollection<string> Keys => serializers.Keys;

        public ICollection<ISerializerFactory> Values => serializers.Values;

        public int Count => serializers.Count;

        public bool IsReadOnly => false;

        public void Add(string key, ISerializerFactory value) => this.Add(key, value, false);

        public void Add(string key, ISerializerFactory value, bool isDefault)
        {
            if (isDefault)
                this.SetDefaultSerializer(key, value);

            serializers.Add(key, value);
        }

        public SerializationFactoryCollection()
        {
            // add json serializer, as default
            this.Add(DEFAULT_KEY, DEFAULT_FACTORY, true);
            // add binary serializer;
            this.Add("binary", new BinarySerializerFactory());
        }
                

        /// <summary>
        /// Set default serializer
        /// </summary>
        private void SetDefaultSerializer(string key, ISerializerFactory value)
        {
            this.CurrentKey = key;
            this.CurrentSerializerFactory = value;

        }

        public void Add(KeyValuePair<string, ISerializerFactory> item) => this.Add(item, false);

        public void Add(KeyValuePair<string, ISerializerFactory> item, bool isDefault)
        {
            if (isDefault)
                this.SetDefaultSerializer(item.Key, item.Value);

            serializers.Add(item.Key, item.Value);
        }

        public void Clear() => serializers.Clear();

        public bool Contains(KeyValuePair<string, ISerializerFactory> item) => serializers.ContainsKey(item.Key);

        public bool ContainsKey(string key) => serializers.ContainsKey(key);

        public void CopyTo(KeyValuePair<string, ISerializerFactory>[] array, int index)
        {
            if (array == null)
                throw new ArgumentNullException("array");

            if (index < 0 || index > array.Length)
                throw new ArgumentOutOfRangeException("index", index, "Index out of range");

            if (array.Length - index < Count)
                throw new ArgumentException("Array too small");

            foreach (var item in this)
                array[index++] = new KeyValuePair<string, ISerializerFactory>(item.Key, item.Value);

        }

        public IEnumerator<KeyValuePair<string, ISerializerFactory>> GetEnumerator() => serializers.GetEnumerator();

        public bool Remove(string key)
        {
            var isDeleted = serializers.Remove(key);

            if (isDeleted && this.CurrentKey == key)
            {
                this.CurrentKey = DEFAULT_KEY;
                this.CurrentSerializerFactory = DEFAULT_FACTORY;
            }

            return isDeleted;
        }

        public bool Remove(KeyValuePair<string, ISerializerFactory> item) => this.Remove(item.Key);

        public bool TryGetValue(string key, out ISerializerFactory value) => this.TryGetValue(key, out value);

        IEnumerator IEnumerable.GetEnumerator() => this.GetEnumerator();
    }
}
