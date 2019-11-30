using Dotmim.Sync.Enumerations;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;

namespace Dotmim.Sync.Serialization
{
    public class SerializationFactoryCollection : ICollection<ISerializerFactory>
    {
        private static ISerializerFactory DEFAULT_FACTORY = new JsonConverterFactory();

        public ISerializerFactory CurrentSerializerFactory { get; private set; }
        public ISerializerFactory DefaultSerializerFactory => DEFAULT_FACTORY;


        public void Add(ISerializerFactory value, bool isDefault)
        {
            if (isDefault)
                this.SetDefaultSerializer(value);

            Add(value);
        }

 
        public SerializationFactoryCollection()
        {
            // add json serializer, as default
            this.Add(DEFAULT_FACTORY, true);
            // add binary serializer;
            this.Add(new ContractSerializerFactory());
        }

        public void SetDefaultSerializer(SerializationFormat serializationFormat)
        {
            switch (serializationFormat)
            {
                case SerializationFormat.Binary:
                    this.CurrentSerializerFactory = this["dc"];
                    break;
                case SerializationFormat.Json:
                default:
                    this.CurrentSerializerFactory = this["json"];
                    break;
            }

        }

        /// <summary>
        /// Set default serializer
        /// </summary>
        private void SetDefaultSerializer(ISerializerFactory value) => this.CurrentSerializerFactory = value;


        Collection<ISerializerFactory> collection = new Collection<ISerializerFactory>();


        public ISerializerFactory this[string key]
        {
            get
            {
                if (string.IsNullOrEmpty(key))
                    throw new ArgumentNullException("key");

                return collection.FirstOrDefault(c => c.Key.ToLowerInvariant() == key);
            }
        }

        public void Add(ISerializerFactory factory) => collection.Add(factory);
        public ISerializerFactory this[int index] => collection[index];
        public int Count => collection.Count;
        public bool IsReadOnly => false;
        public bool Remove(ISerializerFactory factory) => collection.Remove(factory);
        public void Clear() => collection.Clear();
        public bool Contains(ISerializerFactory item) => collection.Contains(item);
        public void CopyTo(ISerializerFactory[] array, int arrayIndex) => collection.CopyTo(array, arrayIndex);
        public int IndexOf(ISerializerFactory factory) => collection.IndexOf(factory);
        public void RemoveAt(int index) => collection.RemoveAt(index);
        IEnumerator IEnumerable.GetEnumerator() => collection.GetEnumerator();
        public IEnumerator<ISerializerFactory> GetEnumerator() => collection.GetEnumerator();
        public override string ToString() => this.collection.Count.ToString();
    }
}
