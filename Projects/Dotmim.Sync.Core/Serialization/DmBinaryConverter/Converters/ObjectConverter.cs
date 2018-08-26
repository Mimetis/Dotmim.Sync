using System;
using System.Collections.Concurrent;
using System.Globalization;

namespace Dotmim.Sync.Serialization.Converters
{
    public abstract class ObjectConverter
    {

        private static ConcurrentDictionary<Type, ObjectConverter> converters = new ConcurrentDictionary<Type, ObjectConverter>();

        static ObjectConverter()
        {
            converters.TryAdd(typeof(Type), new ObjectTypeConverter());
            converters.TryAdd((typeof(Type).GetType()), new ObjectTypeConverter());
            converters.TryAdd(typeof(CultureInfo), new CultureInfoConverter());
            converters.TryAdd(typeof(Version), new VersionConverter());

        }
        /// <summary>
        /// Adding a converter
        /// </summary>
        public static void AddConverter(Type type, ObjectConverter converter)
        {
            if (ObjectConverter.converters.ContainsKey(type))
                throw new ArgumentException($"Converter {type.Name} already exists.");

            ObjectConverter.converters.TryAdd(type, converter);
        }

        /// <summary>
        /// Getting an existing converter
        /// </summary>
        public static ObjectConverter GetConverter(Type objType)
        {

            if (ObjectConverter.converters.ContainsKey(objType))
                return ObjectConverter.converters[objType];

            return null ;
        }
        public abstract object ConvertFromString(String obj);
        public abstract string ConvertToString(Object s);

    }
}
