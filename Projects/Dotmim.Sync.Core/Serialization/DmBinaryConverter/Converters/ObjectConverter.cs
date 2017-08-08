using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;

namespace Dotmim.Sync.Serialization.Converters
{
    public abstract class ObjectConverter
    {

        private static Dictionary<Type, ObjectConverter> converters = new Dictionary<Type, ObjectConverter>();

        static ObjectConverter()
        {
            converters.Add(typeof(Type), new ObjectTypeConverter());
            converters.Add((typeof(Type).GetType()), new ObjectTypeConverter());
            converters.Add(typeof(CultureInfo), new CultureInfoConverter());
            converters.Add(typeof(Version), new VersionConverter());

        }
        /// <summary>
        /// Adding a converter
        /// </summary>
        public static void AddConverter(Type type, ObjectConverter converter)
        {
            if (ObjectConverter.converters.ContainsKey(type))
                throw new ArgumentException($"Converter {type.Name} already exists.");

            ObjectConverter.converters.Add(type, converter);
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
