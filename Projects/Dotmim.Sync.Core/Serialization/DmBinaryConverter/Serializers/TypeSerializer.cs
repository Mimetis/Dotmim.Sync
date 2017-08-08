using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Reflection;
using System.Text;
using System.Linq;
using System.Collections.ObjectModel;
using Dotmim.Sync.Serialization.Converters;

namespace Dotmim.Sync.Serialization.Serializers
{
    public abstract class TypeSerializer
    {
        public abstract void Serialize(DmSerializer dmSerializer, object obj, Type objType);
        public abstract Object Deserialize(DmSerializer dmSerializer, Type objType, bool isDebugMode = false);

        /// <summary>
        /// Get the correct serializer for a given type
        /// </summary>
        public static TypeSerializer GetSerializer(Type t)
        {
            // Stop recursive 
            if (t == typeof(Type).GetType())
                throw new Exception("Can't serialize, du to unknwon type");

            if (t.IsPrimitiveManagedType())
                return new PrimitiveSerializer();

            // Check if nullable then get the underlying value
            if (t.IsNullableType())
                t = Nullable.GetUnderlyingType(t);

            if (t.IsDictionary())
                return new DictionarySerializer();

            if (t.IsEnumerable())
                return new ArraySerializer();

            // for some type we can convertfrom string 
            var converter = t.GetConverter();
            if (converter != null)
                return new CoreConverterSerializer(converter);

            // are own converters
            var ownConverter = ObjectConverter.GetConverter(t);
            if (ownConverter != null)
                return new ConverterSerializer(ownConverter);


            if (t.IsISerializable())
                return new SerializableSerializer();


            return new ObjectSerializer();
        }



    }


}
