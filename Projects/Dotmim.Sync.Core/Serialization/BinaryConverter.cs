using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.Serialization.Formatters;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;

namespace Dotmim.Sync.Serialization
{

    public class BinarySerializerFactory : ISerializerFactory
    {
        private static BinarySerializerFactory instance = null;
        public static BinarySerializerFactory Current => instance ?? new BinarySerializerFactory();

        public ISerializer<T> GetSerializer<T>() => new BinarySerializer<T>();

    }
    public class BinarySerializer<T> : ISerializer<T>
    {

        public BinarySerializer()
        {
        }
        public T Deserialize(Stream ms)
        {

            var binaryFormatter = new BinaryFormatter
            {
                TypeFormat = FormatterTypeStyle.TypesAlways
            };
            var obj = binaryFormatter.Deserialize(ms);
            return (T)obj;
        }


        public byte[] Serialize(T obj)
        {
            using (var ms = new MemoryStream())
            {
                var binaryFormatter = new BinaryFormatter
                {
                    TypeFormat = FormatterTypeStyle.TypesAlways
                };
                binaryFormatter.Serialize(ms, obj);

                ms.Seek(0, SeekOrigin.Begin);

                byte[] array = ms.ToArray();

                return array;
            }

        }
    }
}
