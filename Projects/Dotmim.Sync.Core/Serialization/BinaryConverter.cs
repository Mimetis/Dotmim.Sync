using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.Serialization.Formatters;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;

namespace Dotmim.Sync.Serialization
{
    public class BinaryConverter<T> : BaseConverter<T>
    {
        BinaryFormatter binaryFormatter = new BinaryFormatter();

        public BinaryConverter()
        {
            binaryFormatter.TypeFormat = FormatterTypeStyle.TypesAlways;
        }
        public override T Deserialize(Stream ms)
        {
            var obj = binaryFormatter.Deserialize(ms);

            return (T)obj;
        }

        public override void Serialize(T obj, Stream ms)
        {
            binaryFormatter.Serialize(ms, obj);
        }

        public override byte[] Serialize(T obj)
        {
            using (var ms = new MemoryStream())
            {
                binaryFormatter.Serialize(ms, obj);

                ms.Seek(0, SeekOrigin.Begin);

                Byte[] array = ms.ToArray();

                return array;
            }

        }
    }
}
