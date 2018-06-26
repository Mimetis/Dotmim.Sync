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

        public BinaryConverter()
        {
        }
        public override T Deserialize(Stream ms)
        {

            BinaryFormatter binaryFormatter = new BinaryFormatter
            {
                TypeFormat = FormatterTypeStyle.TypesAlways
            };
            var obj = binaryFormatter.Deserialize(ms);
            return (T)obj;
        }

        public override void Serialize(T obj, Stream ms)
        {
            BinaryFormatter binaryFormatter = new BinaryFormatter
            {
                TypeFormat = FormatterTypeStyle.TypesAlways
            };
            binaryFormatter.Serialize(ms, obj);
        }

        public override byte[] Serialize(T obj)
        {
            using (var ms = new MemoryStream())
            {
                BinaryFormatter binaryFormatter = new BinaryFormatter
                {
                    TypeFormat = FormatterTypeStyle.TypesAlways
                };
                binaryFormatter.Serialize(ms, obj);

                ms.Seek(0, SeekOrigin.Begin);

                Byte[] array = ms.ToArray();

                return array;
            }

        }
    }
}
