using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;

namespace Dotmim.Sync.Serialization
{

    public class ContractSerializerFactory : ISerializerFactory
    {
        private static ContractSerializerFactory instance = null;
        public static ContractSerializerFactory Current => instance ?? new ContractSerializerFactory();

        public ISerializer<T> GetSerializer<T>() => new ContractSerializer<T>();

    }
    public class ContractSerializer<T> : ISerializer<T>
    {

        public ContractSerializer()
        {
        }
        public T Deserialize(Stream ms)
        {

            var serializer = new DataContractSerializer(typeof(T));
            using (var decompress = new GZipStream(ms, CompressionMode.Decompress))
            {
                return (T)serializer.ReadObject(decompress);
            }

        }


        public byte[] Serialize(T obj)
        {
            var serializer = new DataContractSerializer(typeof(T));

            using (var ms = new MemoryStream())
            {
                using (var compress = new GZipStream(ms, CompressionMode.Compress))
                {
                    serializer.WriteObject(compress, obj);

                    return ms.ToArray();
                }
            }

        }
    }
}

