//using System;
//using System.Collections.Generic;
//using System.IO;
//using System.Runtime.Serialization.Formatters;
//using System.Runtime.Serialization.Formatters.Binary;
//using System.Text;
//using System.Threading.Tasks;

//namespace Dotmim.Sync.Serialization
//{

//    // Obsolete
//    // https://docs.microsoft.com/en-us/dotnet/standard/serialization/binaryformatter-security-guide
//    public class BinarySerializerFactory : ISerializerFactory
//    {
//        public string Key => "binary";
//        private static BinarySerializerFactory instance = null;
//        public static BinarySerializerFactory Current => instance ?? new BinarySerializerFactory();

//        public ISerializer<T> GetSerializer<T>() => new BinarySerializer<T>();

//    }
//    public class BinarySerializer<T> : ISerializer<T>
//    {

//        public BinarySerializer()
//        {
//        }
//        public Task<T> DeserializeAsync(Stream ms)
//        {

//            var binaryFormatter = new BinaryFormatter
//            {
//                TypeFormat = FormatterTypeStyle.TypesAlways
//            };
//            var obj = binaryFormatter.Deserialize(ms);
//            return Task.FromResult((T)obj);
//        }


//        public Task<byte[]> SerializeAsync(T obj)
//        {
//            using (var ms = new MemoryStream())
//            {
//                var binaryFormatter = new BinaryFormatter
//                {
//                    TypeFormat = FormatterTypeStyle.TypesAlways
//                };
//                binaryFormatter.Serialize(ms, obj);

//                ms.Seek(0, SeekOrigin.Begin);

//                byte[] array = ms.ToArray();

//                return Task.FromResult(array);
//            }

//        }
//    }
//}
