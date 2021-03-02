//using System;
//using System.Collections.Generic;
//using System.IO;
//using System.Text;
//using System.Threading.Tasks;

//namespace Dotmim.Sync.Serialization
//{
//    public class Utf8JsonConverterFactory : ISerializerFactory
//    {
//        public string Key => "utf8json";
//        private static Utf8JsonConverterFactory instance = null;
//        public static Utf8JsonConverterFactory Current => instance ?? new Utf8JsonConverterFactory();

//        public ISerializer<T> GetSerializer<T>() => new Utf8JsonConverter<T>();

//    }
//    public class Utf8JsonConverter<T> : ISerializer<T>
//    {
//        public Utf8JsonConverter()
//        {
                
//        }
//        public Task<T> DeserializeAsync(Stream ms)
//        {
//            return Utf8Json.JsonSerializer.DeserializeAsync<T>(ms);
//        }
//        public Task<byte[]> SerializeAsync(T obj)
//        {
//            return Task.FromResult(Utf8Json.JsonSerializer.SerializeUnsafe(obj).Array);
//        }
//    }
//}
