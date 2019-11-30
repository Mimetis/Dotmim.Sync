using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Dotmim.Sync.Serialization
{

    public class JsonConverterFactory : ISerializerFactory
    {
        public string Key => "json";
        private static JsonConverterFactory instance = null;
        public static JsonConverterFactory Current => instance ?? new JsonConverterFactory();

        public ISerializer<T> GetSerializer<T>() => new JsonConverter<T>();

    }

    public class JsonConverter<T> : ISerializer<T>
    {

        public T Deserialize(Stream ms)
        {
            using (var sr = new StreamReader(ms))
            {
                using (var reader = new JsonTextReader(sr))
                {
                    var serializer = new JsonSerializer();
                    return serializer.Deserialize<T>(reader);
                }
            }
        }


        public byte[] Serialize(T obj)
        {
            using (var ms = new MemoryStream())
            {
                using (var writer = new StreamWriter(ms))
                {
                    using (var jsonWriter = new JsonTextWriter(writer))
                    {
                        var serializer = new JsonSerializer();
                        serializer.Serialize(jsonWriter, obj);
                    }
                }
                return ms.ToArray();
            }
        }

    }
}
