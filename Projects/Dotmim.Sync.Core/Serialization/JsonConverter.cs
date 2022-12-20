//using Newtonsoft.Json;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync.Serialization
{

    public class JsonConverterFactory : ISerializerFactory
    {
        public string Key => "json";

        public ISerializer<T> GetSerializer<T>() => new JsonConverter<T>();
        public ISerializer GetSerializer(Type objectType) => new JsonConverter(objectType);

    }
  
    public class JsonConverter<T> : ISerializer<T>
    {
        public async Task<T> DeserializeAsync(Stream ms)
        {
            using var sr = new StreamReader(ms);
            using var jtr = new JsonTextReader(sr);
            
            var jobject = await JObject.LoadAsync(jtr);

            return jobject.ToObject<T>();
        }


        public async Task<byte[]> SerializeAsync(T obj)
        {
            var jobject = JObject.FromObject(obj);

            using var ms = new MemoryStream();
            using var sw = new StreamWriter(ms);
            using var jtw = new JsonTextWriter(sw);

#if DEBUG
            jtw.Formatting = Formatting.Indented;
#endif
            await jobject.WriteToAsync(jtw);
            
            await jtw.FlushAsync();
            await sw.FlushAsync();

            return ms.ToArray();
        }

    }

    public class JsonConverter : ISerializer
    {
        public JsonConverter(Type objectType) => this.ObjectType = objectType;

        public Type ObjectType { get; }

        public async Task<object> DeserializeAsync(Stream ms)
        {
            using var sr = new StreamReader(ms);
            using var jtr = new JsonTextReader(sr);

            var jobject = await JObject.LoadAsync(jtr);

            return jobject.ToObject(this.ObjectType);
        }

        public async Task<byte[]> SerializeAsync(object obj)
        {
            var jobject = JObject.FromObject(obj);

            using var ms = new MemoryStream();
            using var sw = new StreamWriter(ms);
            using var jtw = new JsonTextWriter(sw);

#if DEBUG
            jtw.Formatting = Formatting.Indented;
#endif
            await jobject.WriteToAsync(jtw);

            await jtw.FlushAsync();
            await sw.FlushAsync();

            return ms.ToArray();
        }

    }

}
