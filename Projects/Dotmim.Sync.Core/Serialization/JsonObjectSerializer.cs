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

    public class JsonObjectSerializerFactory : ISerializerFactory
    {
        public string Key => "json";

        public ISerializer GetSerializer() => new JsonObjectSerializer();

    }

    public class JsonObjectSerializer : ISerializer
    {
        public async Task<T> DeserializeAsync<T>(Stream ms) => (T)await DeserializeAsync(ms, typeof(T)).ConfigureAwait(false);

        public Task<byte[]> SerializeAsync<T>(T obj)=> SerializeAsync((object)obj);

        public async Task<object> DeserializeAsync(Stream ms, Type type)
        {
            using var sr = new StreamReader(ms);
            using var jtr = new JsonTextReader(sr);

            var jobject = await JObject.LoadAsync(jtr).ConfigureAwait(false);

            return jobject.ToObject(type);
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
            await jobject.WriteToAsync(jtw).ConfigureAwait(false);

            await jtw.FlushAsync().ConfigureAwait(false);
            await sw.FlushAsync().ConfigureAwait(false);

            return ms.ToArray();
        }


    }


}
