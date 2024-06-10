using Newtonsoft.Json;
using System;
using System.IO;
using System.Threading.Tasks;

namespace Dotmim.Sync.Serialization
{
    public class JsonObjectSerializerFactory : ISerializerFactory
    {
        private static readonly JsonObjectSerializer _jsonSerializer = new();

        public string Key => "json";

        public ISerializer GetSerializer() => _jsonSerializer;
    }

    public class JsonObjectSerializer : ISerializer
    {
        private static readonly JsonSerializer jsonSerializer = new();

#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
        public async Task<T> DeserializeAsync<T>(Stream ms)
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
        {
            using var sr = new StreamReader(ms);
            using var jtr = new JsonTextReader(sr);
            return jsonSerializer.Deserialize<T>(jtr);
        }

        public async Task<byte[]> SerializeAsync<T>(T obj)
        {
            using var ms = new MemoryStream();
            using var sw = new StreamWriter(ms);
            using var jtw = new JsonTextWriter(sw);

//#if DEBUG
//            jtw.Formatting = Formatting.Indented;
//#endif
            jsonSerializer.Serialize(jtw, obj);

            await jtw.FlushAsync().ConfigureAwait(false);
            await sw.FlushAsync().ConfigureAwait(false);

            return ms.ToArray();
        }

#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
        public async Task<object> DeserializeAsync(Stream ms, Type type)
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
        {
            using var sr = new StreamReader(ms);
            using var jtr = new JsonTextReader(sr);
            return jsonSerializer.Deserialize(jtr, type);
        }
    }
}