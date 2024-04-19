using System;
using System.IO;
using System.Text.Json;
using System.Text.Json.Serialization;
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
        private static readonly JsonSerializerOptions options = new()
        {
            TypeInfoResolver = System.Text.Json.Serialization.Metadata.DataContractResolver.Default,
        };

        public async Task<T> DeserializeAsync<T>(Stream ms) => await JsonSerializer.DeserializeAsync<T>(ms, options);

        public T Deserialize<T>(string value) => JsonSerializer.Deserialize<T>(value, options);

        public Task<byte[]> SerializeAsync<T>(T obj) => Task.FromResult(JsonSerializer.SerializeToUtf8Bytes(obj, options));
        
        public byte[] Serialize<T>(T obj) => JsonSerializer.SerializeToUtf8Bytes(obj, options);

        public byte[] Serialize(object obj, Type type) => JsonSerializer.SerializeToUtf8Bytes(obj, type, options);

        public async Task<object> DeserializeAsync(Stream ms, Type type) => await JsonSerializer.DeserializeAsync(ms, type, options);
    }
}