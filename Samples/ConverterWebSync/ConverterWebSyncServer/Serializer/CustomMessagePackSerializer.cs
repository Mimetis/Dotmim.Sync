using Dotmim.Sync.Serialization;
using MessagePack;
using MessagePack.Resolvers;
using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace ConverterWebSyncServer.Serializer
{
    public class CustomMessagePackSerializerFactory : ISerializerFactory
    {
        public string Key => "mpack";

        public ISerializer GetSerializer() => new CustomMessagePackSerializer();
    }

    public class CustomMessagePackSerializer : ISerializer
    {
        private MessagePackSerializerOptions options = MessagePackSerializerOptions.Standard.WithResolver(ContractlessStandardResolver.Instance);

        public CustomMessagePackSerializer() { }

        public async Task<T> DeserializeAsync<T>(Stream ms) => (T)await this.DeserializeAsync(ms, typeof(T)).ConfigureAwait(false);

        public Task<byte[]> SerializeAsync<T>(T obj) => this.SerializeAsync((object)obj);

        public async Task<object> DeserializeAsync(Stream ms, Type type)
        {
            var val = await MessagePackSerializer.DeserializeAsync(type, ms, this.options).ConfigureAwait(false);
            return val;
        }

        public T Deserialize<T>(string value)
        {
            var bytes = Encoding.UTF8.GetBytes(value);
            using var ms = new MemoryStream(bytes);
            var val = MessagePackSerializer.Deserialize(typeof(T), ms, this.options);
            return (T)val;
        }

        public async Task<byte[]> SerializeAsync(object obj)
        {
            using var ms = new MemoryStream();
            await MessagePackSerializer.SerializeAsync(ms, obj, this.options).ConfigureAwait(false);
            return ms.ToArray();
        }

        public byte[] Serialize<T>(T obj)
        {
            using var ms = new MemoryStream();
            MessagePackSerializer.Serialize(ms, obj, this.options);
            return ms.ToArray();
        }

        public async Task<byte[]> SerializeAsync(object obj, Type type)
        {
            using var ms = new MemoryStream();
            await MessagePackSerializer.SerializeAsync(type, ms, obj, this.options).ConfigureAwait(false);
            return ms.ToArray();
        }

        public byte[] Serialize(object obj, Type type)
        {
            var b = MessagePackSerializer.Serialize(type, obj, this.options);
            return b;
        }
    }
}