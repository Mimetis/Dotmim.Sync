using Dotmim.Sync.Serialization;
using MessagePack;
using MessagePack.Resolvers;
using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.SampleConsole
{
    public class CustomMessagePackSerializerFactory : ISerializerFactory
    {
        public string Key => "mpack";
        public ISerializer GetSerializer() => new CustomMessagePackSerializer();
    }

    public class CustomMessagePackSerializer : ISerializer
    {
        public CustomMessagePackSerializer() { }

        public async Task<T> DeserializeAsync<T>(Stream ms) => (T)await DeserializeAsync(ms, typeof(T)).ConfigureAwait(false);

        public Task<byte[]> SerializeAsync<T>(T obj) => SerializeAsync((object)obj);

        public async Task<T> DeserializeAsync<T>(string value)
        {
            var bytes = Encoding.UTF8.GetBytes(value);
            await using var ms = new MemoryStream(bytes);
            return (T)await DeserializeAsync(ms, typeof(T)).ConfigureAwait(false);
        }

        public async Task<object> DeserializeAsync(Stream ms, Type type)
        {
            var val = await MessagePackSerializer.DeserializeAsync(type, ms, MessagePackSerializerOptions.Standard.WithResolver(ContractlessStandardResolver.Instance));
            return val;
        }

        public async Task<byte[]> SerializeAsync(object obj)
        {
            using var ms = new MemoryStream();
            await MessagePackSerializer.SerializeAsync(ms, obj, MessagePackSerializerOptions.Standard.WithResolver(ContractlessStandardResolver.Instance));

            return ms.ToArray();
        }

        public T Deserialize<T>(string value)
        {
            var bytes = Encoding.UTF8.GetBytes(value);
            using var ms = new MemoryStream(bytes);
            var val = MessagePackSerializer.Deserialize(typeof(T), ms, MessagePackSerializerOptions.Standard.WithResolver(ContractlessStandardResolver.Instance));
            return (T)val;
        }

        public byte[] Serialize<T>(T obj)
        {
            using var ms = new MemoryStream();
            MessagePackSerializer.Serialize(ms, obj, MessagePackSerializerOptions.Standard.WithResolver(ContractlessStandardResolver.Instance));

            return ms.ToArray();
        }
    }
}





