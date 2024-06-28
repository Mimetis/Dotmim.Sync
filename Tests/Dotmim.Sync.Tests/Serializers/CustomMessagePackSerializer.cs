using Dotmim.Sync.Serialization;
using MessagePack;
using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.Tests.Serializers
{
    public class CustomMessagePackSerializerFactory : ISerializerFactory
    {
        public string Key => "mpack";
        public ISerializer GetSerializer() => new CustomMessagePackSerializer();
    }

    public class CustomMessagePackSerializer : ISerializer
    {
        public CustomMessagePackSerializer() { }

#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
        public async Task<T> DeserializeAsync<T>(Stream ms)
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
        {
            var val = (T)MessagePackSerializer.Typeless.Deserialize(ms);
            return val;
        }

        public async Task<object> DeserializeAsync(Stream ms, Type type)
        {
            var val = await MessagePackSerializer.Typeless.DeserializeAsync(ms);
            var val2 = Convert.ChangeType(val, type);
            return val2;
        }

        public async Task<T> DeserializeAsync<T>(string value)
        {
            var bytes = Encoding.UTF8.GetBytes(value);
            await using var ms = new MemoryStream(bytes);
            return (T)MessagePackSerializer.Typeless.Deserialize(ms);
        }

        public Task<byte[]> SerializeAsync<T>(T obj)
        {
            var blob = MessagePackSerializer.Typeless.Serialize(obj);
            return Task.FromResult(blob);
        }

        public byte[] Serialize<T>(T obj)
        {
            var blob = MessagePackSerializer.Typeless.Serialize(obj);
            return blob;
        }

        public T Deserialize<T>(string value)
        {
            var bytes = Encoding.UTF8.GetBytes(value);
            using var ms = new MemoryStream(bytes);
            return (T)MessagePackSerializer.Typeless.Deserialize(ms);
        }

        public byte[] Serialize(object obj, Type type)
        {
            var blob = MessagePackSerializer.Typeless.Serialize(obj);
            return blob;
        }

        public Task<byte[]> SerializeAsync(object obj, Type type)
        {
            var blob = MessagePackSerializer.Typeless.Serialize(obj);
            return Task.FromResult(blob);
        }
    }
}
