using Dotmim.Sync.Serialization;
using MessagePack;
using MessagePack.Formatters;
using MessagePack.Resolvers;
using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Runtime.Serialization;
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

        public async Task<T> DeserializeAsync<T>(Stream ms)
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


        public Task<byte[]> SerializeAsync<T>(T obj)
        {
            var blob = MessagePackSerializer.Typeless.Serialize(obj);
            return Task.FromResult(blob);
        }



        public Task<byte[]> SerializeAsync(object obj)
        {
            var blob = MessagePackSerializer.Typeless.Serialize(obj);
            return Task.FromResult(blob);
        }
    }
}





