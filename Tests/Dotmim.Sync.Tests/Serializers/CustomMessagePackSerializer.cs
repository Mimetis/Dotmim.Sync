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
            var val = (T)MessagePackSerializer.Typeless.Deserialize(ms, MessagePackSerializerOptions.Standard.WithResolver(ContractlessStandardResolver.Instance));
            return val;
        }

        public Task<byte[]> SerializeAsync<T>(T obj) => SerializeAsync((object)obj);


        public async Task<object> DeserializeAsync(Stream ms, Type type)
        {
            var val = await MessagePackSerializer.Typeless.DeserializeAsync(ms, MessagePackSerializerOptions.Standard.WithResolver(ContractlessStandardResolver.Instance));
            var val2 = Convert.ChangeType(val, type);
            return val2;
        }

        public async Task<byte[]> SerializeAsync(object obj)
        {
            using var ms = new MemoryStream();
            await MessagePackSerializer.Typeless.SerializeAsync(ms, obj, MessagePackSerializerOptions.Standard.WithResolver(ContractlessStandardResolver.Instance));
            //await MessagePackSerializer.Typeless.SerializeAsync(ms, obj, MessagePackSerializerOptions.Standard.WithResolver(ContractlessStandardResolver.Instance));
            //await MessagePackSerializer.SerializeAsync(ms, obj, MessagePackSerializerOptions.Standard.WithResolver(ContractlessStandardResolver.Instance));

            return ms.ToArray();
        }
    }
}





