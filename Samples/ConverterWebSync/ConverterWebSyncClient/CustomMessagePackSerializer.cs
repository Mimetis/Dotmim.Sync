using Dotmim.Sync.Serialization;
using MessagePack;
using MessagePack.Resolvers;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace ConverterWebSyncClient
{
    public class CustomMessagePackSerializerFactory : ISerializerFactory
    {
        public string Key => "mpack";
        public ISerializer<T> GetSerializer<T>() => new CustomMessagePackSerializer<T>();
        public ISerializer GetSerializer(Type objectType) => new CustomMessagePackSerializer(objectType);
    }

    public class CustomMessagePackSerializer<T> : ISerializer<T>
    {
        public CustomMessagePackSerializer() { }

        public async Task<T> DeserializeAsync(Stream ms)
        {
            var val = await MessagePackSerializer.DeserializeAsync<T>(ms, MessagePackSerializerOptions.Standard.WithResolver(ContractlessStandardResolver.Instance));
            return val;
        }

        public async Task<byte[]> SerializeAsync(T obj)
        {
            using (var ms = new MemoryStream())
            {
                await MessagePackSerializer.SerializeAsync<T>(ms, obj, MessagePackSerializerOptions.Standard.WithResolver(ContractlessStandardResolver.Instance));

                return ms.ToArray();
            }
        }
    }
    public class CustomMessagePackSerializer : ISerializer
    {
        public CustomMessagePackSerializer(Type objectType)
        {
            this.ObjectType = objectType;
        }

        public Type ObjectType { get; }

        public async Task<object> DeserializeAsync(Stream ms)
        {
            var val = await MessagePackSerializer.DeserializeAsync(ObjectType, ms, MessagePackSerializerOptions.Standard.WithResolver(ContractlessStandardResolver.Instance)); ;
            return val;
        }

        public async Task<byte[]> SerializeAsync(object obj)
        {
            using var ms = new MemoryStream();
            await MessagePackSerializer.SerializeAsync(ObjectType, ms, obj, MessagePackSerializerOptions.Standard.WithResolver(ContractlessStandardResolver.Instance));

            return ms.ToArray();
        }
    }
}
