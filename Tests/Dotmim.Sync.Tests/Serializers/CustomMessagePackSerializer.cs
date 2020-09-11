using Dotmim.Sync.Serialization;
using MessagePack;
using MessagePack.Formatters;
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
        public ISerializer<T> GetSerializer<T>() => new CustomMessagePackSerializer<T>();
    }

    public class CustomMessagePackSerializer<T> : ISerializer<T>
    {
        private MessagePackSerializerOptions options;

        public CustomMessagePackSerializer() => this.options = MessagePack.Resolvers.ContractlessStandardResolver.Options;

        public async Task<T> DeserializeAsync(Stream ms)
        {
            var t = await MessagePackSerializer.DeserializeAsync<T>(ms, options);

            return t;

        }
        public async Task<byte[]> SerializeAsync(T obj)
        {
            using (var ms = new MemoryStream())
            {
                await MessagePackSerializer.SerializeAsync(ms, obj, options);
                return ms.ToArray();
            }
        }
    }
}





