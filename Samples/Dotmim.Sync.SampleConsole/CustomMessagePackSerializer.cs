using Dotmim.Sync.Serialization;
using MessagePack;
using MessagePack.Formatters;
using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Runtime.Serialization;
using System.Text;

namespace Dotmim.Sync.SampleConsole
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

        public T Deserialize(Stream ms) => MessagePackSerializer.Deserialize<T>(ms, options);
        public byte[] Serialize(T obj) => MessagePackSerializer.Serialize(obj, options);
    }
}





