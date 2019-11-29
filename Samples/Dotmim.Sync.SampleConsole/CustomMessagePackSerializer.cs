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
        public ISerializer<T> GetSerializer<T>() => new CustomMessagePackSerializer<T>();
    }

    public class CustomMessagePackSerializer<T> : ISerializer<T>
    {

        public CustomMessagePackSerializer()
        {
            MessagePackSerializer.SetDefaultResolver(MessagePack.Resolvers.ContractlessStandardResolver.Instance);
        }
        public T Deserialize(Stream ms)
        {
            return MessagePackSerializer.Deserialize<T>(ms);
        }

        public byte[] Serialize(T obj)
        {
            return MessagePackSerializer.Serialize(obj);
        }
    }


    public class SerializableResolver : IFormatterResolver
    {
        public static readonly IFormatterResolver Instance = new SerializableResolver();
        SerializableResolver() { }

        public IMessagePackFormatter<T> GetFormatter<T>() => FormatterCache<T>.Formatter;

        static class FormatterCache<T>
        {
            public static readonly IMessagePackFormatter<T> Formatter;
            static FormatterCache() => Formatter = (IMessagePackFormatter<T>)SerializableFormatterHelper.GetFormatter<T>();
        }
    }

    internal static class SerializableFormatterHelper
    {
        internal static object GetFormatter<T>()
        {
            return typeof(Exception).IsAssignableFrom(typeof(T)) ? new SerializableFormatter<T>() : null;
        }
    }

    public class SerializableFormatter<T> : IMessagePackFormatter<T>
    {
        private static readonly IMessagePackFormatter<object> ObjectFormatter = TypelessFormatter.Instance;

        public int Serialize(ref byte[] bytes, int offset, T value, IFormatterResolver formatterResolver)
        {
            if (value == null)
            {
                return MessagePackBinary.WriteNil(ref bytes, offset);
            }

            var startOffset = offset;

            var serializable = value as ISerializable;
            var serializationInfo = new SerializationInfo(value.GetType(), new FormatterConverter());
            serializable.GetObjectData(serializationInfo, new StreamingContext());

            offset += MessagePackBinary.WriteMapHeader(ref bytes, offset, serializationInfo.MemberCount);
            foreach (var info in serializationInfo)
            {
                offset += MessagePackBinary.WriteString(ref bytes, offset, info.Name);
                offset += ObjectFormatter.Serialize(ref bytes, offset, info.Value, formatterResolver);
            }

            return offset - startOffset;
        }

        public T Deserialize(byte[] bytes, int offset, IFormatterResolver formatterResolver, out int readSize)
        {
            if (MessagePackBinary.IsNil(bytes, offset))
            {
                readSize = 1;
                return default(T);
            }

            int startOffset = offset;
            var serializationInfo = new SerializationInfo(typeof(T), new FormatterConverter());

            var len = MessagePackBinary.ReadMapHeader(bytes, offset, out readSize);
            offset += readSize;

            for (int i = 0; i < len; i++)
            {
                var key = MessagePackBinary.ReadString(bytes, offset, out readSize);
                offset += readSize;
                var val = ObjectFormatter.Deserialize(bytes, offset, formatterResolver, out readSize);
                offset += readSize;

                serializationInfo.AddValue(key, val);
            }

            ISerializable obj = null;
            ConstructorInfo constructorInfo = typeof(T).GetConstructor(
                BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance,
                null,
                new[] { typeof(SerializationInfo), typeof(StreamingContext) },
                null);

            if (constructorInfo != null)
            {
                object[] args = { serializationInfo, new StreamingContext() };
                obj = constructorInfo.Invoke(args) as ISerializable;
            }

            readSize = offset - startOffset;
            return (T)obj;
        }
    }
}





