using Dotmim.Sync.Core.Enumerations;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Dotmim.Sync.Core.Serialization
{
    public abstract class BaseConverter<T>
    {
        public abstract void Serialize(T obj, Stream ms);
        public abstract T Deserialize(Stream ms);
        public abstract byte[] Serialize(T obj);

        public static BaseConverter<T> GetConverter(SerializationFormat serializationFormat)
        {
            switch (serializationFormat)
            {
                case SerializationFormat.Json:
                    return new JsonConverter<T>();
                case SerializationFormat.DmSerializer:
                    return new DmBinaryConverter<T>();
                case SerializationFormat.Custom:
                    return null;
            }

            throw new Exception("Cant get Converter");
        }


    }
}
