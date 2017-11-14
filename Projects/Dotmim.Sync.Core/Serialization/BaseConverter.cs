using Dotmim.Sync.Enumerations;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Dotmim.Sync.Serialization
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
                case SerializationFormat.Binary:
                    return new BinaryConverter<T>();
                case SerializationFormat.Custom:
                    return new DmBinaryConverter<T>();
            }

            throw new Exception("Cant get Converter");
        }


    }
}
