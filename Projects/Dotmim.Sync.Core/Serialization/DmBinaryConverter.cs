using DmBinaryFormatter;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Dotmim.Sync.Core.Serialization
{
    public class DmBinaryConverter<T> : BaseConverter<T>
    {
        DmSerializer serializer;

        public DmBinaryConverter()
        {
            serializer = new DmSerializer();
        }

        public override T Deserialize(Stream ms)
        {
            return serializer.Deserialize<T>(ms);
        }

        public override void Serialize(T obj, Stream ms)
        {
            serializer.Serialize(obj, ms);
        }
        public override byte[] Serialize(T obj)
        {
            return serializer.Serialize(obj);
        }
    }
}
