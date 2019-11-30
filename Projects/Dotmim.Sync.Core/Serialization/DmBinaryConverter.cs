using System.IO;

namespace Dotmim.Sync.Serialization
{
    public class DmBinaryConverter<T> : ISerializer<T>
    {
        public string key => "dmbin";

        DmSerializer serializer;

        public DmBinaryConverter()
        {
            serializer = new DmSerializer();
        }

        public T Deserialize(Stream ms)
        {
            return serializer.Deserialize<T>(ms);
        }

        public byte[] Serialize(T obj)
        {
            return serializer.Serialize(obj);
        }
    }
}
