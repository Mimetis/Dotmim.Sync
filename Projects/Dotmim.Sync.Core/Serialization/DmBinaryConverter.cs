using System.IO;

namespace Dotmim.Sync.Serialization
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

        public override byte[] Serialize(T obj)
        {
            return serializer.Serialize(obj);
        }
    }
}
