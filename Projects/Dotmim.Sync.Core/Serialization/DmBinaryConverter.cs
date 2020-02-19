using System.IO;
using System.Threading.Tasks;

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

        public Task<T> DeserializeAsync(Stream ms)
        {
            return Task.FromResult(serializer.Deserialize<T>(ms));
        }

        public Task<byte[]> SerializeAsync(T obj)
        {
            return Task.FromResult(serializer.Serialize(obj));
        }
    }
}
