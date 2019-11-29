using System.IO;

namespace Dotmim.Sync.Serialization
{

    public interface ISerializerFactory
    {
        ISerializer<T> GetSerializer<T>();
    }

    public interface ISerializer<T>
    {
        T Deserialize(Stream ms);
        byte[] Serialize(T obj);
    }
}