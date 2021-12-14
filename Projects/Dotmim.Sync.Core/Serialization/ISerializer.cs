using System.IO;
using System.Runtime.Serialization;
using System.Threading.Tasks;

namespace Dotmim.Sync.Serialization
{

    /// <summary>
    /// Represents a factory of generic serializers.
    /// This object should be able to get a serializer of each type of T
    /// </summary>
    public interface ISerializerFactory
    {
        string Key { get; }

        ISerializer<T> GetSerializer<T>();
    }

    /// <summary>
    /// Represents a generic serializer for a defined type of T
    /// </summary>
    public interface ISerializer<T>
    {
        Task<T> DeserializeAsync(Stream ms);
        Task<byte[]> SerializeAsync(T obj);
    }


}