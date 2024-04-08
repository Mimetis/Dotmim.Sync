using System;
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

        ISerializer GetSerializer();
    }


    /// <summary>
    /// Represents a generic serializer for a defined type
    /// </summary>
    public interface ISerializer
    {
        Task<object> DeserializeAsync(Stream ms, Type type);

        Task<T> DeserializeAsync<T>(Stream ms);

        T Deserialize<T>(string value);

        Task<byte[]> SerializeAsync<T>(T obj);

        byte[] Serialize<T>(T obj);
    }
}