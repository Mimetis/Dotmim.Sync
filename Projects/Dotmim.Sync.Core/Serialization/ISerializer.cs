using System;
using System.IO;
using System.Threading.Tasks;

namespace Dotmim.Sync.Serialization
{

    /// <summary>
    /// Represents a factory of generic serializers.
    /// This object should be able to get a serializer of each type of T.
    /// </summary>
    public interface ISerializerFactory
    {
        /// <summary>
        /// Gets the key for the serializer.
        /// </summary>
        string Key { get; }

        /// <summary>
        /// Gets the serializer.
        /// </summary>
        /// <returns></returns>
        ISerializer GetSerializer();
    }

    /// <summary>
    /// Represents a generic serializer for a defined type.
    /// </summary>
    public interface ISerializer
    {
        /// <summary>
        /// Deserialize an object from a stream.
        /// </summary>
        Task<object> DeserializeAsync(Stream ms, Type type);

        /// <summary>
        /// Deserialize an object from a stream.
        /// </summary>
        Task<T> DeserializeAsync<T>(Stream ms);

        /// <summary>
        /// Deserialize an object from a string.
        /// </summary>
        T Deserialize<T>(string value);

        /// <summary>
        /// Serialize an object to a stream.
        /// </summary>
        Task<byte[]> SerializeAsync<T>(T obj);

        /// <summary>
        /// Serialize an object to a stream.
        /// </summary>
        Task<byte[]> SerializeAsync(object obj, Type type);

        /// <summary>
        /// Serialize an object to a byte array.
        /// </summary>
        byte[] Serialize(object obj, Type type);

        /// <summary>
        /// Serialize an object to a byte array.
        /// </summary>
        byte[] Serialize<T>(T obj);
    }
}