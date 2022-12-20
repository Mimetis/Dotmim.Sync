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

        ISerializer<T> GetSerializer<T>();
        ISerializer GetSerializer(Type objectType);
    }

    /// <summary>
    /// Represents a generic serializer for a defined type of T
    /// </summary>
    public interface ISerializer<T>
    {
        Task<T> DeserializeAsync(Stream ms);
        Task<byte[]> SerializeAsync(T obj);
    }

    /// <summary>
    /// Represents a generic serializer for a defined type
    /// </summary>
    public interface ISerializer
    {
        Task<object> DeserializeAsync(Stream ms);
        Task<byte[]> SerializeAsync(object obj);
    }

    /// <summary>
    /// Represents a generic serializer for a defined type
    /// </summary>
    public interface ISerializer2
    {
        Task<object> DeserializeAsync(Stream ms);
        Task<byte[]> SerializeAsync(object obj);
    }



}