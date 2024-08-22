namespace Dotmim.Sync.Serialization
{
    /// <summary>
    /// Collection of serializers. By default, only the Json serializer is available.
    /// </summary>
    public static class SerializersFactory
    {
        /// <summary>
        /// Gets get the default Json serializer.
        /// </summary>
        public static ISerializerFactory JsonSerializerFactory { get; } = new JsonObjectSerializerFactory();
    }
}