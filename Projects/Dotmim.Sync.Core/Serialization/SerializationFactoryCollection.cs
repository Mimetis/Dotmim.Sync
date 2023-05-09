namespace Dotmim.Sync.Serialization
{
    public static class SerializersCollection
    {
        /// <summary>
        /// Get the default Json serializer
        /// </summary>
        public static ISerializerFactory JsonSerializerFactory { get; } = new JsonObjectSerializerFactory();
    }
}
