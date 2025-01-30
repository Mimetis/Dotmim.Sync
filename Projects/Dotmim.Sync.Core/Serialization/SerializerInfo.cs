namespace Dotmim.Sync.Serialization
{

    /// <summary>
    /// Serializer info used to store the serializer key and the client batch size when using a custom serializer.
    /// </summary>
    internal class SerializerInfo
    {

        /// <summary>
        /// Initializes a new instance of the <see cref="SerializerInfo"/> class.
        /// </summary>
        public SerializerInfo()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SerializerInfo"/> class.
        /// </summary>
        public SerializerInfo(string serializerKey, int clientBatchSize)
        {
            this.SerializerKey = serializerKey;
            this.ClientBatchSize = clientBatchSize;
        }

        /// <summary>
        /// Gets or sets the serializer key.
        /// </summary>
        public string SerializerKey { get; set; }

        /// <summary>
        /// Gets or sets the client batch size.
        /// </summary>
        public int ClientBatchSize { get; set; }
    }
}