namespace Dotmim.Sync.Serialization
{
    internal class SerializerInfo
    {
        public SerializerInfo()
        {

        }
        public SerializerInfo(string serializerKey, int clientBatchSize)
        {
            this.SerializerKey = serializerKey;
            this.ClientBatchSize = clientBatchSize;
        }

        public string SerializerKey { get; set; }
        public int ClientBatchSize { get; set; }
    }
}