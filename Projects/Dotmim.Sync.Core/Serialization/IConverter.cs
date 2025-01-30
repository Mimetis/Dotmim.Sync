namespace Dotmim.Sync.Serialization
{
    /// <summary>
    /// Interface for converters.
    /// </summary>
    public interface IConverter
    {

        /// <summary>
        /// Gets the unique key for this converter.
        /// </summary>
        string Key { get; }

        /// <summary>
        /// Convert a row before being serialized.
        /// </summary>
        void BeforeSerialize(SyncRow row, SyncTable schemaTable);

        /// <summary>
        /// Convert a row afeter being deserialized.
        /// </summary>
        void AfterDeserialized(SyncRow row, SyncTable schemaTable);
    }
}