using Dotmim.Sync.Enumerations;
using System;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    /// <summary>
    /// Event arg raised before serialize a change set to get a byte array.
    /// </summary>
    public class SerializingRowArgs : ProgressArgs
    {
        /// <inheritdoc cref="SerializingRowArgs" />
        public SerializingRowArgs(SyncContext context, SyncTable schemaTable, object[] rowArray)
            : base(context, null, null)
        {
            this.SchemaTable = schemaTable;
            this.RowArray = rowArray;
        }

        /// <summary>
        /// Gets or Sets the result string that will be serialized in the json stream.
        /// </summary>
        public string Result { get; set; }

        /// <summary>
        /// Gets the schema table, corresponding to the row array ObjectArray.
        /// </summary>
        public SyncTable SchemaTable { get; }

        /// <summary>
        /// Gets the row array to serialize.
        /// </summary>
#pragma warning disable CA1819 // Properties should not return arrays
        public object[] RowArray { get; }
#pragma warning restore CA1819 // Properties should not return arrays

        /// <inheritdoc cref="ProgressArgs.Message" />
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;

        /// <inheritdoc cref="ProgressArgs.EventId" />
        public override int EventId => 8000;
    }

    /// <summary>
    /// Event args raised just after loading a binary change set from disk, just before calling the deserializer.
    /// </summary>
    public class DeserializingRowArgs : ProgressArgs
    {
        /// <inheritdoc cref="DeserializingRowArgs" />
        public DeserializingRowArgs(SyncContext context, SyncTable schemaTable, string rowString)
            : base(context, null, null)
        {
            this.SchemaTable = schemaTable;
            this.RowString = rowString;
        }

        /// <summary>
        /// Gets or Sets the result array that will be deserialized from the json stream.
        /// </summary>
        public object[] Result { get; set; }

        /// <summary>
        /// Gets the schema table, corresponding to the row array objects.
        /// </summary>
        public SyncTable SchemaTable { get; }

        /// <summary>
        /// Gets the row string to deserialize.
        /// </summary>
        public string RowString { get; }

        /// <inheritdoc cref="ProgressArgs.Message" />
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;

        /// <inheritdoc cref="ProgressArgs.EventId" />
        public override int EventId => 8050;
    }

    /// <summary>
    /// Interceptor extension methods.
    /// </summary>
    public partial class InterceptorsExtensions
    {

        /// <summary>
        /// Occurs just before serializing a SyncRow in a json stream.
        /// </summary>
        public static Guid OnSerializingSyncRow(this BaseOrchestrator orchestrator, Action<SerializingRowArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Occurs just before serializing a SyncRow in a json stream.
        /// </summary>
        public static Guid OnSerializingSyncRow(this BaseOrchestrator orchestrator, Func<SerializingRowArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Occurs just after loading a serialized SyncRow from a json stream.
        /// </summary>
        public static Guid OnDeserializingSyncRow(this BaseOrchestrator orchestrator, Action<DeserializingRowArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Occurs just after loading a serialized SyncRow from a json stream.
        /// </summary>
        public static Guid OnDeserializingSyncRow(this BaseOrchestrator orchestrator, Func<DeserializingRowArgs, Task> action)
            => orchestrator.AddInterceptor(action);
    }
}