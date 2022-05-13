using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Serialization;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    /// <summary>
    /// Raise before serialize a change set to get a byte array
    /// </summary>
    public class SerializingRowArgs : ProgressArgs
    {
        public SerializingRowArgs(SyncContext context, SyncTable schemaTable, object[] rowArray) : base(context, null, null)
        {
            this.SchemaTable = schemaTable;
            this.RowArray = rowArray;
        }
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;

        /// <summary>
        /// Gets or Sets the result string that will be serialized in the json stream
        /// </summary>
        public string Result { get; set; }

        /// <summary>
        /// Gets the schema table, corresponding to the row array ObjectArray
        /// </summary>
        public SyncTable SchemaTable { get; }

        /// <summary>
        /// Gets the row array to serialize
        /// </summary>
        public object[] RowArray { get; }


        public override int EventId => SyncEventsId.SerializingSyncRow.Id;

    }

    /// <summary>
    /// Raise just after loading a binary change set from disk, just before calling the deserializer
    /// </summary>
    public class DeserializingRowArgs : ProgressArgs
    {
        public DeserializingRowArgs(SyncContext context, SyncTable schemaTable, string rowString) : base(context, null, null)
        {
            this.SchemaTable = schemaTable;
            this.RowString = rowString;
        }
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;

        public object[] Result { get; set; }
        public override int EventId => SyncEventsId.DeserializingSyncRow.Id;

        public SyncTable SchemaTable { get; }
        public string RowString { get; }
    }


    public static partial class InterceptorsExtensions
    {

        /// <summary>
        /// Occurs just before serializing a SyncRow in a json stream
        /// </summary>
        public static void OnSerializingSyncRow(this BaseOrchestrator orchestrator, Action<SerializingRowArgs> action)
            => orchestrator.SetInterceptor(action);
        /// <summary>
        /// Occurs just before serializing a SyncRow in a json stream
        /// </summary>
        public static void OnSerializingSyncRow(this BaseOrchestrator orchestrator, Func<SerializingRowArgs, Task> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Occurs just after loading a serialized SyncRow from a json stream
        /// </summary>
        public static void OnDeserializingSyncRow(this BaseOrchestrator orchestrator, Action<DeserializingRowArgs> action)
            => orchestrator.SetInterceptor(action);
        /// <summary>
        /// Occurs just after loading a serialized SyncRow from a json stream
        /// </summary>
        public static void OnDeserializingSyncRow(this BaseOrchestrator orchestrator, Func<DeserializingRowArgs, Task> action)
            => orchestrator.SetInterceptor(action);


    }
    public static partial class SyncEventsId
    {
        public static EventId SerializingSyncRow => CreateEventId(8000, nameof(SerializingSyncRow));
        public static EventId DeserializingSyncRow => CreateEventId(8050, nameof(DeserializingSyncRow));

    }
}