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
    public class SerializingSetArgs : ProgressArgs
    {
        public SerializingSetArgs(SyncContext context, ContainerSet set, ISerializerFactory serializerFactory, string fileName, string directoryPath) : base(context, null, null)
        {
            this.Set = set;
            this.SerializerFactory = serializerFactory;
            this.FileName = fileName;
            this.DirectoryPath = directoryPath;
        }
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;

        /// <summary>
        /// Gets or Sets byte array representing the Set to serialize to the disk. If the Result property is Null, Dotmim.Sync will serialized the container set using the serializer factory configured in the SyncOptions instance
        /// </summary>
        public byte[] Result { get; set; }

        /// <summary>
        /// Container set to serialize
        /// </summary>
        public ContainerSet Set { get; }
        
        /// <summary>
        /// Gets or Sets the serializer factory used to serialize the ContainerSet
        /// </summary>
        public ISerializerFactory SerializerFactory { get; }

        /// <summary>
        /// File name, where the content will be serialized
        /// </summary>
        public string FileName { get; }

        /// <summary>
        /// Directory containing the file, about to be serialized
        /// </summary>
        public string DirectoryPath { get; }
        public override int EventId => SyncEventsId.SerializingSet.Id;

        public override string Source => String.IsNullOrEmpty(DirectoryPath) ? "" : new DirectoryInfo(DirectoryPath).Name;
        public override string Message => $"[{FileName}] Serializing Set.";

    }

    /// <summary>
    /// Raise just after loading a binary change set from disk, just before calling the deserializer
    /// </summary>
    public class DeserializingSetArgs : ProgressArgs
    {
        public DeserializingSetArgs(SyncContext context, FileStream fileStream, ISerializerFactory serializerFactory, string fileName, string directoryPath) : base(context, null, null)
        {
            this.FileStream = fileStream;
            this.SerializerFactory = serializerFactory;
            this.FileName = fileName;
            this.DirectoryPath = directoryPath;
        }
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;

        /// <summary>
        /// Gets the Filestream to deserialize
        /// </summary>
        public FileStream FileStream { get; }

        /// <summary>
        /// Gets or Sets the serializer factory used to deserialize the filestream
        /// </summary>
        public ISerializerFactory SerializerFactory { get; }

        /// <summary>
        /// File name containing the set to be deserialized
        /// </summary>
        public string FileName { get; }

        /// <summary>
        /// Directory containing the file, about to be deserialized
        /// </summary>
        public string DirectoryPath { get; }

        public override string Source => String.IsNullOrEmpty(DirectoryPath) ? "" : new DirectoryInfo(DirectoryPath).Name;
        public override string Message => $"[{FileName}] Deserializing Set.";

        /// <summary>
        /// Gets or Sets the container set result, after having deserialized the FileStream. If the Result property is Null, Dotmim.Sync will deserialized the stream using a simple Json converter
        /// </summary>
        public ContainerSet Result { get; set; }

        public override int EventId => SyncEventsId.DeserializingSet.Id;
    }


    public static partial class InterceptorsExtensions
    {

        /// <summary>
        /// Occurs just before saving a serialized set to disk
        /// </summary>
        public static void OnSerializingSet(this BaseOrchestrator orchestrator, Action<SerializingSetArgs> action)
            => orchestrator.SetInterceptor(action);
        public static void OnSerializingSet(this BaseOrchestrator orchestrator, Func<SerializingSetArgs, Task> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Occurs just after loading a serialized set from disk
        /// </summary>
        public static void OnDeserializingSet(this BaseOrchestrator orchestrator, Action<DeserializingSetArgs> action)
            => orchestrator.SetInterceptor(action);
        public static void OnDeserializingSet(this BaseOrchestrator orchestrator, Func<DeserializingSetArgs, Task> action)
            => orchestrator.SetInterceptor(action);


    }
    public static partial class SyncEventsId
    {
        public static EventId SerializingSet => CreateEventId(8000, nameof(SerializingSet));
        public static EventId DeserializingSet => CreateEventId(8050, nameof(DeserializingSet));

    }
}
