using System;
using System.Collections.Generic;
using System.Data.Common;
using System.IO;
using System.Text;

namespace Dotmim.Sync
{
    /// <summary>
    /// Raise before serialize a change set to get a byte array
    /// </summary>
    public class SerializingSetArgs : ProgressArgs
    {
        public SerializingSetArgs(SyncContext context, ContainerSet set, string fileName, string directoryPath) : base(context, null, null)
        {
            this.Set = set;
            this.FileName = fileName;
            this.DirectoryPath = directoryPath;
        }

        /// <summary>
        /// Gets or Sets byte array representing the Set to serialize to the disk. If the Result property is Null, Dotmim.Sync will serialized the container set using a simple Json converter
        /// </summary>
        public byte[] Result { get; set; }

        /// <summary>
        /// Container set to serialize
        /// </summary>
        public ContainerSet Set { get; }

        /// <summary>
        /// File name, where the content will be serialized
        /// </summary>
        public string FileName { get; }

        /// <summary>
        /// Directory containing the file, about to be serialized
        /// </summary>
        public string DirectoryPath { get; }
        public override int EventId => 33;
    }

    /// <summary>
    /// Raise just after loading a binary change set from disk, just before calling the deserializer
    /// </summary>
    public class DeserializingSetArgs : ProgressArgs
    {
        public DeserializingSetArgs(SyncContext context, FileStream fileStream, string fileName, string directoryPath) : base(context, null, null)
        {
            this.FileStream = fileStream;
            this.FileName = fileName;
            this.DirectoryPath = directoryPath;
        }

        /// <summary>
        /// Gets the Filestream to deserialize
        /// </summary>
        public FileStream FileStream { get; }

        /// <summary>
        /// File name containing the set to be deserialized
        /// </summary>
        public string FileName { get; }

        /// <summary>
        /// Directory containing the file, about to be deserialized
        /// </summary>
        public string DirectoryPath { get; }


        /// <summary>
        /// Gets or Sets the container set result, after having deserialized the FileStream. If the Result property is Null, Dotmim.Sync will deserialized the stream using a simple Json converter
        /// </summary>
        public ContainerSet Result { get; set; }

        public override int EventId => 34;
    }


    public static partial class InterceptorsExtensions
    {

        /// <summary>
        /// Occurs just before saving a serialized set to disk
        /// </summary>
        public static void OnSerializingSet(this BaseOrchestrator orchestrator, Action<SerializingSetArgs> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Occurs just after loading a serialized set from disk
        /// </summary>
        public static void OnDeserializingSet(this BaseOrchestrator orchestrator, Action<DeserializingSetArgs> action)
            => orchestrator.SetInterceptor(action);


    }
}
