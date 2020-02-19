using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;

namespace Dotmim.Sync
{
    /// <summary>
    /// Raise before serialize a change set to get a byte array
    /// </summary>
    public class SerializingSetArgs : ProgressArgs
    {
        public SerializingSetArgs(SyncContext context,  ContainerSet set, DbConnection connection, DbTransaction transaction)
            : base(context, connection, transaction)
        {
            this.Set = set;
        }

        /// <summary>
        /// Gets the data that will be serialized on disk
        /// </summary>
        public byte[] Data { get; set; }

        /// <summary>
        /// Set to serialize
        /// </summary>
        public ContainerSet Set { get; }
    }


    /// <summary>
    /// Raise just after loading a binary change set from disk, just before calling the deserializer
    /// </summary>
    public class DeserializingSetArgs : SerializingSetArgs
    {
        public DeserializingSetArgs(SyncContext context, ContainerSet set, DbConnection connection, DbTransaction transaction) : base(context, set, connection, transaction)
        {
        }
    }
}
