using Dotmim.Sync.Data;
using Dotmim.Sync.Enumerations;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Messages
{
    /// <summary>
    /// Message exchanged during the Ensure Schema sync stage
    /// </summary>
    [Serializable]
    public class MessageEnsureSchema
    {
        public MessageEnsureSchema(SyncSchema schema)
        {
            this.Schema = schema ?? throw new ArgumentNullException(nameof(schema));
        }

        /// <summary>
        /// Gets or Sets the database schema
        /// </summary>
        public SyncSchema Schema { get; set; }

    }
}
