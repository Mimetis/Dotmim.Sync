using Dotmim.Sync.Data;
using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Messages
{
    /// <summary>
    /// Message exchanged during the Ensure Schema sync stage
    /// </summary>
    public class MessageEnsureSchema
    {
        /// <summary>
        /// Gets or Sets the database schema
        /// </summary>
        public DmSet Schema { get; set; }

    }
}
