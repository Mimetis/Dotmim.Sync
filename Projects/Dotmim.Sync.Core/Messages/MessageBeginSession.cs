using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Messages
{
    /// <summary>
    /// Message exchanged during the Begin session sync stage
    /// </summary>
    public class MessageBeginSession
    {

        /// <summary>
        /// Gets or Sets the configuration, exchanged between the client and the server
        /// </summary>
        public SyncConfiguration SyncConfiguration { get; set; }

    }
}
