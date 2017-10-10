using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Enumerations
{
    public enum SyncDirection
    {
        /// <summary>
        /// Table will be sync from server to client and from client to server
        /// </summary>
        Bidirectional = 1,

        /// <summary>
        /// Table will be sync from server to client only
        /// Every changes on client won't be uploaded to server
        /// </summary>
        DownloadOnly = 2,

        /// <summary>
        /// Table will be sync from client to server only
        /// Every changes from server won't be downloaded to client
        /// </summary>
        UploadOnly = 3

    }
}