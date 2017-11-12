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
        /// Table will be sync from server to client only.
        /// All changes occured client won't be uploaded to server
        /// </summary>
        DownloadOnly = 2,

        /// <summary>
        /// Table will be sync from client to server only
        /// All changes from server won't be downloaded to client
        /// </summary>
        UploadOnly = 3
    }

    public enum SyncWay
    {
        /// <summary>
        /// No sync engaged
        /// </summary>
        None = 0,

        /// <summary>
        /// Sync is selecting then downloading changes from server
        /// </summary>
        Download = 1,

        /// <summary>
        /// Sync is selecting then uploading changes from client
        /// </summary>
        Upload = 2
    }
}