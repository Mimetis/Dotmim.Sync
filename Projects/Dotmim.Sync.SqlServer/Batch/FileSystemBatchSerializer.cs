using Dotmim.Sync.Core.Batch;
using Dotmim.Sync.Core.Serialization;
using Dotmim.Sync.Data;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.SqlServer.Batch
{
    public class FileSystemBatchSerializer : SyncBatchSerializer
    {
        string batchFileName;

        public override SyncBatchInfo Deserialize(bool deserializeData = true)
        {
            DmBinaryConverter<SyncBatchInfo> converter = new DmBinaryConverter<SyncBatchInfo>();

            SyncBatchInfo dbSyncBatchInfo = null;
            using (FileStream fileStream = new FileStream(batchFileName, FileMode.Open, FileAccess.ReadWrite))
            {
                try
                {
                    dbSyncBatchInfo = converter.Deserialize(fileStream);

                    //if (deserializeData)
                    //    dbSyncBatchInfo.DmSet = converter.Deserialize<DmSet>(dbSyncBatchInfo.DmSet);
                }
                catch
                {

                }
            }
            return dbSyncBatchInfo;
        }

        public override void Initialize(string batchFileName)
        {
            this.batchFileName = batchFileName;
        }

        public override void Serialize(SyncBatchInfo batchInfo)
        {
            DmBinaryConverter<SyncBatchInfo> converter = new DmBinaryConverter<SyncBatchInfo>();
            using (FileStream fileStream = new FileStream(batchFileName, FileMode.Create, FileAccess.ReadWrite))
            {
                try
                {
                    converter.Serialize(batchInfo, fileStream);
                }
                catch
                {
                    
                }
            }
        }
    }
}
