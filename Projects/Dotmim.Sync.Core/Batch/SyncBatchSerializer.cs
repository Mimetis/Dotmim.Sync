using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.Core.Batch
{
    public abstract class SyncBatchSerializer
    {
        public abstract void Initialize(string batchFileName);
        public abstract SyncBatchInfo Deserialize(bool deserializeData = true);
        public abstract void Serialize(SyncBatchInfo batchInfo);
    }
}