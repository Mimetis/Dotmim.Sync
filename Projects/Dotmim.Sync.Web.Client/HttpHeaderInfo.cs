//using Dotmim.Sync.Batch;
//using Dotmim.Sync.Enumerations;
//using System;
//using System.Collections.Generic;
//using System.Text;

//namespace Dotmim.Sync.Web.Client
//{
//    public class HttpHeaderInfo
//    {
//        public HttpHeaderInfo()
//        {

//        }
//        public HttpHeaderInfo(SyncContext ctx)
//        {
//            this.SyncContext = ctx;
//        }
//        public bool IsLastBatch { get; set; }
//        public int BatchCount { get; set; }
//        public int BatchIndex { get; set; }
//        public int RowsCount { get; set; }
//        public HttpStep Step { get; set; }
//        public BatchPartTableInfo[] Tables { get; set; }
//        public SyncContext SyncContext { get; set; }
//        public long RemoteClientTimestamp { get; set; }
//        public DatabaseChangesSelected ServerChangesSelected { get; set; }
//        public DatabaseChangesApplied ClientChangesApplied { get; set; }
//        public ConflictResolutionPolicy ConflictResolutionPolicy { get; set; }

//    }
//}
