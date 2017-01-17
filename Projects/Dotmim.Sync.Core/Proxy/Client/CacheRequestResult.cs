//using System;
//using System.Collections.Generic;
//using System.Text;

//namespace Dotmim.Sync.Core.Proxy.Client
//{
//    /// <summary>
//    /// Event args for the CacheRequestHandler.ProcessCacheRequestAsync call.
//    /// </summary>
//    public class CacheRequestResult
//    {
//        public Guid Id { get; set; }
//        public ChangeSet ChangeSet { get; set; }
//        public ChangeSetResponse ChangeSetResponse { get; set; }
//        public Exception Error { get; set; }
//        public Object State { get; set; }

//        /// <summary>
//        /// Get the state of the Http request response
//        /// </summary>
//        public HttpState HttpStep { get; set; }

//        public uint BatchUploadCount { get; set; }

//        public CacheRequestResult(Guid id, ChangeSetResponse response, int uploadCount,
//                                        Exception error, HttpState step, object state)
//        {
//            this.ChangeSetResponse = response;
//            this.Error = error;
//            this.State = state;
//            this.HttpStep = step;
//            this.Id = id;
//            this.BatchUploadCount = (uint)uploadCount;

//            // Check that error is carried over to the response
//            if (this.Error == null) return;

//            if (this.ChangeSetResponse == null)
//                this.ChangeSetResponse = new ChangeSetResponse();

//            this.ChangeSetResponse.Error = this.Error;
//        }

//        public CacheRequestResult(Guid id, ChangeSet changeSet, Exception error, HttpState step, object state)
//        {
//            this.ChangeSet = changeSet;
//            this.Error = error;
//            this.State = state;
//            this.Id = id;
//            this.HttpStep = step;
//        }
//    }
//}
