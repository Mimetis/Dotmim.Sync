using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync
{
    // TODO : Should we implement this ?
    // IF Yes, it will add more in memory data (as we need to get the data, then create a memory stream after intercept to get the real data from serializer)
    public class HttpGettingDataArgs : ProgressArgs
    {
        public HttpGettingDataArgs(byte[] content, SyncContext context, string host) : base(context, null)
        {
            this.Content = content;
            this.Host = host;
        }
        public override int EventId => HttpSyncEventsId.HttpGettingSchema.Id;
        public override string Message => $"[{this.Host}] Getting Data.";
        public byte[] Content { get; }
        public string Host { get; }
    }
    public class HttpSendingDataArgs : ProgressArgs
    {
        public HttpSendingDataArgs(byte[] content, SyncContext context, string host) : base(context, null)
        {
            this.Content = content;
            this.Host = host;
        }
        public override int EventId => HttpSyncEventsId.HttpGettingSchema.Id;
        public override string Message => $"[{this.Host}] Sending Data.";
        public byte[] Content { get; }
        public string Host { get; }
    }
}
