namespace Dotmim.Sync.Web.Client
{
    public class HttpMessageSendChangesResponseArgs : ProgressArgs
    {
        public HttpMessageSendChangesResponseArgs(byte[] content) : base(null, null, null)
            => this.Content = content;

        public byte[] Content{ get; }
        public override int EventId => 50;
    }

    public class HttpMessageGetMoreChangesRequestArgs : ProgressArgs
    {
        public HttpMessageGetMoreChangesRequestArgs(HttpMessageGetMoreChangesRequest request) 
            : base(request.SyncContext, null, null)
        {
            this.Request = request;
        }

        public HttpMessageGetMoreChangesRequest Request { get; }
        public override int EventId => 51;
    }

    public class HttpMessageSendChangesRequestArgs : ProgressArgs
    {
        public HttpMessageSendChangesRequestArgs(HttpMessageSendChangesRequest request) 
            : base(request.SyncContext, null, null)
        {
            this.Request = request;
        }

        public HttpMessageSendChangesRequest Request { get; }
        public override int EventId => 52;
    }

    public class HttpMessageEnsureScopesResponseArgs : ProgressArgs
    {
        public HttpMessageEnsureScopesResponseArgs(byte[] content) : base(null, null, null)
            => this.Content = content;

        public byte[] Content { get; }
        public override int EventId => 53;
    }

    public class HttpMessageEnsureSchemaResponseArgs : ProgressArgs
    {
        public HttpMessageEnsureSchemaResponseArgs(byte[] content) : base(null, null, null)
            => this.Content = content;

        public byte[] Content { get; }
        public override int EventId => 54;
    }

    public class HttpMessageEnsureScopesRequestArgs : ProgressArgs
    {
        public HttpMessageEnsureScopesRequestArgs(byte[] content) : base(null, null, null)
            => this.Content = content;

        public byte[] Content { get; }
        public override int EventId => 55;
    }
}
