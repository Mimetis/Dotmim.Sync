using Dotmim.Sync.Batch;
using Dotmim.Sync.Builders;
using System;
using System.Collections.Generic;
using System.Text;
using Dotmim.Sync.Enumerations;
using System.Runtime.Serialization;
using System.Data.Common;

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
        public HttpMessageGetMoreChangesRequestArgs(byte[] content) : base(null, null, null)
            => this.Content = content;

        public byte[] Content { get; }
        public override int EventId => 51;
    }

    public class HttpMessageSendChangesRequestArgs : ProgressArgs
    {
        public HttpMessageSendChangesRequestArgs(byte[] content) : base(null, null, null)
            => this.Content = content;

        public byte[] Content { get; }
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
