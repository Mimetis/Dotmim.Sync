using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Builders
{
    public class ParserString
    {
        public string QuotePrefix { get; set; } = "[";
        public string QuoteSuffix { get; set; } = "]";

        public string SchemaName { get; set; }
        public string ObjectName { get; set; }
        public string DatabaseName { get; set; }
        public string QuotedSchemaName { get; set; }
        public string QuotedObjectName { get; set; }
        public string QuotedDatabaseName { get; set; }

    }
}
