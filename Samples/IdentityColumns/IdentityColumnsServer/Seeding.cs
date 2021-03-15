using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace IdentityColumnsServer
{
    public class Seeding
    {
        public Guid ClientScopeId { get; set; }
        public string TableName { get; set; }
        public string SchemaName { get; set; }
        public int Seed { get; set; }
        public int Step { get; set; }

    }
}
