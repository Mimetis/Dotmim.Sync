using Dotmim.Sync.Data;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;

namespace Dotmim.Sync.Manager
{
    public interface IDbManagerTable
    {
        string TableName { set; }
        DmTable GetTableDefinition();
        DmTable GetTableRelations();
    }
}
