using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.Data
{
    public enum DmRowVersion
    {
        Original = 0x0100,
        Current = 0x0200,
        Proposed = 0x0400,
        Default = Proposed | Current,
    }
}
