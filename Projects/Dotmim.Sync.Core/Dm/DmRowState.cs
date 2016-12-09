using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.Data
{
    [Flags]
    public enum DmRowState
    {
        // The row has been created but is not part of any DataRowCollection.
        // A DataRow is in this state immediately after it has been created and 
        // before it is added to a collection, or if it has been removed from a collection.
        Detached = 0,
        // The row has not changed since AcceptChanges was last called.
        Unchanged = 1,
        // The row was added to a DataRowCollection, and AcceptChanges has not been called.
        Added = 2,
        // The row was modified and AcceptChanges has not been called.
        Modified = 4,
        // The row was deleted using the Delete method of the DataRow.
        Deleted = 8
    }
}
