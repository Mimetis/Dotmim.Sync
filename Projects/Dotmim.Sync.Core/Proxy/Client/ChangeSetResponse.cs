using Dotmim.Sync.Core.Scope;
using Dotmim.Sync.Data;
using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Core.Proxy.Client
{
    /// <summary>
    /// Denotes a response for the a ChangeSet that was uploaded.
    /// </summary>
    public class ChangeSetResponse
    {
        DmSet _conflicts;
        DmSet _updatedItems;

        /// <summary>
        /// ScopeInfo
        /// </summary>
        public ScopeInfo ScopeInfo { get; set; }

        /// <summary>
        /// Any fatal/protocol related error encountered while applying the upload
        /// </summary>
        public Exception Error { get; set; }

        /// <summary>
        /// An collection of conflict objects
        /// </summary>
        public DmSet Conflicts
        {
            get
            {
                return _conflicts;
            }
        }

        /// <summary>
        /// A read only collection of Insert entities uploaded by clients that have been issued
        /// </summary>
        public DmSet UpdatedItems
        {
            get
            {
                return _updatedItems;
            }
        }

        internal ChangeSetResponse()
        {
            _conflicts = new DmSet();
            _updatedItems = new DmSet();
        }

       

    }
}
