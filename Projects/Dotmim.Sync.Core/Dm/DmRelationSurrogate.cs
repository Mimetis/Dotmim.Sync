using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Data.Surrogate
{
    [Serializable]
    public class DmRelationSurrogate
    {
        /// <summary>
        /// Gets or Sets an array of DmColumnSurrogate objects that represent the parent key.
        /// </summary>
        public DmColumnSurrogate[] ParentKeySurrogates { get; set; }

        /// <summary>
        /// Gets ro Sets an array of DmColumnSurrogate objects that represent the parent key.
        /// </summary>
        public DmColumnSurrogate[] ChildKeySurrogates { get; set; }

        /// <summary>
        /// Gets or Sets the relation name 
        /// </summary>
        public string RelationName { get; set; }

    }
}
