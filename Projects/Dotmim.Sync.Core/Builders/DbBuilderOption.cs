using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Core.Builders
{
    public enum DbBuilderOption
    {
        /// <summary>Creates the object.</summary>
		Create,
        /// <summary>Do not create the object.</summary>
        Skip,
        /// <summary>Creates the object if it does not exist; otherwise, uses the existing object.</summary>
        CreateOrUseExisting
    }
}
