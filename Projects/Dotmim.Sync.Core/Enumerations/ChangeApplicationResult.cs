using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.Enumerations
{
    /// <summary>
    /// Result of the change process. Managed by the framework itself
    /// </summary>
    internal enum ChangeApplicationResult
    {
        RowApplicationError,
        MetadataApplicationError,
        RowMetadataNotFound,
        RowNotApplied,
        ChangeNotNeeded,
        RetryNextSync,
        RowAndMetadataPending,
        RowAppliedMetadataPending,
        RowAppliedMetadataFailed,
        RowAndMetadataApplied
    }
}
