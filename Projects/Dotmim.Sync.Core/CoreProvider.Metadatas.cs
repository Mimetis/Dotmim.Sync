using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Manager;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    public abstract partial class CoreProvider
    {
        /// <summary>
        /// update configuration object with tables desc from server database
        /// </summary>
        public async Task<SyncContext> DeleteMetadatasAsync(SyncContext context, SyncSet schema, long timestampLimit,
                             DbConnection connection, DbTransaction transaction,
                             CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {
             foreach (var schemaTable in schema.Tables)
            {
                var tableBuilder = this.GetTableBuilder(schemaTable);

                var syncAdapter = tableBuilder.CreateSyncAdapter(connection, transaction);

                // Delete metadatas
                syncAdapter.DeleteMetadatas(timestampLimit);
            }
            return context;
        }

    }
}
