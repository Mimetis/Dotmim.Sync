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
        public virtual Task<SyncContext> DeleteMetadatasAsync(SyncContext context, SyncSetup setup, long timestampLimit,
                             DbConnection connection, DbTransaction transaction,
                             CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {

            SyncSet schema = new SyncSet();

            foreach (var setupTable in setup.Tables)
                schema.Tables.Add(new SyncTable(setupTable.TableName, setupTable.SchemaName));


            return DeleteMetadatasAsync(context, schema, timestampLimit, connection, transaction, cancellationToken, progress);

        }

        public virtual async Task<SyncContext> DeleteMetadatasAsync(SyncContext context, SyncSet schema, long timestampLimit,
                            DbConnection connection, DbTransaction transaction,
                            CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            foreach (var syncTable in schema.Tables)
            {
                // get table builder
                var tableBuilder = this.GetTableBuilder(syncTable);

                var tableHelper = tableBuilder.CreateTableBuilder(connection, transaction);

                // check if table exists
                if (tableHelper.NeedToCreateTable())
                    return await Task.FromResult(context).ConfigureAwait(false);

                // Create sync adapter
                var syncAdapter = tableBuilder.CreateSyncAdapter(connection, transaction);

                // Delete metadatas
                syncAdapter.DeleteMetadatas(timestampLimit);
            }
            return await Task.FromResult(context).ConfigureAwait(false);
        }

    }
}
