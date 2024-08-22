using Microsoft.Data.SqlClient;
using System.Data.Common;
using System.Threading.Tasks;

namespace Dotmim.Sync.SqlServer.Builders
{
    /// <inheritdoc />
    public class SqlChangeTrackingDatabaseBuilder : SqlDatabaseBuilder
    {
        /// <inheritdoc />
        public override async Task EnsureDatabaseAsync(DbConnection connection, DbTransaction transaction = null)
        {
            // Chek if db exists
            await base.EnsureDatabaseAsync(connection, transaction).ConfigureAwait(false);

            // Check if we are using change tracking and it's enabled on the source
            var isChangeTrackingEnabled = await SqlManagementUtils.IsChangeTrackingEnabledAsync(connection as SqlConnection, transaction as SqlTransaction).ConfigureAwait(false);

            if (!isChangeTrackingEnabled)
                throw new MissingChangeTrackingException(connection.Database);
        }
    }
}