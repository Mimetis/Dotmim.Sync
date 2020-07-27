using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;
using System.Linq;
using System.Data;
using System.Diagnostics;
using System.Threading.Tasks;

namespace Dotmim.Sync.Builders
{
    public abstract partial class DbTableBuilder
    {

        /// <summary>
        /// Check if trigger exists
        /// </summary>
        public async Task<bool> TriggerExistsAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction)
        {
            if (TableDescription.PrimaryKeys.Count <= 0)
                throw new MissingPrimaryKeyException(TableDescription.TableName);

            var alreadyOpened = connection.State != ConnectionState.Closed;

            if (!alreadyOpened)
                await connection.OpenAsync().ConfigureAwait(false);

            var triggerBuilder = CreateTriggerBuilder();

            var exists = !await triggerBuilder.NeedToCreateTriggerAsync(triggerType, connection, transaction).ConfigureAwait(false);

            if (!alreadyOpened)
                connection.Close();

            return exists;
        }

        /// <summary>
        /// Create a trigger if not exists already
        /// </summary>
        public async Task CreateTriggerAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction)
        {
            if (TableDescription.PrimaryKeys.Count <= 0)
                throw new MissingPrimaryKeyException(TableDescription.TableName);

            var triggerBuilder = CreateTriggerBuilder();

            var alreadyOpened = connection.State != ConnectionState.Closed;

            if (!alreadyOpened)
                await connection.OpenAsync().ConfigureAwait(false);

            var needToCreateTrigger = await triggerBuilder.NeedToCreateTriggerAsync(triggerType, connection, transaction).ConfigureAwait(false);

            if (needToCreateTrigger)
            {
                switch (triggerType)
                {
                    case DbTriggerType.Insert:
                        await triggerBuilder.CreateInsertTriggerAsync(connection, transaction).ConfigureAwait(false);
                        break;
                    case DbTriggerType.Update:
                        await triggerBuilder.CreateUpdateTriggerAsync(connection, transaction).ConfigureAwait(false);
                        break;
                    case DbTriggerType.Delete:
                        await triggerBuilder.CreateDeleteTriggerAsync(connection, transaction).ConfigureAwait(false);
                        break;
                    default:
                        break;
                }
            }

            if (!alreadyOpened)
                connection.Close();
        }

        /// <summary>
        /// Createa all triggers (Insert / Update / Delete)
        /// </summary>
        public async Task CreateTriggersAsync(DbConnection connection, DbTransaction transaction)
        {

            await this.CreateTriggerAsync(DbTriggerType.Insert, connection, transaction);
            await this.CreateTriggerAsync(DbTriggerType.Update, connection, transaction);
            await this.CreateTriggerAsync(DbTriggerType.Delete, connection, transaction);

            // Can't use the Task.WhenAll parrallel tasks because we are not sure to have MultipleActiveResultsSet enabled on the connection
            //var t1 = this.CreateTriggerAsync(DbTriggerType.Insert, connection, transaction);
            //var t2 = this.CreateTriggerAsync(DbTriggerType.Update, connection, transaction);
            //var t3 = this.CreateTriggerAsync(DbTriggerType.Delete, connection, transaction);

            //await Task.WhenAll(t1, t2, t3).ConfigureAwait(false);
        }

        /// <summary>
        /// Drop all triggers (Insert / Update / Delete)
        /// </summary>
        public async Task DropTriggersAsync(DbConnection connection, DbTransaction transaction = null)
        {
            var alreadyOpened = connection.State != ConnectionState.Closed;

            if (!alreadyOpened)
                await connection.OpenAsync().ConfigureAwait(false);

            var triggerBuilder = CreateTriggerBuilder();

            if (!await triggerBuilder.NeedToCreateTriggerAsync(DbTriggerType.Insert, connection, transaction).ConfigureAwait(false))
                await triggerBuilder.DropInsertTriggerAsync(connection, transaction).ConfigureAwait(false);
            if (!await triggerBuilder.NeedToCreateTriggerAsync(DbTriggerType.Update, connection, transaction).ConfigureAwait(false))
                await triggerBuilder.DropUpdateTriggerAsync(connection, transaction).ConfigureAwait(false);
            if (!await triggerBuilder.NeedToCreateTriggerAsync(DbTriggerType.Delete, connection, transaction).ConfigureAwait(false))
                await triggerBuilder.DropDeleteTriggerAsync(connection, transaction).ConfigureAwait(false);


            if (!alreadyOpened)
                connection.Close();

        }

        /// <summary>
        /// Drop a trigger if already exists
        /// </summary>
        public async Task DropTriggerAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction)
        {
            if (TableDescription.PrimaryKeys.Count <= 0)
                throw new MissingPrimaryKeyException(TableDescription.TableName);

            var alreadyOpened = connection.State != ConnectionState.Closed;

            if (!alreadyOpened)
                await connection.OpenAsync().ConfigureAwait(false);

            var triggerBuilder = CreateTriggerBuilder();

            if (!await triggerBuilder.NeedToCreateTriggerAsync(triggerType, connection, transaction).ConfigureAwait(false))
            {
                switch (triggerType)
                {
                    case DbTriggerType.Insert:
                        await triggerBuilder.DropInsertTriggerAsync(connection, transaction).ConfigureAwait(false);
                        break;
                    case DbTriggerType.Update:
                        await triggerBuilder.DropUpdateTriggerAsync(connection, transaction).ConfigureAwait(false);
                        break;
                    case DbTriggerType.Delete:
                        await triggerBuilder.DropDeleteTriggerAsync(connection, transaction).ConfigureAwait(false);
                        break;
                    default:
                        break;
                }
            }

            if (!alreadyOpened)
                connection.Close();
        }

    }
}
