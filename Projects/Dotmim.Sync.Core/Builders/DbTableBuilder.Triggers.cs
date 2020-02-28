using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;
using System.Linq;
using System.Data;
using Dotmim.Sync.Log;
using System.Diagnostics;

namespace Dotmim.Sync.Builders
{
    public abstract partial class DbTableBuilder
    {

        /// <summary>
        /// Check if trigger exists
        /// </summary>
        public bool TriggerExists(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction= null)
        {
            if (TableDescription.PrimaryKeys.Count <= 0)
                throw new MissingPrimaryKeyException(TableDescription.TableName);

            var alreadyOpened = connection.State != ConnectionState.Closed;

            var triggerBuilder = CreateTriggerBuilder(connection, transaction);

            var exists = !triggerBuilder.NeedToCreateTrigger(triggerType);

            if (!alreadyOpened)
                connection.Close();

            return exists;
        }

        /// <summary>
        /// Create a trigger if not exists already
        /// </summary>
        public void CreateTrigger(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction = null)
        {
            if (TableDescription.PrimaryKeys.Count <= 0)
                throw new MissingPrimaryKeyException(TableDescription.TableName);

            var alreadyOpened = connection.State != ConnectionState.Closed;

            var triggerBuilder = CreateTriggerBuilder(connection, transaction);

            if (triggerBuilder.NeedToCreateTrigger(triggerType))
            {
                switch (triggerType)
                {
                    case DbTriggerType.Insert:
                        triggerBuilder.CreateInsertTrigger();
                        break;
                    case DbTriggerType.Update:
                        triggerBuilder.CreateUpdateTrigger();
                        break;
                    case DbTriggerType.Delete:
                        triggerBuilder.CreateDeleteTrigger();
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
        public void CreateTriggers(DbConnection connection, DbTransaction transaction = null)
        {
            this.CreateTrigger(DbTriggerType.Insert, connection, transaction);
            this.CreateTrigger(DbTriggerType.Update, connection, transaction);
            this.CreateTrigger(DbTriggerType.Delete, connection, transaction);

        }

        /// <summary>
        /// Drop all triggers (Insert / Update / Delete)
        /// </summary>
        public void DropTriggers(DbConnection connection, DbTransaction transaction = null)
        {
            var alreadyOpened = connection.State != ConnectionState.Closed;

            if (!alreadyOpened)
                connection.Open();

            var triggerBuilder = CreateTriggerBuilder(connection, transaction);

            if (!triggerBuilder.NeedToCreateTrigger(DbTriggerType.Insert))
                triggerBuilder.DropInsertTrigger();
            if (!triggerBuilder.NeedToCreateTrigger(DbTriggerType.Update))
                triggerBuilder.DropUpdateTrigger();
            if (!triggerBuilder.NeedToCreateTrigger(DbTriggerType.Delete))
                triggerBuilder.DropDeleteTrigger();


            if (!alreadyOpened)
                connection.Close();

        }

        /// <summary>
        /// Drop a trigger if already exists
        /// </summary>
        public void DropTrigger(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction = null)
        {
            if (TableDescription.PrimaryKeys.Count <= 0)
                throw new MissingPrimaryKeyException(TableDescription.TableName);

            var alreadyOpened = connection.State != ConnectionState.Closed;

            var triggerBuilder = CreateTriggerBuilder(connection, transaction);

            if (!triggerBuilder.NeedToCreateTrigger(triggerType))
            {
                switch (triggerType)
                {
                    case DbTriggerType.Insert:
                        triggerBuilder.DropInsertTrigger();
                        break;
                    case DbTriggerType.Update:
                        triggerBuilder.DropUpdateTrigger();
                        break;
                    case DbTriggerType.Delete:
                        triggerBuilder.DropDeleteTrigger();
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
