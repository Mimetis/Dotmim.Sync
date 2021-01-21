using Dotmim.Sync.Builders;
using Dotmim.Sync.Manager;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.Tests.UnitTests
{
    public class MockTableBuilder : DbTableBuilder
    {
        public MockTableBuilder(SyncTable tableDescription, ParserName tableName, ParserName trackingTableName, SyncSetup setup) : base(tableDescription, tableName, trackingTableName, setup)
        {
        }

        public override Task<DbCommand> GetCreateSchemaCommandAsync(DbConnection connection, DbTransaction transaction) => throw new NotImplementedException();
        public override Task<DbCommand> GetCreateTableCommandAsync(DbConnection connection, DbTransaction transaction) => throw new NotImplementedException();
        public override Task<DbCommand> GetExistsTableCommandAsync(DbConnection connection, DbTransaction transaction) => throw new NotImplementedException();
        public override Task<DbCommand> GetExistsSchemaCommandAsync(DbConnection connection, DbTransaction transaction) => throw new NotImplementedException();
        public override Task<DbCommand> GetDropTableCommandAsync(DbConnection connection, DbTransaction transaction) => throw new NotImplementedException();
        public override Task<DbCommand> GetExistsStoredProcedureCommandAsync(DbStoredProcedureType storedProcedureType, DbConnection connection, DbTransaction transaction) => throw new NotImplementedException();
        public override Task<DbCommand> GetCreateStoredProcedureCommandAsync(DbStoredProcedureType storedProcedureType, SyncFilter filter, DbConnection connection, DbTransaction transaction) => throw new NotImplementedException();
        public override Task<DbCommand> GetDropStoredProcedureCommandAsync(DbStoredProcedureType storedProcedureType, SyncFilter filter, DbConnection connection, DbTransaction transaction) => throw new NotImplementedException();
        public override Task<DbCommand> GetCreateTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction) => throw new NotImplementedException();
        public override Task<DbCommand> GetDropTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction) => throw new NotImplementedException();
        public override Task<DbCommand> GetRenameTrackingTableCommandAsync(ParserName oldTableName, DbConnection connection, DbTransaction transaction) => throw new NotImplementedException();
        public override Task<DbCommand> GetExistsTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction) => throw new NotImplementedException();
        public override Task<DbCommand> GetExistsTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction) => throw new NotImplementedException();
        public override Task<DbCommand> GetCreateTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction) => throw new NotImplementedException();
        public override Task<DbCommand> GetDropTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction) => throw new NotImplementedException();
        public override Task<IEnumerable<SyncColumn>> GetColumnsAsync(DbConnection connection, DbTransaction transaction) => throw new NotImplementedException();
        public override Task<IEnumerable<DbRelationDefinition>> GetRelationsAsync(DbConnection connection, DbTransaction transaction) => throw new NotImplementedException();
        public override Task<IEnumerable<SyncColumn>> GetPrimaryKeysAsync(DbConnection connection, DbTransaction transaction) => throw new NotImplementedException();
    }
}
