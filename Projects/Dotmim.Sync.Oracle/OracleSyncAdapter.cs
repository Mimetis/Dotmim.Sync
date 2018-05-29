using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Data;

namespace Dotmim.Sync.Oracle
{
    public class OracleSyncAdapter : DbSyncAdapter
    {
        private readonly DbConnection _connection;
        private readonly DbTransaction _transaction;

        public OracleSyncAdapter(DmTable tableDescription) : base(tableDescription)
        {
        }

        public OracleSyncAdapter(DmTable tableDescription, DbConnection connection, DbTransaction transaction) 
            : this(tableDescription)
        {
            _connection = connection;
            _transaction = transaction;
        }

        public override DbConnection Connection => _connection;

        public override DbTransaction Transaction => _transaction;


        public override void ExecuteBatchCommand(DbCommand cmd, DmView applyTable, DmTable failedRows, ScopeInfo scope)
        {
            throw new NotImplementedException();
        }

        public override DbCommand GetCommand(DbCommandType commandType, IEnumerable<string> additionals = null)
        {
            throw new NotImplementedException();
        }

        public override bool IsPrimaryKeyViolation(Exception Error)
        {
            throw new NotImplementedException();
        }

        public override void SetCommandParameters(DbCommandType commandType, DbCommand command)
        {
            throw new NotImplementedException();
        }
    }
}
