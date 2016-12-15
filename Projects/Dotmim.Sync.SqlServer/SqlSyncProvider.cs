using Dotmim.Sync.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dotmim.Sync.Core.Batch;
using Dotmim.Sync.SqlServer.Batch;
using Dotmim.Sync.SqlServer.Builders;
using System.Data.Common;
using System.Data.SqlClient;
using Dotmim.Sync.Core.Adapter;

using Dotmim.Sync.Core.Log;
using Dotmim.Sync.Core.Scope;
using Dotmim.Sync.Data;
using Dotmim.Sync.Core.Builders;

namespace Dotmim.Sync.SqlServer
{
    public class SqlSyncProvider : CoreProvider
    {
        FileSystemBatchSerializer serializer;
        string batchFileName;
        string connectionString;
        SqlConnection sqlConnection;


        /// <summary>
        /// Set the batch filename spooler
        /// If == null, no batch enabled
        /// </summary>
        public string BatchFileName
        {
            get
            {
                return batchFileName;
            }
            set
            {
                batchFileName = value;

                if (string.IsNullOrEmpty(batchFileName))
                {
                    serializer = null;
                }
                else
                {
                    if (serializer == null)
                        serializer = new FileSystemBatchSerializer();

                    serializer.Initialize(batchFileName);
                }
            }
        }

        /// <summary>
        /// Get the sql connection used to access the server side database
        /// </summary>
        public override DbConnection Connection
        {
            get
            {
                if (sqlConnection == null)
                    sqlConnection = new SqlConnection(this.connectionString);

                return sqlConnection;
            }
        }


        public SqlSyncProvider(string connectionString) : base()
        {
            this.BatchFileName = Environment.ExpandEnvironmentVariables("%Temp%");
            this.connectionString = connectionString;
        }

        public override SyncBatchSerializer CreateSerializer()
        {
                return serializer;
        }

        
        public override SyncAdapter CreateSyncAdapter()
        {
            return new SqlSyncAdapter();
        }

        public override DbBuilder CreateDatabaseBuilder(DmTable table, DbBuilderOption option = DbBuilderOption.Create)
        {
            return new SqlBuilder(table, this.Connection, option);
        }
    }
}
