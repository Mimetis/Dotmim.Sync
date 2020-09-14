using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.Builders
{
    public abstract class DbBuilder
    {

        /// <summary>
        /// Gets or Sets if the Database builder shoud use change tracking
        /// </summary>
        public bool UseChangeTracking { get; set; } = false;

        /// <summary>
        /// First step before creating schema
        /// </summary>
        public abstract Task EnsureDatabaseAsync(DbConnection connection, DbTransaction transaction = null);

        /// <summary>
        /// Make a hello test on the current database
        /// </summary>
        public abstract Task<(string DatabaseName, string Version)> GetHelloAsync(DbConnection connection, DbTransaction transaction = null);

    }
}
