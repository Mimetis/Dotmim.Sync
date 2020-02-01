using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;

namespace Dotmim.Sync.Builders
{
    public abstract class DbBuilder
    {
        /// <summary>
        /// Gets or Sets if the Database builder supports bulk procedures
        /// </summary>
        public bool UseBulkProcedures { get; set; } = true;

        /// <summary>
        /// Gets or Sets if the Database builder shoud use change tracking
        /// </summary>
        public bool UseChangeTracking { get; set; } = false;

        /// <summary>
        /// First step before creating schema
        /// </summary>
        public abstract void EnsureDatabase(DbConnection connection, DbTransaction transaction = null);

    }
}
