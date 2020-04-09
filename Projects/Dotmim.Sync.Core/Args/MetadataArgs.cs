using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;

namespace Dotmim.Sync
{
    public class MetadataCleaningArgs : ProgressArgs
    {
        public SyncSetup Setup { get; }
        public long TimeStampStart { get; }

        public MetadataCleaningArgs(SyncContext context, SyncSetup setup, long timeStampStart, DbConnection connection, DbTransaction transaction)
        : base(context, connection, transaction)

        {
            this.Setup = setup;
            this.TimeStampStart = timeStampStart;
        }

        public override string Message => $"tables cleaning count:{Setup.Tables.Count}";

    }

    public class MetadataCleanedArgs : ProgressArgs
    {
        public MetadataCleanedArgs(SyncContext context, DatabaseMetadatasCleaned databaseMetadatasCleaned, DbConnection connection = null, DbTransaction transaction = null) 
            : base(context, connection, transaction)
        {
            this.DatabaseMetadatasCleaned = databaseMetadatasCleaned;
        }

        /// <summary>
        /// Gets or Sets the rows count cleaned for all tables, during a DeleteMetadatasAsync call
        /// </summary>
        public DatabaseMetadatasCleaned DatabaseMetadatasCleaned { get; set; }

        public override string Message => $"Tables cleaned count:{DatabaseMetadatasCleaned.Tables.Count}. Rows cleaned count:{DatabaseMetadatasCleaned.RowsCleanedCount}";

    }
}
