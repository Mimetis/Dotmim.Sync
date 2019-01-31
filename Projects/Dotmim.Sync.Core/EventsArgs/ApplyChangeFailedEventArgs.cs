//using Dotmim.Sync.Data;
//using Dotmim.Sync.Enumerations;
//using System;
//using System.Collections.Generic;
//using System.Data.Common;
//using System.Linq;
//using System.Text;
//using System.Threading.Tasks;

//namespace Dotmim.Sync
//{
//    public class ApplyChangeFailedEventArgs : ProgressEventArgs
//    {
//        ConflictResolution resolution;
//        DbTransaction transaction;
//        DbConnection connection;
//        DmRow finalRow;
//        DmTable finalRowTable;

//        /// <summary>
//        /// Gets or Sets the action to be taken when resolving the conflict. 
//        /// If you choose MergeRow, you have to fill the FinalRow property
//        /// </summary>
//        public ConflictResolution Resolution
//        {
//            get => this.resolution;
//            set
//            {
//                if (this.resolution != value)
//                {
//                    this.resolution = value;

//                    if (this.resolution == ConflictResolution.MergeRow)
//                        this.FinalRow = this.finalRowTable.ImportRow(this.Conflict.RemoteRow);
//                    else if (this.FinalRow != null && this.finalRowTable.Rows.Count > 0)
//                        this.finalRowTable.Clear();
//                }
//            }
//        }

//        /// <summary>
//        /// Gets the object that contains data and metadata for the row being applied and for the existing row in the database that caused the failure.
//        /// </summary>
//        public SyncConflict Conflict { get; }

//        /// <summary>
//        /// If we have a merge action, the final row represents the merged row
//        /// </summary>
//        public DmRow FinalRow { get; set; }


//        public ApplyChangeFailedEventArgs(string providerTypeName, SyncStage stage, SyncConflict dbSyncConflict, ConflictResolution action, DbConnection connection, DbTransaction transaction)
//            : base(providerTypeName, stage, connection, transaction)
//        {
//            this.Conflict = dbSyncConflict;
//            this.connection = connection;
//            this.transaction = transaction;
//            this.resolution = action;

//            this.finalRowTable = dbSyncConflict.RemoteRow.Table.Clone();
//            this.finalRowTable.TableName = dbSyncConflict.RemoteRow.Table.TableName;
//        }
//    }
//}
