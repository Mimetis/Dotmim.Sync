using Dotmim.Sync.Core.Batch;
using Dotmim.Sync.Data;
using Dotmim.Sync.Enumerations;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.Core
{
    ///// <summary>
    ///// Represents a set of synchronization progress statistics for a table that 
    ///// is involved in peer synchronization.
    ///// </summary>
    //public class SyncTableProgress
    //{
    //    DmTable dmTable;
    //    SyncBatchSerializer serializer;
    //    //SyncBatchInfo syncBatchInfo;

    //    /// <summary>
    //    /// Get the table name used for that progress (== Changes.TableName
    //    /// </summary>
    //    public string TableName
    //    {
    //        get
    //        {
    //            if (Changes != null)
    //                return Changes.TableName;

    //            return null;
    //        }
    //    }

   
    //    /// <summary>
    //    /// Gets or sets the DmTable that contains the changes to be synchronized.
    //    /// </summary>
    //    public DmTable Changes
    //    {
    //        get
    //        {

    //            //if (this.dmTable == null && !string.IsNullOrEmpty(this._batchFileName))
    //            //{
    //            //    this._batchInfoHelper = new SyncBatchInfoHelper(this._batchFileName);
    //            //    this._dataTable = this._batchInfoHelper.BatchDataSet.Tables[this._tableName];
    //            //}
                
    //            return this.dmTable;
    //        }
    //        set
    //        {
    //            this.dmTable = value;
    //        }
    //    }


    //    /// <summary>
    //    /// Initializes a new instance of the class by using a DmTable parameter.
    //    /// </summary>
    //    public SyncTableProgress(DmTable table, SyncBatchSerializer serializer = null)
    //    {
    //        this.Changes = table;
    //        this.serializer = serializer;
    //    }

    //}
}
