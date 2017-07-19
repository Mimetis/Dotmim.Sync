using Dotmim.Sync.Core.Batch;
using Dotmim.Sync.Data;
using Dotmim.Sync.Enumerations;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dotmim.Sync.Core.Scope;
using Newtonsoft.Json;
using System.ComponentModel;

namespace Dotmim.Sync.Core
{
    /// <summary>
    /// Context of the current Sync session
    /// Encapsulates data changes and metadata for a synchronization session.
    /// </summary>
    public class SyncContext
    {
        /// <summary>
        /// Current Session, in progress
        /// </summary>
        public Guid SessionId { get; internal set; }

        /// <summary>Gets or sets the time when a sync sessionn started.
        /// </summary>
        public DateTime SyncStartTime { get; set; }

        /// <summary>
        /// Actual sync stage
        /// </summary>
        public SyncStage SyncStage { get; set; }


         public SyncContext(Guid sessionId)
        {
            this.SessionId = sessionId;
        }

        /// <summary>
        /// Generate a DmTable based on a SyncContext object
        /// </summary>
        public static void SerializeInDmSet(DmSet set, SyncContext context)
        {
            if (set == null)
                return;

            DmTable dmTableContext = null;
   
            if (!set.Tables.Contains("DotmimSync__ServiceConfiguration"))
            {
                dmTableContext = new DmTable("DotmimSync__SyncContext");
                set.Tables.Add(dmTableContext);
            }

            dmTableContext = set.Tables["DotmimSync__SyncContext"];

            dmTableContext.columns.Add<Guid>("SessionId");
            dmTableContext.columns.Add<DateTime>("SyncStartTime");
            dmTableContext.columns.Add<int>("SyncStage");
      
            DmRow dmRow = dmTableContext.NewRow();

            dmRow["SessionId"] = context.SessionId;
            dmRow["SyncStartTime"] = context.SyncStartTime;
            dmRow["SyncStage"] = (int)context.SyncStage;
     
            dmTableContext.Rows.Add(dmRow);
            
        }
        public static SyncContext DeserializeFromDmSet(DmSet set)
        {
            if (set == null)
                return null;

            if (!set.Tables.Contains("DotmimSync__SyncContext"))
                return null;

            var dmRow = set.Tables["DotmimSync__SyncContext"].Rows[0];

            var sessionId = (Guid)dmRow["SessionId"] ;

            SyncContext syncContext = new SyncContext(sessionId);
            syncContext.SyncStage = (SyncStage)dmRow["SyncStage"];
            syncContext.SyncStartTime = (DateTime)dmRow["SyncStartTime"];
     
            return syncContext;
        }
    }
}
