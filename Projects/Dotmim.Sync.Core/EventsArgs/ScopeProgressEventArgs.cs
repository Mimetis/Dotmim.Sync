using Dotmim.Sync.Core.Scope;
using Dotmim.Sync.Data;
using Dotmim.Sync.Enumerations;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;

namespace Dotmim.Sync.Core
{
    /// <summary>
    /// Event args during a sync progress
    /// </summary>
    public class SyncProgressEventArgs
    {
        /// <summary>
        /// Get the provider type name which raised the event
        /// </summary>
        public string ProviderTypeName { get;  set; }

        /// <summary>
        /// When check if database exist, we generate a script
        /// </summary>
        public String DatabaseScript { get; set; }

        /// <summary>
        /// Gets the configuration used for this sync
        /// </summary>
        public ServiceConfiguration Configuration { get;  set; }

        /// <summary>
        /// Gets the scope info during WriteScopes event
        /// </summary>
        public ScopeInfo ScopeInfo { get;  set; }

        /// <summary>
        /// Gets or Sets the action to be taken : Could eventually Rollback the current processus
        /// </summary>
        public ChangeApplicationAction Action { get; set; }

        /// <summary>
        /// Statistics
        /// </summary>
        public ChangesStatistics ChangesStatistics { get; set; }

        /// <summary>
        /// Current sync context
        /// </summary>
        public SyncContext Context { get;  set; }


    }

    /// <summary>
    /// Changes statistics on a data store
    /// </summary>
    public class ChangesStatistics
    {
        /// <summary>
        /// Get the changes selected to be applied
        /// </summary>
        public List<SelectedChanges> SelectedChanges { get;  set; } = new List<SelectedChanges>();

        /// <summary>
        /// Get the view to be applied 
        /// </summary>
        public List<AppliedChanges> AppliedChanges { get;  set; } = new List<AppliedChanges>();

        /// <summary>
        /// Gets the total number of changes that are to be applied during the synchronization session.
        /// </summary>
        public int TotalSelectedChanges
        {
            get
            {
                int totalChanges = 0;

                foreach (var tableProgress in this.SelectedChanges)
                    totalChanges = totalChanges + tableProgress.TotalChanges;

                return totalChanges;
            }
        }

        /// <summary>
        /// Gets the total number of changes that have been applied during the synchronization session.
        /// </summary>
        public int TotalAppliedChanges
        {
            get
            {
                int changesApplied = 0;
                foreach (var tableProgress in this.AppliedChanges)
                {
                    changesApplied = changesApplied + tableProgress.ChangesApplied;
                }
                return changesApplied;
            }
        }

        /// <summary>
        /// Gets the total number of changes that have failed to be applied during the synchronization session.
        /// </summary>
        public int TotalAppliedChangesFailed
        {
            get
            {
                int changesFailed = 0;
                foreach (var tableProgress in this.AppliedChanges)
                    changesFailed = changesFailed + tableProgress.ChangesFailed;

                return changesFailed;
            }
        }

        /// <summary>
        /// Gets the total number of deletes that are to be applied during the synchronization session.
        /// </summary>
        public int TotalSelectedChangesDeletes
        {
            get
            {
                int deletes = 0;
                foreach (var tableProgress in this.SelectedChanges)
                    deletes = deletes + tableProgress.Deletes;

                return deletes;
            }
        }

        /// <summary>
        /// Gets the total number of inserts that are to be applied during the synchronization session.
        /// </summary>
        public int TotalSelectedChangesInserts
        {
            get
            {
                int inserts = 0;
                foreach (var tableProgress in this.SelectedChanges)
                    inserts = inserts + tableProgress.Inserts;

                return inserts;
            }
        }

        /// <summary>
        /// Gets the total number of updates that are to be applied during the synchronization session.
        /// </summary>
        public int TotalSelectedChangesUpdates
        {
            get
            {
                int updates = 0;
                foreach (var tableProgress in this.SelectedChanges)
                    updates = updates + tableProgress.Updates;

                return updates;
            }
        }
    }


    /// <summary>
    /// Args for applied changed on a source, for each kind of DmRowState (Update / Delete / Insert)
    /// </summary>
    public class AppliedChanges
    {
        /// <summary>
        /// Gets the table where changes were applied
        /// </summary>
        public string TableName { get;  set; }

        /// <summary>
        /// Gets the RowState of the apploed
        /// </summary>
        public DmRowState State { get;  set; }

        /// <summary>
        /// Gets the rows changes applied count
        /// </summary>
        public int ChangesApplied { get; set; }

        /// <summary>
        /// Gets the rows changes failed count
        /// </summary>
        public int ChangesFailed { get; set; }

         void Cleanup()
        {
        }
    }

    /// <summary>
    /// Get changes to be applied (contains Deletes AND Inserts AND Updates)
    /// </summary>
    public class SelectedChanges
    {
        /// <summary>
        /// Gets the table name
        /// </summary>
        public string TableName { get;  set; }

        /// <summary>
        /// Gets or sets the number of deletes that should be applied to a table during the synchronization session.
        /// </summary>
        public int Deletes { get;  set; }

        /// <summary>
        /// Gets or sets the number of inserts that should be applied to a table during the synchronization session.
        /// </summary>
        public int Inserts { get;  set; }

        /// <summary>
        /// Gets or sets the number of updates that should be applied to a table during the synchronization session.
        /// </summary>
        public int Updates { get;  set; }

        /// <summary>
        /// Gets the total number of changes that are applied to a table during the synchronization session.
        /// TODO : DEBUG TIME : To be sure we have the correct number, I set this value from CoreProvider
        /// </summary>
        public int TotalChanges { get;  set; } // => this.Inserts + this.Updates + this.Deletes;
    }


}
