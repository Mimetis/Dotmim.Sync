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
    public class ScopeProgressEventArgs
    {
        /// <summary>
        /// Get the provider type name which raised the event
        /// </summary>
        public string ProviderTypeName { get; internal set; }

        /// <summary>
        /// Get the scope information (name and last local timestamp)
        /// </summary>
        public ScopeInfo ScopeInfo { get; internal set; }

        /// <summary>
        /// When check if database exist, we generate a script
        /// </summary>
        public String DatabaseScript { get; set; }

        /// <summary>
        /// Gets the configuration used for this sync
        /// </summary>
        public ServiceConfiguration Configuration { get; internal set; }

        /// <summary>
        /// Gets or Sets the action to be taken : Could eventually Rollback the current processus
        /// </summary>
        public ChangeApplicationAction Action { get; set; }

        /// <summary>
        /// Current sync progress stage
        /// </summary>
        public SyncStage Stage { get; internal set; }

        /// <summary>
        /// Get the changes selected to be applied
        /// </summary>
        public List<ScopeSelectedChanges> SelectedChanges { get; internal set; } = new List<ScopeSelectedChanges>();

        /// <summary>
        /// Get the view to be applied 
        /// </summary>
        public List<ScopeAppliedChanges> AppliedChanges { get; internal set; } = new List<ScopeAppliedChanges>();

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


        internal void Cleanup()
        {
            SelectedChanges.ForEach(sc => sc.Cleanup());
            SelectedChanges.Clear();

            AppliedChanges.ForEach(sc => sc.Cleanup());
            AppliedChanges.Clear();
        }
    }

    /// <summary>
    /// Args for applied changed on a source, for each kind of DmRowState (Update / Delete / Insert)
    /// </summary>
    public class ScopeAppliedChanges
    {
        /// <summary>
        /// Gets the table where changes were applied
        /// </summary>
        public string TableName { get; internal set; }

        /// <summary>
        /// Gets the RowState of the apploed
        /// </summary>
        public DmRowState State { get; internal set; }

        /// <summary>
        /// Gets the rows changes applied count
        /// </summary>
        public int ChangesApplied { get; set; }

        /// <summary>
        /// Gets the rows changes failed count
        /// </summary>
        public int ChangesFailed { get; set; }
   
        internal void Cleanup()
        {
        }
    }

    /// <summary>
    /// Get changes to be applied (contains Deletes AND Inserts AND Updates)
    /// </summary>
    public class ScopeSelectedChanges
    {
        /// <summary>
        /// Gets the table name
        /// </summary>
        public string TableName => View.Table.TableName;

        /// <summary>
        /// Gets the View
        /// </summary>
        public DmView View { get; internal set; }
        int GetCountByRowState(DmRowState state) => View.Count(r => r.RowState == state);

        internal void Cleanup()
        {
            View = null;
        }

        /// <summary>
        /// Gets or sets the number of deletes that should be applied to a table during the synchronization session.
        /// </summary>
        public int Deletes => GetCountByRowState(DmRowState.Deleted);

        /// <summary>
        /// Gets or sets the number of inserts that should be applied to a table during the synchronization session.
        /// </summary>
        public int Inserts => GetCountByRowState(DmRowState.Added);

        /// <summary>
        /// Gets or sets the number of updates that should be applied to a table during the synchronization session.
        /// </summary>
        public int Updates => GetCountByRowState(DmRowState.Modified);

        /// <summary>
        /// Gets the total number of changes that are applied to a table during the synchronization session.
        /// </summary>
        public int TotalChanges => this.Inserts + this.Updates + this.Deletes;
    }


}
