using Dotmim.Sync.Batch;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Data;
using Dotmim.Sync.Data.Surrogate;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Manager;
using Dotmim.Sync.Serialization;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    public abstract partial class CoreProvider
    {

        /// <summary>
        /// Apply changes : Insert / Updates Delete
        /// the fromScope is local client scope when this method is called from server
        /// the fromScope is server scope when this method is called from client
        /// </summary>
        public virtual async Task<(SyncContext, ChangesApplied)> ApplyChangesAsync(SyncContext context, ScopeInfo fromScope, BatchInfo changes)
        {
            try
            {
                ChangeApplicationAction changeApplicationAction;
                DbTransaction applyTransaction = null;
                ChangesApplied changesApplied = new ChangesApplied();

                using (var connection = this.CreateConnection())
                {
                    try
                    {
                        await connection.OpenAsync();

                        // Create a transaction
                        applyTransaction = connection.BeginTransaction();

                        Debug.WriteLine($"----- Applying Changes for Scope \"{fromScope.Name}\" -----");
                        Debug.WriteLine("");

                        // -----------------------------------------------------
                        // 0) Check if we are in a reinit mode
                        // -----------------------------------------------------
                        if (context.SyncWay == SyncWay.Download && context.SyncType != SyncType.Normal)
                        {
                            changeApplicationAction = this.ResetInternal(context, connection, applyTransaction, fromScope);

                            // Rollback
                            if (changeApplicationAction == ChangeApplicationAction.Rollback)
                                throw SyncException.CreateRollbackException(context.SyncStage);
                        }

                        // -----------------------------------------------------
                        // 1) Applying deletes. Do not apply deletes if we are in a new database
                        // -----------------------------------------------------
                        if (!fromScope.IsNewScope)
                        {
                            changeApplicationAction = this.ApplyChangesInternal(context, connection, applyTransaction, fromScope, changes, DmRowState.Deleted, changesApplied);

                            // Rollback
                            if (changeApplicationAction == ChangeApplicationAction.Rollback)
                                throw SyncException.CreateRollbackException(context.SyncStage);
                        }

                        // -----------------------------------------------------
                        // 1) Applying Inserts
                        // -----------------------------------------------------
                        changeApplicationAction = this.ApplyChangesInternal(context, connection, applyTransaction, fromScope, changes, DmRowState.Added, changesApplied);

                        // Rollback
                        if (changeApplicationAction == ChangeApplicationAction.Rollback)
                            throw SyncException.CreateRollbackException(context.SyncStage);

                        // -----------------------------------------------------
                        // 1) Applying updates
                        // -----------------------------------------------------
                        changeApplicationAction = this.ApplyChangesInternal(context, connection, applyTransaction, fromScope, changes, DmRowState.Modified, changesApplied);

                        // Rollback
                        if (changeApplicationAction == ChangeApplicationAction.Rollback)
                            throw SyncException.CreateRollbackException(context.SyncStage);

                        applyTransaction.Commit();

                        Debug.WriteLine($"--- End Applying Changes for Scope \"{fromScope.Name}\" ---");
                        Debug.WriteLine("");

                    }
                    catch (Exception exception)
                    {
                        Debug.WriteLine($"Caught exception while applying changes: {exception}");
                        throw;
                    }
                    finally
                    {
                        if (applyTransaction != null)
                        {
                            applyTransaction.Dispose();
                            applyTransaction = null;
                        }

                        if (connection != null && connection.State == ConnectionState.Open)
                            connection.Close();

                        if (changes != null)
                            changes.Clear();

                    }
                    return (context, changesApplied);
                }
            }
            catch (Exception)
            {
                throw;
            }
        }

        /// <summary>
        /// Here we are reseting all tables and tracking tables to be able to Reinitialize completely
        /// </summary>
        private ChangeApplicationAction ResetInternal(SyncContext context, DbConnection connection, DbTransaction transaction, ScopeInfo fromScope)
        {
            var configuration = GetCacheConfiguration();

            for (int i = 0; i < configuration.Count; i++)
            {
                try
                {
                   var tableDescription = configuration[configuration.Count - i - 1];

                    var builder = this.GetDatabaseBuilder(tableDescription);
                    var syncAdapter = builder.CreateSyncAdapter(connection, transaction);

                    // reset table
                    syncAdapter.ResetTable(tableDescription);

                }
                catch (Exception ex)
                {
                    Debug.WriteLine($"Error during ResetInternal : {ex.Message}");
                    throw;
                }
            }
            return ChangeApplicationAction.Continue;

        }

        /// <summary>
        /// Apply changes internal method for one Insert or Update or Delete for every dbSyncAdapter
        /// </summary>
        internal ChangeApplicationAction ApplyChangesInternal(SyncContext context, DbConnection connection, DbTransaction transaction, ScopeInfo fromScope, BatchInfo changes, DmRowState applyType, ChangesApplied changesApplied)
        {
            ChangeApplicationAction changeApplicationAction = ChangeApplicationAction.Continue;

            var configuration = GetCacheConfiguration();


            // for each adapters (Zero to End for Insert / Updates -- End to Zero for Deletes
            for (int i = 0; i < configuration.Count; i++)
            {
                try
                {
                    // If we have a delete we must go from Up to Down, orthewise Dow to Up index
                    var tableDescription = (applyType != DmRowState.Deleted ?
                            configuration[i] :
                            configuration[configuration.Count - i - 1]);

                    // if we are in upload stage, so check if table is not download only
                    if (context.SyncWay == SyncWay.Upload && tableDescription.SyncDirection == SyncDirection.DownloadOnly)
                        continue;

                    // if we are in download stage, so check if table is not download only
                    if (context.SyncWay == SyncWay.Download && tableDescription.SyncDirection == SyncDirection.UploadOnly)
                        continue;


                    var builder = this.GetDatabaseBuilder(tableDescription);
                    var syncAdapter = builder.CreateSyncAdapter(connection, transaction);

                    syncAdapter.ConflictApplyAction = configuration.GetApplyAction();

                    // Set syncAdapter properties
                    syncAdapter.applyType = applyType;

                    if (syncAdapter.ConflictActionInvoker == null && this.ApplyChangedFailed != null)
                        syncAdapter.ConflictActionInvoker = GetConflictAction;

                    Debug.WriteLine($"----- Operation {applyType.ToString()} for Table \"{tableDescription.TableName}\" -----");

                    if (changes.BatchPartsInfo != null && changes.BatchPartsInfo.Count > 0)
                    {
                        // getting the table to be applied
                        // we may have multiple batch files, so we can have multipe dmTable with the same Name
                        // We can say that dmTable may be contained in several files
                        foreach (DmTable dmTablePart in changes.GetTable(tableDescription.TableName))
                        {
                            if (dmTablePart == null || dmTablePart.Rows.Count == 0)
                                continue;

                            // check and filter
                            var dmChangesView = new DmView(dmTablePart, (r) => r.RowState == applyType);

                            if (dmChangesView.Count == 0)
                            {
                                dmChangesView.Dispose();
                                dmChangesView = null;
                                continue;
                            }

                            // Conflicts occured when trying to apply rows
                            List<SyncConflict> conflicts = new List<SyncConflict>();

                            // Raise event progress only if there are rows to be applied
                            context.SyncStage = SyncStage.TableChangesApplying;
                            var args = new TableChangesApplyingEventArgs(this.ProviderTypeName, context.SyncStage, tableDescription.TableName, applyType);
                            this.TryRaiseProgressEvent(args, this.TableChangesApplying);

                            int rowsApplied;
                            // applying the bulkchanges command
                            if (configuration.UseBulkOperations && this.SupportBulkOperations)
                                rowsApplied = syncAdapter.ApplyBulkChanges(dmChangesView, fromScope, conflicts);
                            else
                                rowsApplied = syncAdapter.ApplyChanges(dmChangesView, fromScope, conflicts);

                            // If conflicts occured
                            // Eventuall, conflicts are resolved on server side.
                            if (conflicts != null && conflicts.Count > 0)
                            {
                                foreach (var conflict in conflicts)
                                {
                                    DmRow resolvedRow;
                                    var scopeBuilder = this.GetScopeBuilder();
                                    var scopeInfoBuilder = scopeBuilder.CreateScopeInfoBuilder(connection, transaction);
                                    var localTimeStamp = scopeInfoBuilder.GetLocalTimestamp();

                                    changeApplicationAction = syncAdapter.HandleConflict(conflict, fromScope, localTimeStamp, out resolvedRow);

                                    if (changeApplicationAction == ChangeApplicationAction.Continue)
                                    {
                                        // row resolved
                                        if (resolvedRow != null)
                                            rowsApplied++;
                                    }
                                    else
                                    {
                                        context.TotalSyncErrors++;
                                        // TODO : Should we break at the first error ?
                                        return ChangeApplicationAction.Rollback;
                                    }
                                }
                            }

                            // Get all conflicts resolved
                            context.TotalSyncConflicts = conflicts.Where(c => c.Type != ConflictType.ErrorsOccurred).Sum(c => 1);

                            // Handle sync progress for this syncadapter (so this table)
                            var changedFailed = dmChangesView.Count - rowsApplied;

                            // raise SyncProgress Event

                            var existAppliedChanges = changesApplied.TableChangesApplied.FirstOrDefault(
                                    sc => string.Equals(sc.TableName, tableDescription.TableName) && sc.State == applyType);

                            if (existAppliedChanges == null)
                            {
                                existAppliedChanges = new TableChangesApplied();
                                existAppliedChanges.TableName = tableDescription.TableName;
                                existAppliedChanges.Applied = rowsApplied;
                                existAppliedChanges.Failed = changedFailed;
                                existAppliedChanges.State = applyType;
                                changesApplied.TableChangesApplied.Add(existAppliedChanges);
                            }
                            else
                            {
                                existAppliedChanges.Applied += rowsApplied;
                                existAppliedChanges.Failed += changedFailed;
                            }

                            // Event progress
                            context.SyncStage = SyncStage.TableChangesApplied;
                            var progressEventArgs = new TableChangesAppliedEventArgs(this.ProviderTypeName, context.SyncStage, existAppliedChanges);
                            this.TryRaiseProgressEvent(progressEventArgs, this.TableChangesApplied);

                        }
                    }

                    if (syncAdapter.ConflictActionInvoker != null)
                        syncAdapter.ConflictActionInvoker = null;

                    Debug.WriteLine("");
                    Debug.WriteLine($"--- End {applyType.ToString()} for Table \"{tableDescription.TableName}\" ---");
                    Debug.WriteLine("");

                }
                catch (Exception ex)
                {
                    Debug.WriteLine($"Error during ApplyInternalChanges : {ex.Message}");
                    throw;
                }
            }

            return ChangeApplicationAction.Continue;
        }


        /// <summary>
        /// A conflict has occured, we try to ask for the solution to the user
        /// </summary>
        internal (ApplyAction, DmRow) GetConflictAction(SyncConflict conflict, DbConnection connection, DbTransaction transaction = null)
        {
            Debug.WriteLine("Raising Apply Change Failed Event");
            var configuration = GetCacheConfiguration();

            var configApplyAction = configuration.GetApplyAction();

            ConflictAction conflictAction = ConflictAction.ServerWins;

            if (configuration.ConflictResolutionPolicy == ConflictResolutionPolicy.ClientWins)
                conflictAction = ConflictAction.ClientWins;

            var arg = new ApplyChangeFailedEventArgs(conflict, conflictAction, connection, transaction);

            this.ApplyChangedFailed?.Invoke(this, arg);

            // if ConflictAction is ServerWins or MergeRow it's Ok to set to Continue
            ApplyAction action = ApplyAction.Continue;

            if (arg.Action == ConflictAction.ClientWins)
                action = ApplyAction.RetryWithForceWrite;

            DmRow finalRow = arg.Action == ConflictAction.MergeRow ? arg.FinalRow : null;

            // returning the action to take, and actually the finalRow if action is set to Merge
            return (action, finalRow);
        }

    }
}
