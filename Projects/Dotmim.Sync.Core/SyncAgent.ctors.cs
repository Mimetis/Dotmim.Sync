using Dotmim.Sync.Batch;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Serialization;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{

    /// <summary>
    /// Sync agent. It's the sync orchestrator
    /// Knows both the Sync Server provider and the Sync Client provider
    /// </summary>
    public partial class SyncAgent : IDisposable
    {
        // ---------------------------------------------
        // null
        // ---------------------------------------------


        /// <summary>
        /// Launch a Synchronization based on scope DefaultScope
        /// </summary>
        /// <param name="progress">IProgress instance to get a progression status during sync</param>
        /// <returns>Computed sync results</returns>
        public Task<SyncResult> SynchronizeAsync(IProgress<ProgressArgs> progress = null)
            => SynchronizeAsync(SyncOptions.DefaultScopeName, null, SyncType.Normal, null, CancellationToken.None, progress);

        /// <summary>
        /// Launch a Synchronization based on scope DefaultScope
        /// </summary>
        /// <param name="parameters">Parameters values for each of your setup filters</param>
        /// <param name="progress">IProgress instance to get a progression status during sync</param>
        /// <returns>Computed sync results</returns>
        public Task<SyncResult> SynchronizeAsync(SyncParameters parameters, IProgress<ProgressArgs> progress = null)
            => SynchronizeAsync(SyncOptions.DefaultScopeName, null, SyncType.Normal, parameters, CancellationToken.None, progress);

        /// <summary>
        /// Launch a Synchronization based on scope DefaultScope
        /// </summary>
        /// <param name="syncType">Synchronization mode: Normal, Reinitialize or ReinitializeWithUpload</param>
        /// <param name="progress">IProgress instance to get a progression status during sync</param>
        /// <returns>Computed sync results</returns>
        public Task<SyncResult> SynchronizeAsync(SyncType syncType, IProgress<ProgressArgs> progress = null)
            => SynchronizeAsync(SyncOptions.DefaultScopeName, null, syncType, null, CancellationToken.None, progress);

        /// <summary>
        /// Launch a Synchronization based on scope DefaultScope
        /// </summary>
        /// <param name="syncType">Synchronization mode: Normal, Reinitialize or ReinitializeWithUpload</param>
        /// <param name="parameters">Parameters values for each of your setup filters</param>
        /// <param name="progress">IProgress instance to get a progression status during sync</param>
        /// <returns>Computed sync results</returns>
        public Task<SyncResult> SynchronizeAsync(SyncType syncType, SyncParameters parameters, IProgress<ProgressArgs> progress = null)
            => SynchronizeAsync(SyncOptions.DefaultScopeName, null, syncType, parameters, CancellationToken.None, progress);

        /// <summary>
        /// Launch a Synchronization based on a named scope
        /// </summary>
        /// <param name="scopeName">Named scope</param>
        /// <param name="progress">IProgress instance to get a progression status during sync</param>
        /// <returns>Computed sync results</returns>
        public Task<SyncResult> SynchronizeAsync(string scopeName, IProgress<ProgressArgs> progress = null)
            => SynchronizeAsync(scopeName, null, SyncType.Normal, null, CancellationToken.None, progress);


        /// <summary>
        /// Launch a Synchronization based on a named scope
        /// </summary>
        /// <param name="scopeName">Named scope</param>
        /// <param name="parameters">Parameters values for each of your setup filters</param>
        /// <param name="progress">IProgress instance to get a progression status during sync</param>
        /// <returns>Computed sync results</returns>
        public Task<SyncResult> SynchronizeAsync(string scopeName, SyncParameters parameters, IProgress<ProgressArgs> progress = null)
            => SynchronizeAsync(scopeName, null, SyncType.Normal, parameters, CancellationToken.None, progress);

        /// <summary>
        /// Launch a Synchronization based on a named scope
        /// </summary>
        /// <param name="scopeName">Named scope</param>
        /// <param name="syncType">Synchronization mode: Normal, Reinitialize or ReinitializeWithUpload</param>
        /// <param name="progress">IProgress instance to get a progression status during sync</param>
        /// <returns>Computed sync results</returns>
        public Task<SyncResult> SynchronizeAsync(string scopeName, SyncType syncType, IProgress<ProgressArgs> progress = null)
            => SynchronizeAsync(scopeName, null, syncType, null, CancellationToken.None, progress);

        /// <summary>
        /// Launch a Synchronization based on a named scope
        /// </summary>
        /// <param name="scopeName">Named scope</param>
        /// <param name="syncType">Synchronization mode: Normal, Reinitialize or ReinitializeWithUpload</param>
        /// <param name="parameters">Parameters values for each of your setup filters</param>
        /// <param name="progress">IProgress instance to get a progression status during sync</param>
        /// <returns>Computed sync results</returns>
        public Task<SyncResult> SynchronizeAsync(string scopeName, SyncType syncType, SyncParameters parameters, IProgress<ProgressArgs> progress = null)
            => SynchronizeAsync(scopeName, null, syncType, parameters, CancellationToken.None, progress);


        // ---------------------------------------------
        // string[] tables
        // ---------------------------------------------

        /// <summary>
        /// Launch a Synchronization based on scope DefaultScope
        /// </summary>
        /// <param name="tables">Tables list to synchronize</param>
        /// <param name="progress">IProgress instance to get a progression status during sync</param>
        /// <returns>Computed sync results</returns>
        public Task<SyncResult> SynchronizeAsync(string[] tables, IProgress<ProgressArgs> progress = default) =>
            SynchronizeAsync(SyncOptions.DefaultScopeName, new SyncSetup(tables), SyncType.Normal, null, CancellationToken.None, progress);

        /// <summary>
        /// Launch a Synchronization based on scope DefaultScope
        /// </summary>
        /// <param name="tables">Tables list to synchronize</param>
        /// <param name="parameters">Parameters values for each of your setup filters</param>
        /// <param name="progress">IProgress instance to get a progression status during sync</param>
        /// <returns>Computed sync results</returns>
        public Task<SyncResult> SynchronizeAsync(string[] tables, SyncParameters parameters, IProgress<ProgressArgs> progress = default) =>
            SynchronizeAsync(SyncOptions.DefaultScopeName, new SyncSetup(tables), SyncType.Normal, parameters, CancellationToken.None, progress);

        /// <summary>
        /// Launch a Synchronization based on scope DefaultScope
        /// </summary>
        /// <param name="tables">Tables list to synchronize</param>
        /// <param name="syncType">Synchronization mode: Normal, Reinitialize or ReinitializeWithUpload</param>
        /// <param name="progress">IProgress instance to get a progression status during sync</param>
        /// <returns>Computed sync results</returns>
        public Task<SyncResult> SynchronizeAsync(string[] tables, SyncType syncType, IProgress<ProgressArgs> progress = default) =>
            SynchronizeAsync(SyncOptions.DefaultScopeName, new SyncSetup(tables), syncType, null, CancellationToken.None, progress);

        /// <summary>
        /// Launch a Synchronization based on scope DefaultScope
        /// </summary>
        /// <param name="tables">Tables list to synchronize</param>
        /// <param name="syncType">Synchronization mode: Normal, Reinitialize or ReinitializeWithUpload</param>
        /// <param name="parameters">Parameters values for each of your setup filters</param>
        /// <param name="progress">IProgress instance to get a progression status during sync</param>
        /// <returns>Computed sync results</returns>
        public Task<SyncResult> SynchronizeAsync(string[] tables, SyncType syncType, SyncParameters parameters, IProgress<ProgressArgs> progress = default) =>
            SynchronizeAsync(SyncOptions.DefaultScopeName, new SyncSetup(tables), syncType, parameters, CancellationToken.None, progress);

        /// <summary>
        /// Launch a Synchronization based on a named scope
        /// </summary>
        /// <param name="scopeName">Named scope</param>
        /// <param name="tables">Tables list to synchronize</param>
        /// <param name="progress">IProgress instance to get a progression status during sync</param>
        /// <returns>Computed sync results</returns>
        public Task<SyncResult> SynchronizeAsync(string scopeName, string[] tables, IProgress<ProgressArgs> progress = default) =>
            SynchronizeAsync(scopeName, new SyncSetup(tables), SyncType.Normal, null, CancellationToken.None, progress);

        /// <summary>
        /// Launch a Synchronization based on a named scope
        /// </summary>
        /// <param name="scopeName">Named scope</param>
        /// <param name="tables">Tables list to synchronize</param>
        /// <param name="parameters">Parameters values for each of your setup filters</param>
        /// <param name="progress">IProgress instance to get a progression status during sync</param>
        /// <returns>Computed sync results</returns>
        public Task<SyncResult> SynchronizeAsync(string scopeName, string[] tables, SyncParameters parameters, IProgress<ProgressArgs> progress = default) =>
            SynchronizeAsync(scopeName, new SyncSetup(tables), SyncType.Normal, parameters, CancellationToken.None, progress);

        /// <summary>
        /// Launch a Synchronization based on a named scope
        /// </summary>
        /// <param name="scopeName">Named scope</param>
        /// <param name="tables">Tables list to synchronize</param>
        /// <param name="syncType">Synchronization mode: Normal, Reinitialize or ReinitializeWithUpload</param>
        /// <param name="progress">IProgress instance to get a progression status during sync</param>
        /// <returns>Computed sync results</returns>
        public Task<SyncResult> SynchronizeAsync(string scopeName, string[] tables, SyncType syncType, IProgress<ProgressArgs> progress = default) =>
            SynchronizeAsync(scopeName, new SyncSetup(tables), syncType, null, CancellationToken.None, progress);

        /// <summary>
        /// Launch a Synchronization based on a named scope
        /// </summary>
        /// <param name="scopeName">Named scope</param>
        /// <param name="tables">Tables list to synchronize</param>
        /// <param name="syncType">Synchronization mode: Normal, Reinitialize or ReinitializeWithUpload</param>
        /// <param name="parameters">Parameters values for each of your setup filters</param>
        /// <param name="progress">IProgress instance to get a progression status during sync</param>
        /// <returns>Computed sync results</returns>
        public Task<SyncResult> SynchronizeAsync(string scopeName, string[] tables, SyncType syncType, SyncParameters parameters, IProgress<ProgressArgs> progress = default) =>
            SynchronizeAsync(scopeName, new SyncSetup(tables), syncType, parameters, CancellationToken.None, progress);


        // ---------------------------------------------
        // SyncSetup setup
        // ---------------------------------------------

        /// <summary>
        /// Launch a Synchronization based on scope DefaultScope
        /// </summary>
        /// <param name="setup">Setup instance containing the table list and optionnally columns</param>
        /// <param name="progress">IProgress instance to get a progression status during sync</param>
        /// <returns>Computed sync results</returns>
        public Task<SyncResult> SynchronizeAsync(SyncSetup setup, IProgress<ProgressArgs> progress = default)
            => SynchronizeAsync(SyncOptions.DefaultScopeName, setup, SyncType.Normal, null, CancellationToken.None, progress);

        /// <summary>
        /// Launch a Synchronization based on scope DefaultScope
        /// </summary>
        /// <param name="setup">Setup instance containing the table list and optionnally columns</param>
        /// <param name="parameters">Parameters values for each of your setup filters</param>
        /// <param name="progress">IProgress instance to get a progression status during sync</param>
        /// <returns>Computed sync results</returns>
        public Task<SyncResult> SynchronizeAsync(SyncSetup setup, SyncParameters parameters, IProgress<ProgressArgs> progress = default)
            => SynchronizeAsync(SyncOptions.DefaultScopeName, setup, SyncType.Normal, parameters, CancellationToken.None, progress);

        /// <summary>
        /// Launch a Synchronization based on scope DefaultScope
        /// </summary>
        /// <param name="setup">Setup instance containing the table list and optionnally columns</param>
        /// <param name="syncType">Synchronization mode: Normal, Reinitialize or ReinitializeWithUpload</param>
        /// <param name="progress">IProgress instance to get a progression status during sync</param>
        /// <returns>Computed sync results</returns>
        public Task<SyncResult> SynchronizeAsync(SyncSetup setup, SyncType syncType, IProgress<ProgressArgs> progress = default)
            => SynchronizeAsync(SyncOptions.DefaultScopeName, setup, syncType, null, CancellationToken.None, progress);

        /// <summary>
        /// Launch a Synchronization based on scope DefaultScope
        /// </summary>
        /// <param name="setup">Setup instance containing the table list and optionnally columns</param>
        /// <param name="syncType">Synchronization mode: Normal, Reinitialize or ReinitializeWithUpload</param>
        /// <param name="parameters">Parameters values for each of your setup filters</param>
        /// <param name="progress">IProgress instance to get a progression status during sync</param>
        /// <returns>Computed sync results</returns>
        public Task<SyncResult> SynchronizeAsync(SyncSetup setup, SyncType syncType, SyncParameters parameters, IProgress<ProgressArgs> progress = default)
            => SynchronizeAsync(SyncOptions.DefaultScopeName, setup, syncType, parameters, CancellationToken.None, progress);

        /// <summary>
        /// Launch a Synchronization based on a named scope
        /// </summary>
        /// <param name="scopeName">Named scope</param>
        /// <param name="setup">Setup instance containing the table list and optionnally columns</param>
        /// <param name="progress">IProgress instance to get a progression status during sync</param>
        /// <returns>Computed sync results</returns>
        public Task<SyncResult> SynchronizeAsync(string scopeName, SyncSetup setup, IProgress<ProgressArgs> progress = default)
            => SynchronizeAsync(scopeName, setup, SyncType.Normal, null, CancellationToken.None, progress);

        /// <summary>
        /// Launch a Synchronization based on a named scope
        /// </summary>
        /// <param name="scopeName">Named scope</param>
        /// <param name="setup">Setup instance containing the table list and optionnally columns</param>
        /// <param name="parameters">Parameters values for each of your setup filters</param>
        /// <param name="progress">IProgress instance to get a progression status during sync</param>
        /// <returns>Computed sync results</returns>
        public Task<SyncResult> SynchronizeAsync(string scopeName, SyncSetup setup, SyncParameters parameters, IProgress<ProgressArgs> progress = default)
            => SynchronizeAsync(scopeName, setup, SyncType.Normal, parameters, CancellationToken.None, progress);

        /// <summary>
        /// Launch a Synchronization based on a named scope
        /// </summary>
        /// <param name="scopeName">Named scope</param>
        /// <param name="setup">Setup instance containing the table list and optionnally columns</param>
        /// <param name="syncType">Synchronization mode: Normal, Reinitialize or ReinitializeWithUpload</param>
        /// <param name="progress">IProgress instance to get a progression status during sync</param>
        /// <returns>Computed sync results</returns>
        public Task<SyncResult> SynchronizeAsync(string scopeName, SyncSetup setup, SyncType syncType, IProgress<ProgressArgs> progress = default)
            => SynchronizeAsync(scopeName, setup, syncType, null, CancellationToken.None, progress);

        /// <summary>
        /// Launch a Synchronization based on a named scope
        /// </summary>
        /// <param name="scopeName">Named scope</param>
        /// <param name="setup">Setup instance containing the table list and optionnally columns</param>
        /// <param name="syncType">Synchronization mode: Normal, Reinitialize or ReinitializeWithUpload</param>
        /// <param name="parameters">Parameters values for each of your setup filters</param>
        /// <param name="progress">IProgress instance to get a progression status during sync</param>
        /// <returns>Computed sync results</returns>
        public Task<SyncResult> SynchronizeAsync(string scopeName, SyncSetup setup, SyncType syncType, SyncParameters parameters, IProgress<ProgressArgs> progress = default)
            => SynchronizeAsync(scopeName, setup, syncType, parameters, CancellationToken.None, progress);

        // ---------------------------------------------

    }
}
