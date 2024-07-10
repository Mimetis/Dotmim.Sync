using Dotmim.Sync.Enumerations;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    /// <summary>
    /// Sync agent. It's the sync orchestrator
    /// Knows both the Sync Server provider and the Sync Client provider.
    /// </summary>
    public partial class SyncAgent : IDisposable
    {
        // ---------------------------------------------
        // null
        // ---------------------------------------------

        /// <summary>
        /// Launch a Synchronization based on scope DefaultScope.
        /// </summary>
        /// <param name="progress">IProgress instance to get a progression status during sync.</param>
        /// <returns>Computed sync results.</returns>
        public Task<SyncResult> SynchronizeAsync(IProgress<ProgressArgs> progress = null)
            => this.SynchronizeAsync(SyncOptions.DefaultScopeName, null, SyncType.Normal, null, progress, CancellationToken.None);

        /// <summary>
        /// Launch a Synchronization based on scope DefaultScope.
        /// </summary>
        /// <param name="parameters">Parameters values for each of your setup filters.</param>
        /// <param name="progress">IProgress instance to get a progression status during sync.</param>
        /// <returns>Computed sync results.</returns>
        public Task<SyncResult> SynchronizeAsync(SyncParameters parameters, IProgress<ProgressArgs> progress = null)
            => this.SynchronizeAsync(SyncOptions.DefaultScopeName, null, SyncType.Normal, parameters, progress, CancellationToken.None);

        /// <summary>
        /// Launch a Synchronization based on scope DefaultScope.
        /// </summary>
        /// <param name="syncType">Synchronization mode: Normal, Reinitialize or ReinitializeWithUpload.</param>
        /// <param name="progress">IProgress instance to get a progression status during sync.</param>
        /// <returns>Computed sync results.</returns>
        public Task<SyncResult> SynchronizeAsync(SyncType syncType, IProgress<ProgressArgs> progress = null)
            => this.SynchronizeAsync(SyncOptions.DefaultScopeName, null, syncType, null, progress, CancellationToken.None);

        /// <summary>
        /// Launch a Synchronization based on scope DefaultScope.
        /// </summary>
        /// <param name="syncType">Synchronization mode: Normal, Reinitialize or ReinitializeWithUpload.</param>
        /// <param name="parameters">Parameters values for each of your setup filters.</param>
        /// <param name="progress">IProgress instance to get a progression status during sync.</param>
        /// <returns>Computed sync results.</returns>
        public Task<SyncResult> SynchronizeAsync(SyncType syncType, SyncParameters parameters, IProgress<ProgressArgs> progress = null)
            => this.SynchronizeAsync(SyncOptions.DefaultScopeName, null, syncType, parameters, progress, CancellationToken.None);

        /// <summary>
        /// Launch a Synchronization based on a named scope.
        /// </summary>
        /// <param name="scopeName">Named scope.</param>
        /// <param name="progress">IProgress instance to get a progression status during sync.</param>
        /// <returns>Computed sync results.</returns>
        public Task<SyncResult> SynchronizeAsync(string scopeName, IProgress<ProgressArgs> progress = null)
            => this.SynchronizeAsync(scopeName, null, SyncType.Normal, null, progress, CancellationToken.None);

        /// <summary>
        /// Launch a Synchronization based on a named scope.
        /// </summary>
        /// <param name="scopeName">Named scope.</param>
        /// <param name="parameters">Parameters values for each of your setup filters.</param>
        /// <param name="progress">IProgress instance to get a progression status during sync.</param>
        /// <returns>Computed sync results.</returns>
        public Task<SyncResult> SynchronizeAsync(string scopeName, SyncParameters parameters, IProgress<ProgressArgs> progress = null)
            => this.SynchronizeAsync(scopeName, null, SyncType.Normal, parameters, progress, CancellationToken.None);

        /// <summary>
        /// Launch a Synchronization based on a named scope.
        /// </summary>
        /// <param name="scopeName">Named scope.</param>
        /// <param name="syncType">Synchronization mode: Normal, Reinitialize or ReinitializeWithUpload.</param>
        /// <param name="progress">IProgress instance to get a progression status during sync.</param>
        /// <returns>Computed sync results.</returns>
        public Task<SyncResult> SynchronizeAsync(string scopeName, SyncType syncType, IProgress<ProgressArgs> progress = null)
            => this.SynchronizeAsync(scopeName, null, syncType, null, progress, CancellationToken.None);

        /// <summary>
        /// Launch a Synchronization based on a named scope.
        /// </summary>
        /// <param name="scopeName">Named scope.</param>
        /// <param name="syncType">Synchronization mode: Normal, Reinitialize or ReinitializeWithUpload.</param>
        /// <param name="parameters">Parameters values for each of your setup filters.</param>
        /// <param name="progress">IProgress instance to get a progression status during sync.</param>
        /// <returns>Computed sync results.</returns>
        public Task<SyncResult> SynchronizeAsync(string scopeName, SyncType syncType, SyncParameters parameters, IProgress<ProgressArgs> progress = null)
            => this.SynchronizeAsync(scopeName, null, syncType, parameters, progress, CancellationToken.None);

        // ---------------------------------------------
        // string[] tables
        // ---------------------------------------------

        /// <summary>
        /// Launch a Synchronization based on scope DefaultScope.
        /// </summary>
        /// <param name="tables">Tables list to synchronize.</param>
        /// <param name="progress">IProgress instance to get a progression status during sync.</param>
        /// <returns>Computed sync results.</returns>
        public Task<SyncResult> SynchronizeAsync(string[] tables, IProgress<ProgressArgs> progress = default) =>
            this.SynchronizeAsync(SyncOptions.DefaultScopeName, new SyncSetup(tables), SyncType.Normal, null, progress, CancellationToken.None);

        /// <summary>
        /// Launch a Synchronization based on scope DefaultScope.
        /// </summary>
        /// <param name="tables">Tables list to synchronize.</param>
        /// <param name="parameters">Parameters values for each of your setup filters.</param>
        /// <param name="progress">IProgress instance to get a progression status during sync.</param>
        /// <returns>Computed sync results.</returns>
        public Task<SyncResult> SynchronizeAsync(string[] tables, SyncParameters parameters, IProgress<ProgressArgs> progress = default) =>
            this.SynchronizeAsync(SyncOptions.DefaultScopeName, new SyncSetup(tables), SyncType.Normal, parameters, progress, CancellationToken.None);

        /// <summary>
        /// Launch a Synchronization based on scope DefaultScope.
        /// </summary>
        /// <param name="tables">Tables list to synchronize.</param>
        /// <param name="syncType">Synchronization mode: Normal, Reinitialize or ReinitializeWithUpload.</param>
        /// <param name="progress">IProgress instance to get a progression status during sync.</param>
        /// <returns>Computed sync results.</returns>
        public Task<SyncResult> SynchronizeAsync(string[] tables, SyncType syncType, IProgress<ProgressArgs> progress = default) =>
            this.SynchronizeAsync(SyncOptions.DefaultScopeName, new SyncSetup(tables), syncType, null, progress, CancellationToken.None);

        /// <summary>
        /// Launch a Synchronization based on scope DefaultScope.
        /// </summary>
        /// <param name="tables">Tables list to synchronize.</param>
        /// <param name="syncType">Synchronization mode: Normal, Reinitialize or ReinitializeWithUpload.</param>
        /// <param name="parameters">Parameters values for each of your setup filters.</param>
        /// <param name="progress">IProgress instance to get a progression status during sync.</param>
        /// <returns>Computed sync results.</returns>
        public Task<SyncResult> SynchronizeAsync(string[] tables, SyncType syncType, SyncParameters parameters, IProgress<ProgressArgs> progress = default) =>
            this.SynchronizeAsync(SyncOptions.DefaultScopeName, new SyncSetup(tables), syncType, parameters, progress, CancellationToken.None);

        /// <summary>
        /// Launch a Synchronization based on a named scope.
        /// </summary>
        /// <param name="scopeName">Named scope.</param>
        /// <param name="tables">Tables list to synchronize.</param>
        /// <param name="progress">IProgress instance to get a progression status during sync.</param>
        /// <returns>Computed sync results.</returns>
        public Task<SyncResult> SynchronizeAsync(string scopeName, string[] tables, IProgress<ProgressArgs> progress = default) =>
            this.SynchronizeAsync(scopeName, new SyncSetup(tables), SyncType.Normal, null, progress, CancellationToken.None);

        /// <summary>
        /// Launch a Synchronization based on a named scope.
        /// </summary>
        /// <param name="scopeName">Named scope.</param>
        /// <param name="tables">Tables list to synchronize.</param>
        /// <param name="parameters">Parameters values for each of your setup filters.</param>
        /// <param name="progress">IProgress instance to get a progression status during sync.</param>
        /// <returns>Computed sync results.</returns>
        public Task<SyncResult> SynchronizeAsync(string scopeName, string[] tables, SyncParameters parameters, IProgress<ProgressArgs> progress = default) =>
            this.SynchronizeAsync(scopeName, new SyncSetup(tables), SyncType.Normal, parameters, progress, CancellationToken.None);

        /// <summary>
        /// Launch a Synchronization based on a named scope.
        /// </summary>
        /// <param name="scopeName">Named scope.</param>
        /// <param name="tables">Tables list to synchronize.</param>
        /// <param name="syncType">Synchronization mode: Normal, Reinitialize or ReinitializeWithUpload.</param>
        /// <param name="progress">IProgress instance to get a progression status during sync.</param>
        /// <returns>Computed sync results.</returns>
        public Task<SyncResult> SynchronizeAsync(string scopeName, string[] tables, SyncType syncType, IProgress<ProgressArgs> progress = default) =>
            this.SynchronizeAsync(scopeName, new SyncSetup(tables), syncType, null, progress, CancellationToken.None);

        /// <summary>
        /// Launch a Synchronization based on a named scope.
        /// </summary>
        /// <param name="scopeName">Named scope.</param>
        /// <param name="tables">Tables list to synchronize.</param>
        /// <param name="syncType">Synchronization mode: Normal, Reinitialize or ReinitializeWithUpload.</param>
        /// <param name="parameters">Parameters values for each of your setup filters.</param>
        /// <param name="progress">IProgress instance to get a progression status during sync.</param>
        /// <returns>Computed sync results.</returns>
        public Task<SyncResult> SynchronizeAsync(string scopeName, string[] tables, SyncType syncType, SyncParameters parameters, IProgress<ProgressArgs> progress = default) =>
            this.SynchronizeAsync(scopeName, new SyncSetup(tables), syncType, parameters, progress, CancellationToken.None);

        // ---------------------------------------------
        // SyncSetup setup
        // ---------------------------------------------

        /// <summary>
        /// Launch a Synchronization based on scope DefaultScope.
        /// </summary>
        /// <param name="setup">Setup instance containing the table list and optionnally columns.</param>
        /// <param name="progress">IProgress instance to get a progression status during sync.</param>
        /// <returns>Computed sync results.</returns>
        public Task<SyncResult> SynchronizeAsync(SyncSetup setup, IProgress<ProgressArgs> progress = default)
            => this.SynchronizeAsync(SyncOptions.DefaultScopeName, setup, SyncType.Normal, null, progress, CancellationToken.None);

        /// <summary>
        /// Launch a Synchronization based on scope DefaultScope.
        /// </summary>
        /// <param name="setup">Setup instance containing the table list and optionnally columns.</param>
        /// <param name="parameters">Parameters values for each of your setup filters.</param>
        /// <param name="progress">IProgress instance to get a progression status during sync.</param>
        /// <returns>Computed sync results.</returns>
        public Task<SyncResult> SynchronizeAsync(SyncSetup setup, SyncParameters parameters, IProgress<ProgressArgs> progress = default)
            => this.SynchronizeAsync(SyncOptions.DefaultScopeName, setup, SyncType.Normal, parameters, progress, CancellationToken.None);

        /// <summary>
        /// Launch a Synchronization based on scope DefaultScope.
        /// </summary>
        /// <param name="setup">Setup instance containing the table list and optionnally columns.</param>
        /// <param name="syncType">Synchronization mode: Normal, Reinitialize or ReinitializeWithUpload.</param>
        /// <param name="progress">IProgress instance to get a progression status during sync.</param>
        /// <returns>Computed sync results.</returns>
        public Task<SyncResult> SynchronizeAsync(SyncSetup setup, SyncType syncType, IProgress<ProgressArgs> progress = default)
            => this.SynchronizeAsync(SyncOptions.DefaultScopeName, setup, syncType, null, progress, CancellationToken.None);

        /// <summary>
        /// Launch a Synchronization based on scope DefaultScope.
        /// </summary>
        /// <param name="setup">Setup instance containing the table list and optionnally columns.</param>
        /// <param name="syncType">Synchronization mode: Normal, Reinitialize or ReinitializeWithUpload.</param>
        /// <param name="parameters">Parameters values for each of your setup filters.</param>
        /// <param name="progress">IProgress instance to get a progression status during sync.</param>
        /// <returns>Computed sync results.</returns>
        public Task<SyncResult> SynchronizeAsync(SyncSetup setup, SyncType syncType, SyncParameters parameters, IProgress<ProgressArgs> progress = default)
            => this.SynchronizeAsync(SyncOptions.DefaultScopeName, setup, syncType, parameters, progress, CancellationToken.None);

        /// <summary>
        /// Launch a Synchronization based on a named scope.
        /// </summary>
        /// <param name="scopeName">Named scope.</param>
        /// <param name="setup">Setup instance containing the table list and optionnally columns.</param>
        /// <param name="progress">IProgress instance to get a progression status during sync.</param>
        /// <returns>Computed sync results.</returns>
        public Task<SyncResult> SynchronizeAsync(string scopeName, SyncSetup setup, IProgress<ProgressArgs> progress = default)
            => this.SynchronizeAsync(scopeName, setup, SyncType.Normal, null, progress, CancellationToken.None);

        /// <summary>
        /// Launch a Synchronization based on a named scope.
        /// </summary>
        /// <param name="scopeName">Named scope.</param>
        /// <param name="setup">Setup instance containing the table list and optionnally columns.</param>
        /// <param name="parameters">Parameters values for each of your setup filters.</param>
        /// <param name="progress">IProgress instance to get a progression status during sync.</param>
        /// <returns>Computed sync results.</returns>
        public Task<SyncResult> SynchronizeAsync(string scopeName, SyncSetup setup, SyncParameters parameters, IProgress<ProgressArgs> progress = default)
            => this.SynchronizeAsync(scopeName, setup, SyncType.Normal, parameters, progress, CancellationToken.None);

        /// <summary>
        /// Launch a Synchronization based on a named scope.
        /// </summary>
        /// <param name="scopeName">Named scope.</param>
        /// <param name="setup">Setup instance containing the table list and optionnally columns.</param>
        /// <param name="syncType">Synchronization mode: Normal, Reinitialize or ReinitializeWithUpload.</param>
        /// <param name="progress">IProgress instance to get a progression status during sync.</param>
        /// <returns>Computed sync results.</returns>
        public Task<SyncResult> SynchronizeAsync(string scopeName, SyncSetup setup, SyncType syncType, IProgress<ProgressArgs> progress = default)
            => this.SynchronizeAsync(scopeName, setup, syncType, null, progress, CancellationToken.None);

        /// <summary>
        /// Launch a Synchronization based on a named scope.
        /// </summary>
        /// <param name="scopeName">Named scope.</param>
        /// <param name="setup">Setup instance containing the table list and optionnally columns.</param>
        /// <param name="syncType">Synchronization mode: Normal, Reinitialize or ReinitializeWithUpload.</param>
        /// <param name="parameters">Parameters values for each of your setup filters.</param>
        /// <param name="progress">IProgress instance to get a progression status during sync.</param>
        /// <returns>Computed sync results.</returns>
        public Task<SyncResult> SynchronizeAsync(string scopeName, SyncSetup setup, SyncType syncType, SyncParameters parameters, IProgress<ProgressArgs> progress = default)
            => this.SynchronizeAsync(scopeName, setup, syncType, parameters, progress, CancellationToken.None);

        // ---------------------------------------------
    }
}