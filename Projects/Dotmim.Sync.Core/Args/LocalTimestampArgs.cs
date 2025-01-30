﻿using Dotmim.Sync.Enumerations;
using System;
using System.Data.Common;
using System.Threading.Tasks;

namespace Dotmim.Sync
{

    /// <summary>
    /// Event args raised when you are getting the local timestamp from the current provider.
    /// </summary>
    public class LocalTimestampLoadingArgs : ProgressArgs
    {
        /// <inheritdoc cref="LocalTimestampLoadingArgs"/>
        public LocalTimestampLoadingArgs(SyncContext context, DbCommand command, DbConnection connection, DbTransaction transaction)
            : base(context, connection, transaction)
        {
            this.Command = command;
        }

        /// <summary>
        /// Gets or sets a value indicating whether the operation should be canceled.
        /// </summary>
        public bool Cancel { get; set; }

        /// <summary>
        /// Gets or sets the command to be executed to get the local timestamp from the current provider.
        /// </summary>
        public DbCommand Command { get; set; }

        /// <inheritdoc cref="ProgressArgs.ProgressLevel"/>
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;

        /// <inheritdoc cref="ProgressArgs.Message"/>/>
        public override string Message => $"Getting Local Timestamp.";

        /// <inheritdoc cref="ProgressArgs.EventId"/>
        public override int EventId => 2000;
    }

    /// <summary>
    /// Event args raised when the local timestamp has been loaded from the current provider.
    /// </summary>
    public class LocalTimestampLoadedArgs : ProgressArgs
    {
        /// <inheritdoc cref="LocalTimestampLoadedArgs"/>
        public LocalTimestampLoadedArgs(SyncContext context, long localTimestamp, DbConnection connection, DbTransaction transaction)
            : base(context, connection, transaction) => this.LocalTimestamp = localTimestamp;

        /// <summary>
        /// Gets the local timestamp loaded from the current provider.
        /// </summary>
        public long LocalTimestamp { get; }

        /// <inheritdoc cref="ProgressArgs.ProgressLevel"/>
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;

        /// <inheritdoc cref="ProgressArgs.Message"/>/>
        public override string Message => $"Local Timestamp Loaded:{this.LocalTimestamp}.";

        /// <inheritdoc cref="ProgressArgs.EventId"/>
        public override int EventId => 2050;
    }

    /// <summary>
    /// Interceptor called before and after getting the local timestamp from the current provider.
    /// </summary>
    public partial class InterceptorsExtensions
    {
        /// <summary>
        /// Intercept the provider action when a database is reading a timestamp.
        /// </summary>
        public static Guid OnLocalTimestampLoading(this BaseOrchestrator orchestrator, Action<LocalTimestampLoadingArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider action when a database is reading a timestamp.
        /// </summary>
        public static Guid OnLocalTimestampLoading(this BaseOrchestrator orchestrator, Func<LocalTimestampLoadingArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider action when a database has read a timestamp.
        /// </summary>
        public static Guid OnLocalTimestampLoaded(this BaseOrchestrator orchestrator, Action<LocalTimestampLoadedArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider action when a database has read a timestamp.
        /// </summary>
        public static Guid OnLocalTimestampLoaded(this BaseOrchestrator orchestrator, Func<LocalTimestampLoadedArgs, Task> action)
            => orchestrator.AddInterceptor(action);
    }
}