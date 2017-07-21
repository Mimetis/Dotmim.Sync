using Dotmim.Sync.Enumerations;
using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Core
{
    public class SyncException : Exception
    {
        public SyncException()
        {

        }

        public SyncException(string message, SyncStage stage,  SyncExceptionType type = SyncExceptionType.Unknown) : base(message)
        {
            this.SyncStage = stage;
            this.ExceptionType = type;
        }


        public SyncException(string message, SyncStage stage, Exception exception, SyncExceptionType type = SyncExceptionType.Unknown) : base(message, exception)
        {
            this.ExceptionType = type;
            this.SyncStage = stage;
        }

        /// <summary>
        /// Gets or Sets the Sync Exception type raised
        /// </summary>
        public SyncExceptionType ExceptionType { get; set; }

        /// <summary>
        /// Gets or Sets the stage when the exception has raised
        /// </summary>
        public SyncStage SyncStage { get; set; }

        /// <summary>
        /// Create a rollback exception to rollback the Sync session
        /// </summary>
        /// <param name="context"></param>
        internal static SyncException CreateRollbackException(SyncStage stage)
        {
            SyncException syncException = new SyncException("User rollback action.", stage, SyncExceptionType.Rollback);
            return syncException;
        }

        internal static SyncException CreateUnknowException(SyncStage stage, Exception ex)
        {
            SyncException syncException = new SyncException("Unknown error has occured", stage, ex, SyncExceptionType.Unknown);
            return syncException;

        }

        public override string ToString()
        {
            return $@"Error occured during stage {SyncStage.ToString()} of type: {ExceptionType.ToString()}. {Message}";

        }

        internal static Exception CreateInProgressException(SyncStage syncStage)
        {
            SyncException syncException = new SyncException("Session already in progress", syncStage, SyncExceptionType.Rollback);
            return syncException;
        }

        internal static SyncException CreateOperationCanceledException(SyncStage syncStage, OperationCanceledException oce)
        {
            SyncException syncException = new SyncException("Operation canceled.", syncStage, SyncExceptionType.OperationCanceled);
            return syncException;
        }
    }


    /// <summary>
    /// Sync Exception type.
    /// </summary>
    public enum SyncExceptionType
    {
        DataStore,
        Conflict,
        SyncInProgress,
        OperationCanceled,
        Rollback,
        Unknown,
    }
}
