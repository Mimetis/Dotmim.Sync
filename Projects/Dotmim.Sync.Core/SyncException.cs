using Dotmim.Sync.Enumerations;
using System;
using System.Collections.Generic;
using System.Text;
using System.Data.Common;
using System.Data;

namespace Dotmim.Sync
{

    /// <summary>
    /// Exception
    /// </summary>
    public class SyncException : Exception
    {
        public SyncException(string message, SyncStage stage) : base(message)
        {
            this.SyncStage = stage;
        }
        public SyncException(String message, SyncStage stage, string providerName, SyncExceptionType type = SyncExceptionType.Unknown, int errorCode = -1) : base(message)
        {
            this.SyncStage = stage;
            this.ProviderName = providerName;
            this.ErrorCode = errorCode;
            this.Type = type;
        }

        public SyncException(Exception exception, SyncStage stage, string providerName) : base(exception.Message, exception)
        {

            this.SyncStage = stage;
            this.ProviderName = providerName;
            if (exception is SyncException)
            {
                this.ErrorCode = ((SyncException)exception).ErrorCode;
                this.Type = ((SyncException)exception).Type;
            }
            if (exception is DbException)
            {
                this.ErrorCode = ((DbException)exception).ErrorCode;
                this.Type = SyncExceptionType.Data;
            }
            if (exception is DataException)
            {
                this.Type = SyncExceptionType.Data;
            }
            else if (exception is ArgumentException)
            {
                this.Type = SyncExceptionType.Argument;
            }
            else if (exception is ArgumentOutOfRangeException)
            {
                this.Type = SyncExceptionType.ArgumentOutOfRange;
            }
            else if (exception is FormatException)
            {
                this.Type = SyncExceptionType.Format;
            }
            else if (exception is IndexOutOfRangeException)
            {
                this.Type = SyncExceptionType.IndexOutOfRange;
            }
            else if (exception is InsufficientMemoryException)
            {
                this.Type = SyncExceptionType.InsufficientMemory;
            }
            else if (exception is InProgressException)
            {
                this.Type = SyncExceptionType.InProgress;
            }
            else if (exception is InvalidCastException)
            {
                this.Type = SyncExceptionType.InvalidCast;
            }
            else if (exception is InvalidExpressionException)
            {
                this.Type = SyncExceptionType.InvalidExpression;
            }
            else if (exception is InvalidOperationException)
            {
                this.Type = SyncExceptionType.InvalidOperation;
            }
            else if (exception is KeyNotFoundException)
            {
                this.Type = SyncExceptionType.KeyNotFound;
            }
            else if (exception is NotImplementedException)
            {
                this.Type = SyncExceptionType.NotImplemented;
            }
            else if (exception is NotSupportedException)
            {
                this.Type = SyncExceptionType.NotSupported;
            }
            else if (exception is NullReferenceException)
            {
                this.Type = SyncExceptionType.NullReference;
            }
            else if (exception is ObjectDisposedException)
            {
                this.Type = SyncExceptionType.ObjectDisposed;
            }
            else if (exception is OperationCanceledException)
            {
                this.Type = SyncExceptionType.OperationCanceled;
            }
            else if (exception is OutOfDateException)
            {
                this.Type = SyncExceptionType.OutOfDate;
            }
            else if (exception is OutOfMemoryException)
            {
                this.Type = SyncExceptionType.OutOfMemory;
            }
            else if (exception is OverflowException)
            {
                this.Type = SyncExceptionType.Overflow;
            }
            else if (exception is PlatformNotSupportedException)
            {
                this.Type = SyncExceptionType.PlatformNotSupported;
            }
            else if (exception is TimeoutException)
            {
                this.Type = SyncExceptionType.Timeout;
            }
            else if (exception is UnauthorizedAccessException)
            {
                this.Type = SyncExceptionType.UnauthorizedAccess;
            }
            else if (exception is UriFormatException)
            {
                this.Type = SyncExceptionType.UriFormat;
            }
            else
            {
                this.Type = SyncExceptionType.Unknown;
            }
        }

        /// <summary>
        /// Exception type
        /// </summary>
        public SyncExceptionType Type { get; set; }

        /// <summary>
        /// Number
        /// </summary>
        public int ErrorCode { get; set; }

        /// <summary>
        /// Provider triggering the exception
        /// </summary>
        public String ProviderName { get; set; }

        /// <summary>
        /// Sync stage when exception occured
        /// </summary>
        public SyncStage SyncStage { get; }
    }

    /// <summary>
    /// Represents an out of date exception
    /// </summary>
    public class OutOfDateException : Exception
    {
        public OutOfDateException() { }
        public OutOfDateException(string message) : base(message) { }
        public OutOfDateException(Exception exception) : base(exception.Message, exception) { }

    }

    public class InProgressException : Exception
    {
        public InProgressException() { }
        public InProgressException(string message) : base(message) { }
        public InProgressException(Exception exception) : base(exception.Message, exception) { }

    }
    public class RollbackException : Exception
    {
        public RollbackException() { }
        public RollbackException(string message) : base(message) { }
        public RollbackException(Exception exception) : base(exception.Message, exception) { }

    }
    
    public enum SyncExceptionType
    {
        ArgumentOutOfRange,
        Argument,
        Data,
        Format,
        IndexOutOfRange,
        InProgress,
        InsufficientMemory,
        InvalidCast,
        InvalidExpression,
        InvalidOperation,
        KeyNotFound,
        NotImplemented,
        NotSupported,
        NullReference,
        ObjectDisposed,
        OperationCanceled,
        OutOfDate,
        OutOfMemory,
        Overflow,
        PlatformNotSupported,
        Rollback,
        Timeout,
        UnauthorizedAccess,
        UriFormat,
        Unknown,
    }
}
