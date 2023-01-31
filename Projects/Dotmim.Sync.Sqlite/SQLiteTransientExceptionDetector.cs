using Microsoft.Data.Sqlite;
using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Sqlite
{
    /// <summary>
    ///     Detects the exceptions caused by SQL Server transient failures.
    /// </summary>
    public static class SqliteTransientExceptionDetector
    {
        /// <summary>
        ///     Detects the exceptions caused by SQL Server transient failures.
        /// </summary>
        public static bool ShouldRetryOn(SqliteException sqliteException)
        {
            switch (sqliteException.SqliteErrorCode)
            {
                case 5: // SQLITE_BUSY
                case 6: // SQLITE_LOCKED
                case 7: // SQLITE_NOMEM
                case 9: // SQLITE_INTERRUPT
                case 10: // SQLITE_IOERR
                case 11: // SQLITE_CORRUPT
                case 12: // SQLITE_NOTFOUND
                case 14: // SQLITE_CANTOPEN
                case 15: // SQLITE_PROTOCOL
                case 22: // SQLITE_NOLFS
                case 26: // SQLITE_NOTADB
                case 27: // SQLITE_NOTICE
                case 28: // SQLITE_WARNING
                    return true;
                default:
                    break;
            }
            return false;

        }
    }
}
