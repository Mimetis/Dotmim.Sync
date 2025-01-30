#if NET6_0 || NET8_0_OR_GREATER
using MySqlConnector;
#elif NETSTANDARD
using MySql.Data.MySqlClient;
#endif

#if MARIADB
namespace Dotmim.Sync.MariaDB
#elif MYSQL
namespace Dotmim.Sync.MySql
#endif
{
    /// <summary>
    ///     Detects the exceptions caused by SQL Server transient failures.
    /// </summary>
    public static class MySqlTransientExceptionDetector
    {
        /// <summary>
        ///     Detects the exceptions caused by MySQL transient failures.
        /// </summary>
        public static bool ShouldRetryOn(MySqlException mysqlException)
        {
            switch (mysqlException.Number)
            {
                case 1205: // Lock wait timeout exceeded; try restarting transaction
                case 1213: // Deadlock found when trying to get lock; try restarting transaction
                case 1614: // Transaction branch was rolled back: deadlock was detected
                case 2013: // Lost connection to MySQL server during query
                    return true;
                default:
                    break;
            }

            return false;
        }
    }
}