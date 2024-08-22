using System;
using System.Globalization;

namespace Dotmim.Sync
{
    /// <summary>
    /// Contains global settings for Sync.
    /// </summary>
    public static class SyncGlobalization
    {
        static SyncGlobalization()
        {
        }

        /// <summary>
        /// Gets or Sets the string comparison used when comparing string from data source.
        /// Default is Invariant Ignore Case.
        /// </summary>
        public static StringComparison DataSourceStringComparison { get; set; }
            = StringComparison.InvariantCultureIgnoreCase;

        /// <summary>
        /// Gets or Sets the number decimal separator used to parse decimal float and double from data source.
        /// Default is Invariant Number Decimal Separator (".").
        /// </summary>
        public static string DataSourceNumberDecimalSeparator { get; set; }
            = CultureInfo.InvariantCulture.NumberFormat.NumberDecimalSeparator;

        /// <summary>
        /// Gets a boolean indicating if the StringComparison is case sensitive.
        /// </summary>
        public static bool IsCaseSensitive() => DataSourceStringComparison.HasFlag(
                StringComparison.InvariantCulture | StringComparison.CurrentCulture | StringComparison.Ordinal);
    }
}