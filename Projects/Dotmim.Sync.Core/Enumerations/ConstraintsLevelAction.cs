namespace Dotmim.Sync.Enumerations
{
    /// <summary>
    /// Defines on which level of the database the constraints are applied.
    /// </summary>
    public enum ConstraintsLevelAction
    {
        /// <summary>
        /// Constraints are applied on the database level (usually on Sqlite).
        /// </summary>
        OnDatabaseLevel,

        /// <summary>
        /// Constraints are applied on the session level (usually on SqlServer).
        /// </summary>
        OnSessionLevel,

        /// <summary>
        /// Constraints are applied on the table level (usually on MySql / PostgreSQL).
        /// </summary>
        OnTableLevel,
    }
}