using System;
using System.Collections.Generic;
using System.Runtime.Serialization;

namespace Dotmim.Sync
{

    /// <summary>
    /// Enum used to define the join type for a filter.
    /// </summary>
    public enum Join
    {
        /// <summary>
        /// Inner join.
        /// </summary>
        Inner,

        /// <summary>
        /// Left join.
        /// </summary>
        Left,

        /// <summary>
        /// Right join.
        /// </summary>
        Right,

        /// <summary>
        /// Outer join.
        /// </summary>
        Outer,
    }

    /// <summary>
    /// Enum used to define the inner table for a filter.
    /// </summary>
    public enum InnerTable
    {
        /// <summary>
        /// Base table.
        /// </summary>
        Base,

        /// <summary>
        /// Side table.
        /// </summary>
        Side,
    }

    /// <summary>
    /// Gets the table that is joined to the base table.
    /// </summary>
    public class SetupFilterOn
    {
        private Join joinEnum;
        private string tableName;
        private string schemaName;
        private SetupFilter filter;

        /// <inheritdoc cref="SetupFilterOn"/>/>
        public SetupFilterOn()
        {
        }

        /// <inheritdoc cref="SetupFilterOn"/>/>
        public SetupFilterOn(SetupFilter filter, Join joinEnum, string tableName, string schemaName = null)
        {
            this.filter = filter;
            this.joinEnum = joinEnum;
            this.tableName = tableName;
            this.schemaName = schemaName;
        }

        /// <summary>
        /// Set the join with the right table.
        /// </summary>
        public SetupFilterOn On(string leftTableName, string leftColumnName, string rightTableName, string rightColumnName, string leftTableSchemaName = null, string rightTableSchemaName = null)
        {
            var join = new SetupFilterJoin(this.joinEnum, this.tableName, leftTableName, leftColumnName, rightTableName, rightColumnName, this.schemaName, leftTableSchemaName, rightTableSchemaName);
            this.filter.AddJoin(join);
            return this;
        }
    }

    /// <summary>
    /// Setup filter join.
    /// </summary>
    [DataContract(Name = "sfj"), Serializable]
    public class SetupFilterJoin : SyncNamedItem<SetupFilterJoin>
    {
        /// <summary>
        /// Gets or sets the join type.
        /// </summary>
        [DataMember(Name = "je", IsRequired = true, Order = 1)]
        public Join JoinEnum { get; set; }

        /// <summary>
        /// Gets or sets the table name.
        /// </summary>
        [DataMember(Name = "tn", IsRequired = true, Order = 2)]
        public string TableName { get; set; }

        /// <summary>
        /// Gets or sets the left table name.
        /// </summary>
        [DataMember(Name = "ltn", IsRequired = true, Order = 3)]
        public string LeftTableName { get; set; }

        /// <summary>
        /// Gets or sets the left column name.
        /// </summary>
        [DataMember(Name = "lcn", IsRequired = true, Order = 4)]
        public string LeftColumnName { get; set; }

        /// <summary>
        /// Gets or sets the right table name.
        /// </summary>
        [DataMember(Name = "rtn", IsRequired = true, Order = 5)]
        public string RightTableName { get; set; }

        /// <summary>
        /// Gets or sets the right column name.
        /// </summary>
        [DataMember(Name = "rcn", IsRequired = true, Order = 6)]
        public string RightColumnName { get; set; }

        /// <summary>
        /// Gets or sets the schema name.
        /// </summary>
        [DataMember(Name = "tsn", IsRequired = false, Order = 7, EmitDefaultValue = false)]
        public string TableSchemaName { get; set; }

        /// <summary>
        /// Gets or sets the left table schema name.
        /// </summary>
        [DataMember(Name = "ltsn", IsRequired = false, Order = 8, EmitDefaultValue = false)]
        public string LeftTableSchemaName { get; set; }

        /// <summary>
        /// Gets or sets the right table schema name.
        /// </summary>
        [DataMember(Name = "rtsn", IsRequired = false, Order = 9, EmitDefaultValue = false)]
        public string RightTableSchemaName { get; set; }

        /// <inheritdoc cref="SetupFilterJoin"/>
        public SetupFilterJoin()
        {
        }

        /// <inheritdoc cref="SetupFilterJoin"/>
        public SetupFilterJoin(Join joinEnum, string tableName, string leftTableName, string leftColumnName, string rightTableName, string rightColumnName)
        {
            this.JoinEnum = joinEnum;
            this.TableName = tableName;
            this.LeftTableName = leftTableName;
            this.LeftColumnName = leftColumnName;
            this.RightTableName = rightTableName;
            this.RightColumnName = rightColumnName;
        }

        /// <inheritdoc cref="SetupFilterJoin"/>
        public SetupFilterJoin(Join joinEnum, string tableName,
            string leftTableName, string leftColumnName,
            string rightTableName, string rightColumnName,
            string tableSchemaName, string leftTableSchemaName, string rightTableSchemaName)
        {
            this.JoinEnum = joinEnum;
            this.TableName = tableName;
            this.LeftTableName = leftTableName;
            this.LeftColumnName = leftColumnName;
            this.RightTableName = rightTableName;
            this.RightColumnName = rightColumnName;
            this.TableSchemaName = tableSchemaName;
            this.LeftTableSchemaName = leftTableSchemaName;
            this.RightTableSchemaName = rightTableSchemaName;
        }

        /// <inheritdoc cref="SyncNamedItem{T}.GetAllNamesProperties"/>
        public override IEnumerable<string> GetAllNamesProperties()
        {
            yield return this.JoinEnum.ToString();
            yield return this.TableName;
            yield return this.LeftColumnName;
            yield return this.LeftTableName;
            yield return this.RightColumnName;
            yield return this.RightTableName;
            yield return this.TableSchemaName;
            yield return this.LeftTableSchemaName;
            yield return this.RightTableSchemaName;
        }
    }
}