using Dotmim.Sync.Builders;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Text;

namespace Dotmim.Sync
{

    public enum Join
    {
        Inner,
        Left,
        Right,
        Outer
    }

    public enum InnerTable
    {
        Base,
        Side
    }

    public class SetupFilterOn
    {
        private Join joinEnum;
        private string tableName;
        private SetupFilter filter;

        public SetupFilterOn(SetupFilter filter, Join joinEnum, string tableName)
        {
            this.filter = filter;
            this.joinEnum = joinEnum;
            this.tableName = tableName;
        }

        public SetupFilterOn On(string leftTableName, string leftColumnName, string rightTableName, string rightColumnName)
        {
            var join = new SetupFilterJoin(this.joinEnum, this.tableName, leftTableName, leftColumnName, rightTableName, rightColumnName);
            this.filter.AddJoin(join);
            return this;
        }

        [EditorBrowsable(EditorBrowsableState.Never)]
        public override bool Equals(object obj) => base.Equals(obj);
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override int GetHashCode() => base.GetHashCode();
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override string ToString() => base.ToString();
    }


    public struct SetupFilterJoin
    {
        internal Join joinEnum;
        internal string tableName;
        internal string leftTableName;
        internal string leftColumnName;
        internal string rightTableName;
        internal string rightColumnName;

        public SetupFilterJoin(Join joinEnum, string tableName, string leftTableName, string leftColumnName, string rightTableName, string rightColumnName)
        {
            this.joinEnum = joinEnum;
            this.tableName = tableName;
            this.leftTableName = leftTableName;
            this.leftColumnName = leftColumnName;
            this.rightTableName = rightTableName;
            this.rightColumnName = rightColumnName;
        }

    }


}
