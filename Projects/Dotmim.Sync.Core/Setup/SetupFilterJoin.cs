using Dotmim.Sync.Builders;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Runtime.Serialization;
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

        public SetupFilterOn()
        {

        }

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


    [DataContract(Name = "sfj"), Serializable]
    public class SetupFilterJoin : SyncNamedItem<SetupFilterJoin>
    {
        [DataMember(Name = "je", IsRequired = true, Order = 1)]
        public Join JoinEnum { get; set; }

        [DataMember(Name = "tn", IsRequired = true, Order = 2)]
        public string TableName { get; set; }

        [DataMember(Name = "ltn", IsRequired = true, Order = 3)]
        public string LeftTableName { get; set; }

        [DataMember(Name = "lcn", IsRequired = true, Order = 4)]
        public string LeftColumnName { get; set; }

        [DataMember(Name = "rtn", IsRequired = true, Order = 5)]
        public string RightTableName { get; set; }

        [DataMember(Name = "rcn", IsRequired = true, Order = 6)]
        public string RightColumnName { get; set; }

        /// <summary>
        /// ctor for serializer, don't use as it, prefer the second ctor
        /// </summary>
        public SetupFilterJoin()
        {

        }

        public SetupFilterJoin(Join joinEnum, string tableName, string leftTableName, string leftColumnName, string rightTableName, string rightColumnName)
        {
            this.JoinEnum = joinEnum;
            this.TableName = tableName;
            this.LeftTableName = leftTableName;
            this.LeftColumnName = leftColumnName;
            this.RightTableName = rightTableName;
            this.RightColumnName = rightColumnName;
        }

        /// <summary>
        /// Get all comparable fields to determine if two instances are identifed are same by name
        /// </summary>
        public override IEnumerable<string> GetAllNamesProperties()
        {
            yield return this.JoinEnum.ToString();
            yield return this.TableName;
            yield return this.LeftColumnName;
            yield return this.LeftTableName;
            yield return this.RightColumnName;
            yield return this.RightTableName;
        }
    }
}
