using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;

namespace Dotmim.Sync
{
    [DataContract(Name = "sfj"), Serializable]
    public class SyncFilterJoin : SyncNamedItem<SyncFilterJoin>
    {

        /// <summary>
        /// Ensure filter parameter as the correct schema (since the property is not serialized)
        /// </summary>
        public void EnsureFilterJoin(SyncSet schema)
        {
            this.Schema = schema;
        }

        /// <summary>
        /// Gets the ShemaTable's SyncSchema
        /// </summary>
        [IgnoreDataMember]
        public SyncSet Schema { get; set; }

        [DataMember(Name = "je", IsRequired = true, Order = 1)]
        public Join JoinEnum { get; set; }

        [DataMember(Name = "tbl", IsRequired = true, Order = 2)]
        public string TableName { get; set; }

        [DataMember(Name = "ltbl", IsRequired = true, Order = 3)]
        public string LeftTableName { get; set; }

        [DataMember(Name = "lcol", IsRequired = true, Order = 4)]
        public string LeftColumnName { get; set; }

        [DataMember(Name = "rtbl", IsRequired = true, Order = 5)]
        public string RightTableName { get; set; }

        [DataMember(Name = "rcol", IsRequired = true, Order = 6)]
        public string RightColumnName { get; set; }

        public SyncFilterJoin()
        {

        }

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
