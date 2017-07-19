using Dotmim.Sync.Data;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.Core.Scope
{
    /// <summary>
    /// Mapping sur la table ScopeInfo
    /// </summary>
    public class ScopeInfo
    {

        /// <summary>
        /// Scope name. Shared by all clients and the server
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Id of the scope owner
        /// </summary>
        public Guid Id { get; set; }

        /// <summary>
        /// Gets or Sets a boolean indicating if the scope info is local to the provider (or remote)
        /// </summary>
        public Boolean IsLocal { get; set; }

        /// <summary>
        /// Last time the remote has done a good sync
        /// IF it's a new scope force to Zero to be sure, the first sync will get all datas
        /// </summary>
        public long LastTimestamp { get; set; }


        /// <summary>
        /// Gets or Sets if the current provider is newly created one in database.
        /// If new, we will override timestamp for first synchronisation to be sure to get all datas from server
        /// </summary>
        public Boolean IsNewScope { get; set; }

        /// <summary>
        /// Check if the database is already created.
        /// If so, we won't do any check on the structure.
        /// Edit this value after EnsureScopes to force checking.
        /// </summary>
        //public Boolean IsDatabaseCreated { get; set; }

        /// <summary>
        /// Generate a DmTable based on a SyncContext object
        /// </summary>
        public static void SerializeInDmSet(DmSet set, IEnumerable<ScopeInfo> scopesInfo)
        {
            if (set == null)
                return;

            DmTable dmTable = null;

            if (!set.Tables.Contains("DotmimSync__ScopeInfo"))
            {
                dmTable = new DmTable("DotmimSync__ScopeInfo");
                set.Tables.Add(dmTable);
            }

            dmTable = set.Tables["DotmimSync__ScopeInfo"];

            dmTable.Columns.Add<Guid>("Id");
            dmTable.Columns.Add<Boolean>("IsDatabaseCreated");
            dmTable.Columns.Add<Boolean>("IsLocal");
            dmTable.Columns.Add<Boolean>("IsNewScope");
            dmTable.Columns.Add<Int64>("LastTimestamp");
            dmTable.Columns.Add<String>("Name");

            foreach (var scopeInfo in scopesInfo)
            {
                DmRow dmRow = dmTable.NewRow();

                dmRow["Id"] = scopeInfo.Id;
                //dmRow["IsDatabaseCreated"] = scopeInfo.IsDatabaseCreated;
                dmRow["IsLocal"] = scopeInfo.IsLocal;
                dmRow["IsNewScope"] = scopeInfo.IsNewScope;
                dmRow["LastTimestamp"] = scopeInfo.LastTimestamp;
                dmRow["Name"] = scopeInfo.Name;
                dmTable.Rows.Add(dmRow);
            }

        }
        public static List<ScopeInfo> DeserializeFromDmSet(DmSet set)
        {
            if (set == null)
                return null;

            if (!set.Tables.Contains("DotmimSync__ScopeInfo"))
                return null;

            List<ScopeInfo> scopesInfo = new List<ScopeInfo>();

            foreach(var dmRow in set.Tables["DotmimSync__ScopeInfo"].Rows)
            {

                ScopeInfo scopeInfo = new ScopeInfo();

                scopeInfo.Id = (Guid)dmRow["Id"];
                //scopeInfo.IsDatabaseCreated = (bool)dmRow["IsDatabaseCreated"];
                scopeInfo.IsLocal = (bool)dmRow["IsLocal"];
                scopeInfo.IsNewScope = (bool)dmRow["IsNewScope"];
                scopeInfo.LastTimestamp = (long)dmRow["LastTimestamp"];
                scopeInfo.Name = dmRow["Name"] as string;

                scopesInfo.Add(scopeInfo);
            }

            return scopesInfo;
        }
    }
}
