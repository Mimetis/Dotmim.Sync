using Dotmim.Sync.Data;
using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Core.Builders
{
    public abstract class DbObjectNames
    {
        Dictionary<DbObjectType, String> names = new Dictionary<DbObjectType, string>();
        public DmTable TableDescription { get; }
     
        public DbObjectNames(DmTable tableDescription)
        {
            this.TableDescription = tableDescription;
        }

        public void AddName(DbObjectType objectType, string name)
        {
            if (names.ContainsKey(objectType))
                throw new Exception("Yous can't add an objectType multiple times");

            names.Add(objectType, name);
        }
        public string GetObjectName(DbObjectType objectType)
        {
            if (!names.ContainsKey(objectType))
                throw new Exception("Yous should provide a value for all DbObjectType");

            return names[objectType];
        }
    }

    public enum DbObjectType
    {
        SelectChangesProcName,
        SelectRowProcName,
        InsertProcName,
        UpdateProcName,
        DeleteProcName,
        InsertMetadataProcName,
        UpdateMetadataProcName,
        DeleteMetadataProcName,
        InsertTriggerName,
        UpdateTriggerName,
        DeleteTriggerName,
        BulkTableTypeName,
        BulkInsertProcName,
        BulkUpdateProcName,
        BulkDeleteProcName
    }

}
