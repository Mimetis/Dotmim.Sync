using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;



namespace Dotmim.Sync.Builders
{
    /// <summary>
    /// This class is responsible to create a stored proc for one particular sync table
    /// </summary>
    public interface IDbBuilderProcedureHelper
    {
        SyncFilter Filter { get; set; }

        bool NeedToCreateProcedure(DbCommandType commandName);
        bool NeedToCreateType(DbCommandType typeName);
        void CreateSelectRow();
        void CreateSelectIncrementalChanges();
        void CreateSelectInitializedChanges();
        void CreateUpdate(bool hasMutableColumns);
        void CreateDelete();
        void CreateDeleteMetadata();
        void CreateTVPType();
        void CreateBulkUpdate(bool hasMutableColumns);
        void CreateBulkDelete();
        void CreateReset();
        void DropSelectRow();
        void DropSelectIncrementalChanges();
        void DropSelectInitializedChanges();
        void DropUpdate();
        void DropDelete();
        void DropDeleteMetadata();
        void DropTVPType();
        void DropBulkUpdate();
        void DropBulkDelete();
        void DropReset();
    }
}
