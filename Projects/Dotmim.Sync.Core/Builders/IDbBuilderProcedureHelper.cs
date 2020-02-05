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
        bool NeedToCreateProcedure(DbCommandType commandName);
        bool NeedToCreateType(DbCommandType typeName);
        void CreateSelectRow();
        void CreateSelectIncrementalChanges(SyncFilter filter);
        void CreateSelectInitializedChanges(SyncFilter filter);
        void CreateUpdate(bool hasMutableColumns);
        void CreateDelete();
        void CreateDeleteMetadata();
        void CreateTVPType();
        void CreateBulkUpdate(bool hasMutableColumns);
        void CreateBulkDelete();
        void CreateReset();
        void DropSelectRow();
        void DropSelectIncrementalChanges(SyncFilter filter);
        void DropSelectInitializedChanges(SyncFilter filter);
        void DropUpdate();
        void DropDelete();
        void DropDeleteMetadata();
        void DropTVPType();
        void DropBulkUpdate();
        void DropBulkDelete();
        void DropReset();
    }
}
