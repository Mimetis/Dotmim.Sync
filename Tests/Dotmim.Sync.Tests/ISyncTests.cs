using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.Tests
{


    public interface ISyncConnection
    {
        Task GetOnTcp();
        Task GetOnHttp();
    }

    public interface ISyncDatabases
    {

        Task Get_Simple_Database();
        Task Get_Database_With_Schemas();
        Task Get_Database_With_Prefix_And_Suffix();
        Task Get_Database_With_Filter();


    }
    public interface ISyncTests
    {
        Task Initialize();
        Task Bad_Server_Connection();
        Task No_Rows(SyncConfiguration conf);

        Task Check_Foreign_Keys(SyncConfiguration conf);
        Task Insert_One_Row_FromServer(SyncConfiguration conf);
        Task Insert_One_Row_FromClient(SyncConfiguration conf);
        Task Update_One_Row_FromServer(SyncConfiguration conf);
        Task Update_One_Row_FromClient(SyncConfiguration conf);
        Task Delete_One_Row_FromServer(SyncConfiguration conf);
        Task Delete_One_Row_FromClient(SyncConfiguration conf);
        Task Conflict_Insert_Insert_Server_Wins(SyncConfiguration conf);
        Task Conflict_Insert_Insert_Client_Wins(SyncConfiguration conf);
        Task Conflict_Update_Update_Client_Wins(SyncConfiguration conf);
        Task Conflict_Update_Update_Server_Wins(SyncConfiguration conf);
        Task Conflict_Update_Update_Merge(SyncConfiguration conf);
        Task Insert_Update_Delete_From_Server(SyncConfiguration conf);
        Task Cascade_Delete_From_Server(SyncConfiguration conf);
        Task Reinitialize(SyncConfiguration conf);
        Task Reinitialize_With_Upload(SyncConfiguration conf);
        Task Deprovision_All(SyncConfiguration conf);
        Task Deprovision_Stored_Procedures(SyncConfiguration conf);
        Task Deprovision_Tracking_Tables(SyncConfiguration conf);
        Task Deprovision_Tables(SyncConfiguration conf);
        Task Deprovision_Scope(SyncConfiguration conf);
        Task Deprovision_All_Except_Tables(SyncConfiguration conf);

        /// <summary>
        /// Insert in a table where columns are only primary keys like (PostTag : [PostId], [TageId] PRIMARY KEY CLUSTERED ([PostId] ASC, [TagId] ASC))
        /// </summary>
        Task Insert_In_N_N_Table(SyncConfiguration conf);
        Task Delete_In_N_N_Table(SyncConfiguration conf);

        /// <summary>
        /// Handle a column auto increment that is not a primary key
        /// </summary>
        Task Insert_Column_Auto_Increment_And_Not_Primary_Key(SyncConfiguration conf);
        Task Update_Column_Auto_Increment_And_Not_Primary_Key(SyncConfiguration conf);
        Task Delete_Column_Auto_Increment_And_Not_Primary_Key(SyncConfiguration conf);



    }
}
