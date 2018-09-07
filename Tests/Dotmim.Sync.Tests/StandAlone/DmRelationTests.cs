using Dotmim.Sync.Data;
using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace Dotmim.Sync.Tests.StandAlone
{
    public class DmRelationTests
    {
        private DmSet _set = null;
        private DmTable clientTable = null;
        private DmTable clientTypeTable = null;

        public DmRelationTests()
        {
            _set = new DmSet();
            clientTable = new DmTable("Client");
            clientTypeTable = new DmTable("TypeClient");
            _set.Tables.Add(clientTable);
            _set.Tables.Add(clientTypeTable);

            DmColumn Col0 = new DmColumn<int>("ClientId");
            DmColumn Col1 = new DmColumn<int>("ClientType");
            DmColumn Col2 = new DmColumn<String>("ClientName");
            clientTable.Columns.Add(Col0);
            clientTable.Columns.Add(Col1);
            clientTable.Columns.Add(Col2);
            clientTable.PrimaryKey = new DmKey(Col0);

            DmColumn Col3 = new DmColumn<int>("TypeId");
            DmColumn Col4 = new DmColumn<string>("TypeName");
            clientTypeTable.Columns.Add(Col3);
            clientTypeTable.Columns.Add(Col4);
            clientTypeTable.PrimaryKey = new DmKey(Col3);
        }

        [Fact]
        public void Foreign()
        {

            DmRelation Relation = new DmRelation("FK_ClientType",
                clientTypeTable.Columns["TypeId"],
                clientTable.Columns["ClientType"]);

            _set.Relations.Add(Relation);

            DmRow Row = clientTable.NewRow();
            Row[0] = 1;
            Row[1] = 1;
            Row[2] = "Sébastien";
            clientTable.Rows.Add(Row);

            Row = clientTable.NewRow();
            Row[0] = 2;
            Row[1] = 1;
            Row[2] = "Pierre";
            clientTable.Rows.Add(Row);

            Row = clientTable.NewRow();
            Row[0] = 3;
            Row[1] = 2;
            Row[2] = "Paul";

            Row = clientTypeTable.NewRow();
            Row[0] = 1;
            Row[1] = "Grand Compte";
            clientTypeTable.Rows.Add(Row);

            Row = clientTypeTable.NewRow();
            Row[0] = 2;
            Row[1] = "PME";
            clientTypeTable.Rows.Add(Row);

            // Get all rows wher ClientType = "Grand Compte"
            var rowTypeGrandCompte = clientTypeTable.FindByKey(1);
            // Get all childs
            var rowsClients = rowTypeGrandCompte.GetChildRows("FK_ClientType");
            Assert.Equal(2, rowsClients.Length);

            // Get all parents
            var rowClient = clientTable.Rows[0];
            var rowClientType = rowClient.GetParentRow("FK_ClientType");


            Assert.Equal("Grand Compte", rowClientType[1]);
        }



    }
}
