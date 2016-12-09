using Dotmim.Sync.Data;
using System;
using System.Collections.Generic;
using System.Text;
using Xunit;
using System.Linq;

namespace Dotmim.Sync.Core.Test
{
    public class DmRowTests
    {

        DmTable tbl = null;
        public DmRowTests()
        {
            this.tbl = new DmTable("ServiceTickets");
            var id = new DmColumn<Guid>("ServiceTicketID");
            tbl.Columns.Add(id);
            var key = new DmKey(new DmColumn[] { id });
            tbl.PrimaryKey = key;
            tbl.Columns.Add(new DmColumn<string>("Title"));
            tbl.Columns.Add(new DmColumn<string>("Description"));
            tbl.Columns.Add(new DmColumn<int>("StatusValue"));
            tbl.Columns.Add(new DmColumn<int>("EscalationLevel"));
            tbl.Columns.Add(new DmColumn<DateTime>("Opened"));
            tbl.Columns.Add(new DmColumn<DateTime>("Closed"));
            tbl.Columns.Add(new DmColumn<int>("CustomerID"));

            #region adding rows
            var st = tbl.NewRow();
            st["ServiceTicketID"] = Guid.NewGuid();
            st["Title"] = "Titre AER";
            st["Description"] = "Description 2";
            st["EscalationLevel"] = 1;
            st["StatusValue"] = 2;
            st["Opened"] = DateTime.Now;
            st["Closed"] = null;
            st["CustomerID"] = 1;
            tbl.Rows.Add(st);

            st = tbl.NewRow();
            st["ServiceTicketID"] = Guid.NewGuid();
            st["Title"] = "Titre DE";
            st["Description"] = "Description 2";
            st["EscalationLevel"] = 3;
            st["StatusValue"] = 2;
            st["Opened"] = DateTime.Now;
            st["Closed"] = null;
            st["CustomerID"] = 1;
            tbl.Rows.Add(st);

            st = tbl.NewRow();
            st["ServiceTicketID"] = Guid.NewGuid();
            st["Title"] = "Titre FF";
            st["Description"] = "Description 2";
            st["EscalationLevel"] = 3;
            st["StatusValue"] = 4;
            st["Opened"] = DateTime.Now;
            st["Closed"] = null;
            st["CustomerID"] = 2;
            tbl.Rows.Add(st);

            st = tbl.NewRow();
            st["ServiceTicketID"] = Guid.NewGuid();
            st["Title"] = "Titre AC";
            st["Description"] = "Description 2";
            st["EscalationLevel"] = 1;
            st["StatusValue"] = 2;
            st["Opened"] = DateTime.Now;
            st["Closed"] = null;
            st["CustomerID"] = 2;
            tbl.Rows.Add(st);

            st = tbl.NewRow();
            st["ServiceTicketID"] = Guid.NewGuid();
            st["Title"] = "Titre ZDZDZ";
            st["Description"] = "Description 2";
            st["EscalationLevel"] = 0;
            st["StatusValue"] = 1;
            st["Opened"] = DateTime.Now;
            st["Closed"] = null;
            st["CustomerID"] = 2;
            tbl.Rows.Add(st);

            st = tbl.NewRow();
            st["ServiceTicketID"] = Guid.NewGuid();
            st["Title"] = "Titre VGH";
            st["Description"] = "Description 2";
            st["EscalationLevel"] = 0;
            st["StatusValue"] = 1;
            st["Opened"] = DateTime.Now;
            st["Closed"] = null;
            st["CustomerID"] = 3;
            tbl.Rows.Add(st);

            st = tbl.NewRow();
            st["ServiceTicketID"] = Guid.NewGuid();
            st["Title"] = "Titre ETTG";
            st["Description"] = "Description 2";
            st["EscalationLevel"] = 2;
            st["StatusValue"] = 1;
            st["Opened"] = DateTime.Now;
            st["Closed"] = null;
            st["CustomerID"] = 3;
            tbl.Rows.Add(st);

            st = tbl.NewRow();
            st["ServiceTicketID"] = Guid.NewGuid();
            st["Title"] = "Titre SADZD";
            st["Description"] = "Description 2";
            st["EscalationLevel"] = 1;
            st["StatusValue"] = 1;
            st["Opened"] = DateTime.Now;
            st["Closed"] = null;
            st["CustomerID"] = 3;
            tbl.Rows.Add(st);

            st = tbl.NewRow();
            st["ServiceTicketID"] = Guid.NewGuid();
            st["Title"] = "Titre AEEE";
            st["Description"] = "Description 2";
            st["EscalationLevel"] = 0;
            st["StatusValue"] = 0;
            st["Opened"] = DateTime.Now;
            st["Closed"] = null;
            st["CustomerID"] = 1;
            tbl.Rows.Add(st);

            st = tbl.NewRow();
            st["ServiceTicketID"] = Guid.NewGuid();
            st["Title"] = "Titre CZDADA";
            st["Description"] = "Description 2";
            st["EscalationLevel"] = 0;
            st["StatusValue"] = 0;
            st["Opened"] = DateTime.Now;
            st["Closed"] = null;
            st["CustomerID"] = 3;
            tbl.Rows.Add(st);

            st = tbl.NewRow();
            st["ServiceTicketID"] = Guid.NewGuid();
            st["Title"] = "Titre AFBBB";
            st["Description"] = "Description 2";
            st["EscalationLevel"] = 0;
            st["StatusValue"] = 3;
            st["Opened"] = DateTime.Now;
            st["Closed"] = null;
            st["CustomerID"] = 3;
            tbl.Rows.Add(st);

            st = tbl.NewRow();
            st["ServiceTicketID"] = Guid.NewGuid();
            st["Title"] = "Titre AZDCV";
            st["Description"] = "Description 2";
            st["EscalationLevel"] = 2;
            st["StatusValue"] = 2;
            st["Opened"] = DateTime.Now;
            st["Closed"] = null;
            st["CustomerID"] = 2;
            tbl.Rows.Add(st);

            st = tbl.NewRow();
            st["ServiceTicketID"] = Guid.NewGuid();
            st["Title"] = "Titre UYTR";
            st["Description"] = "Description 2";
            st["EscalationLevel"] = 0;
            st["StatusValue"] = 1;
            st["Opened"] = DateTime.Now;
            st["Closed"] = null;
            st["CustomerID"] = 3;
            tbl.Rows.Add(st);

            st = tbl.NewRow();
            st["ServiceTicketID"] = Guid.NewGuid();
            st["Title"] = "Titre NHJK";
            st["Description"] = "Description 2";
            st["EscalationLevel"] = 0;
            st["StatusValue"] = 1;
            st["Opened"] = DateTime.Now;
            st["Closed"] = null;
            st["CustomerID"] = 1;
            tbl.Rows.Add(st);

            st = tbl.NewRow();
            st["ServiceTicketID"] = Guid.NewGuid();
            st["Title"] = "Titre XCVBN";
            st["Description"] = "Description 2";
            st["EscalationLevel"] = 0;
            st["StatusValue"] = 1;
            st["Opened"] = DateTime.Now;
            st["Closed"] = null;
            st["CustomerID"] = 2;
            tbl.Rows.Add(st);

            st = tbl.NewRow();
            st["ServiceTicketID"] = Guid.NewGuid();
            st["Title"] = "Titre LKNB";
            st["Description"] = "Description 2";
            st["EscalationLevel"] = 3;
            st["StatusValue"] = 2;
            st["Opened"] = DateTime.Now;
            st["Closed"] = null;
            st["CustomerID"] = 3;
            tbl.Rows.Add(st);

            st = tbl.NewRow();
            st["ServiceTicketID"] = Guid.NewGuid();
            st["Title"] = "Titre ADFVB";
            st["Description"] = "Description 2";
            st["EscalationLevel"] = 0;
            st["StatusValue"] = 2;
            st["Opened"] = DateTime.Now;
            st["Closed"] = null;
            st["CustomerID"] = 1;
            tbl.Rows.Add(st);
            #endregion

        }

        [Fact]
        public void DmRow_Add_Rows()
        {
            var tbl = new DmTable("ServiceTickets");

            var idColumn = new DmColumn<Guid>("ServiceTicketID");
            tbl.Columns.Add(idColumn);
            var key = new DmKey(new DmColumn[] { idColumn });
            tbl.PrimaryKey = key;
            var titleColumn = new DmColumn<string>("Title");
            tbl.Columns.Add(titleColumn);
            var statusValueColumn = new DmColumn<int>("StatusValue");
            tbl.Columns.Add(statusValueColumn);
            var openedColumn = new DmColumn<DateTime>("Opened");
            tbl.Columns.Add(openedColumn);

            var st = tbl.NewRow();

            Assert.Null(st[0]);
            Assert.Null(st[1]);

            var id = Guid.NewGuid();
            var dateNow = DateTime.Now;
            st["ServiceTicketID"] = id;
            st["Title"] = "Titre AER";
            st["StatusValue"] = 2;
            st["Opened"] = dateNow;
            tbl.Rows.Add(st);

            Assert.Equal(id, st["ServiceTicketID"]);
            Assert.Equal("Titre AER", st["Title"]);
            Assert.Equal(2, st["StatusValue"]);
            Assert.Equal(dateNow, st["Opened"]);

        }

    }
}
