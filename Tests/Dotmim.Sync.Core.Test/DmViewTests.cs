using Dotmim.Sync.Data;
using System;
using System.Collections.Generic;
using System.Text;
using Xunit;
namespace Dotmim.Sync.Core.Test
{
    public class DmViewTests
    {
        DmSet set;

        public DmViewTests()
        {
            set = new DmSet("ClientDmSet");

            var tbl = new DmTable("ServiceTickets");
            set.Tables.Add(tbl);
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

            tbl.AcceptChanges();
        }


        [Fact]
        private void DmView_Constructor()
        {
            var t = set.Tables["ServiceTickets"];

            var view = new DmView(t);
            Assert.Equal(17, view.Count);

            var view2 = new DmView(t, DmRowState.Modified);
            Assert.Equal(0, view2.Count);

            // Set one row as Modified
            view[0].SetModified();
            var view22 = new DmView(t, DmRowState.Modified);
            Assert.Equal(1, view22.Count);

            var view3 = new DmView(t, (r) => (int)r["CustomerID"] == 1);
            Assert.Equal(5, view3.Count);

            var view4 = new DmView(view3, (r) => (int)r["StatusValue"] == 2);
            Assert.Equal(3, view4.Count);
        }

        [Fact]
        private void DmView_Order()
        {
            var view = new DmView(set.Tables["ServiceTickets"]);
            view = view.Order((r1, r2) => string.Compare(((string)r1["Title"]), (string)r2["Title"], StringComparison.Ordinal));
            Assert.Equal(17, view.Count);

            Assert.Equal("Titre AC", (string)view[0]["Title"]);
            Assert.Equal("Titre ADFVB", (string)view[1]["Title"]);
            Assert.Equal("Titre AEEE", (string)view[2]["Title"]);
            Assert.Equal("Titre AER", (string)view[3]["Title"]);
            Assert.Equal("Titre AFBBB", (string)view[4]["Title"]);

        }

        [Fact]
        private void DmView_Filter()
        {
            // first filter
            var filter = new Predicate<DmRow>((row) =>
            {
                if (row.RowState == DmRowState.Deleted)
                    return false;

                if ((int)row["CustomerID"] == 1)
                    return true;

                return false;
            });

            var view = new DmView(set.Tables["ServiceTickets"], filter);
            Assert.Equal(5, view.Count);

            // Second Filter
            view = view.Filter((r) => (int)r["StatusValue"] == 2);
            Assert.Equal(3, view.Count);


        }
    }
}
