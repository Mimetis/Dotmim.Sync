using Dotmim.Sync.Data;
using Dotmim.Sync.Data.Surrogate;
using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace Dotmim.Sync.Core.Test
{
    public class DmSurrogateTests
    {
        DmSet set;

        public DmSurrogateTests()
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
        public void Create_DmSetSurrogate()
        {
            DmSetSurrogate dmSetSurrogate = new DmSetSurrogate(set);

            Assert.Equal(set.Tables.Count, dmSetSurrogate.DmTableSurrogates.Length);

            var records = dmSetSurrogate.DmTableSurrogates[0].Records;

            Assert.Equal(set.Tables[0].Rows.Count, records[0].Count);
        }


        [Fact]
        public void Create_DmTableSurrogate()
        {
            DmTable tbl = set.Tables[0];

            DmTableSurrogate dmTableSurrogate = new DmTableSurrogate(tbl);

            var records = dmTableSurrogate.Records;

            Assert.Equal(tbl.Rows.Count, records[0].Count);
        }

        [Fact]
        public void Convert_DmSetSurrogate_To_DmSet()
        {
            DmSetSurrogate dmSetSurrogate = new DmSetSurrogate(set);
            DmSet newSet = dmSetSurrogate.ConvertToDmSet();

            Assert.NotSame(set, newSet);
            Assert.Equal(set.CaseSensitive, newSet.CaseSensitive);
            Assert.Equal(set.Culture, newSet.Culture);
            Assert.Equal(set.DmSetName, newSet.DmSetName);
            Assert.Equal(set.Tables.Count, newSet.Tables.Count);
            Assert.Equal(set.Tables[0].Columns.Count, newSet.Tables[0].Columns.Count);
            Assert.Equal(set.Tables[0].Rows.Count, newSet.Tables[0].Rows.Count);

        }

        [Fact]
        public void Convert_DmSetSurrogate_To_ClonableDmSet()
        {
            DmSetSurrogate dmSetSurrogate = new DmSetSurrogate(set);
            DmSet newSet = dmSetSurrogate.ConvertToDmSet(set);

            Assert.NotSame(set, newSet);
            Assert.Equal(set.CaseSensitive, newSet.CaseSensitive);
            Assert.Equal(set.Culture, newSet.Culture);
            Assert.Equal(set.DmSetName, newSet.DmSetName);
            Assert.Equal(set.Tables.Count, newSet.Tables.Count);
            Assert.Equal(set.Tables[0].Columns.Count, newSet.Tables[0].Columns.Count);
            Assert.Equal(set.Tables[0].Rows.Count, newSet.Tables[0].Rows.Count);

            // Check the PKeys
            Assert.Equal(set.Tables[0].PrimaryKey.Columns.Length, newSet.Tables[0].PrimaryKey.Columns.Length);
        }


        [Fact]
        public void Check_RowsSates()
        {
            var dmTable = set.Tables[0];
            dmTable.Rows[0].Delete();
            dmTable.Rows[1].SetAdded();
            dmTable.Rows[2].SetModified();

            DmSetSurrogate dss = new DmSetSurrogate(set);

            var set2 = dss.ConvertToDmSet(set);
            var rows2 = set2.Tables[0].Rows;

            Assert.Equal(DmRowState.Deleted, rows2[0].RowState);
            Assert.Equal(DmRowState.Added, rows2[1].RowState);
            Assert.Equal(DmRowState.Modified, rows2[2].RowState);
            Assert.Equal(DmRowState.Unchanged, rows2[3].RowState);
            Assert.Equal(DmRowState.Unchanged, rows2[4].RowState);
            Assert.Equal(DmRowState.Unchanged, rows2[5].RowState);


        }


    }
}
