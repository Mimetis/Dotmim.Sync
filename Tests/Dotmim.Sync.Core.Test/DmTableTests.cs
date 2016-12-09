using Dotmim.Sync.Data;
using System;
using Xunit;

namespace Dotmim.Sync.Core.Test
{
    public class DmTableTests
    {
        DmTable tbl = null;
        public DmTableTests()
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
        public void DmTable_Constructor()
        {
            DmTable table = new DmTable();
            Assert.NotNull(table.Columns);
            Assert.NotNull(table.Rows);
            Assert.Equal(0, table.Columns.Count);

            DmTable table2 = new DmTable("Clients");
            Assert.Equal("Clients", table2.TableName);

        }

        [Fact]
        private void DmTable_CheckDmSet_Properties()
        {
            var set0 = new DmSet();
            var tbl0 = new DmTable();
            set0.Tables.Add(tbl0);

            Assert.Equal(set0, tbl0.DmSet);
            Assert.Equal(set0.Culture, tbl0.Culture);
            Assert.Equal(set0.CaseSensitive, tbl0.CaseSensitive);
        }

        [Fact]
        public void DmTable_CaseSensitive()
        {
            var set0 = new DmSet("CaseSensitive");
            var tbl0 = new DmTable("CASESENSITIVE");
            set0.Tables.Add(tbl0);
            // No error throws, it's ok

            Assert.NotEqual(set0.DmSetName, tbl0.TableName);

            Assert.Throws(typeof(ArgumentException), () =>
            {
                var set1 = new DmSet("CaseSensitive");
                set1.CaseSensitive = false;
                var tbl1 = new DmTable("CASESENSITIVE");
                set1.Tables.Add(tbl1);

            });

        }

        [Fact]
        public void DmTable_Clone_Copy()
        {
            var tbl = new DmTable("ServiceTickets");
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

            var tbl2 = tbl.Clone();
            Assert.NotSame(tbl2, tbl);
            Assert.Empty(tbl2.Rows);
            Assert.Equal(8, tbl2.Columns.Count);
            Assert.NotNull(tbl2.PrimaryKey);

            var tbl3 = tbl.Copy();
            Assert.NotSame(tbl3, tbl2);
            Assert.NotSame(tbl3, tbl);
            Assert.NotNull(tbl3.PrimaryKey);
            Assert.Equal(8, tbl3.Columns.Count);
            Assert.NotEmpty(tbl3.Rows);

        }

        [Fact]
        public void DmTable_AcceptChanges()
        {
            tbl.AcceptChanges();

            foreach (var dmRow in tbl.Rows)
                Assert.Equal(DmRowState.Unchanged, dmRow.RowState);
        }

        [Fact]
        public void DmTable_GetChanges()
        {
            tbl.AcceptChanges();

            tbl.Rows[0]["CustomerID"] = 100;
            tbl.Rows[1]["Title"] = "trololoo";
            tbl.Rows[4].SetAdded();
            tbl.Rows[5].SetModified();
            tbl.Rows[6].Delete();

            var changes = tbl.GetChanges();

            Assert.NotSame(tbl, changes);
            Assert.Equal(5, changes.Rows.Count);

            var changes2 = tbl.GetChanges(DmRowState.Added);

            Assert.NotSame(tbl, changes2);
            Assert.Equal(1, changes2.Rows.Count);

            var changes3 = tbl.GetChanges(DmRowState.Added | DmRowState.Modified);

            Assert.NotSame(tbl, changes3);
            Assert.Equal(4, changes3.Rows.Count);

            var changes4 = tbl.GetChanges(DmRowState.Deleted);

            Assert.NotSame(tbl, changes4);
            Assert.Equal(1, changes4.Rows.Count);

        }

        [Fact]
        public void DmTable_FindByKey()
        {
            var key = Guid.NewGuid();
            var st = tbl.NewRow();
            st["ServiceTicketID"] = key;
            st["Title"] = "Titre AER";
            st["Description"] = "Description 2";
            st["EscalationLevel"] = 1;
            st["StatusValue"] = 2;
            st["Opened"] = DateTime.Now;
            st["Closed"] = null;
            st["CustomerID"] = 1;
            tbl.Rows.Add(st);

            var dmRow = tbl.FindByKey(key);

            Assert.NotNull(dmRow);
            Assert.Equal(key, dmRow["ServiceTicketID"]);

            // Multiple Key
            // Check if the culture / case sensitive works as well with 
            // string in the pkey

            var tbl2 = new DmTable("ServiceTickets");
            tbl2.CaseSensitive = true;
            
            var id1 = new DmColumn<Guid>("ServiceTicketID");
            var id2 = new DmColumn<int>("CustomerID");
            var id3 = new DmColumn<string>("Title");
            tbl2.Columns.Add(id1);
            tbl2.Columns.Add(id2);
            tbl2.Columns.Add(id3);
            var pkey = new DmKey(new DmColumn[] { id1, id2, id3 });
            tbl2.PrimaryKey = pkey;

            tbl2.Columns.Add(new DmColumn<string>("Description"));
            tbl2.Columns.Add(new DmColumn<int>("StatusValue"));
            tbl2.Columns.Add(new DmColumn<int>("EscalationLevel"));
            tbl2.Columns.Add(new DmColumn<DateTime>("Opened"));
            tbl2.Columns.Add(new DmColumn<DateTime>("Closed"));

            var rowPkeyGuid = Guid.NewGuid();
            var row = tbl2.NewRow();
            row["ServiceTicketID"] = rowPkeyGuid;
            row["CustomerID"] = 1;
            row["Title"] = "Titre AER";

            row["Description"] = "Description 2";
            row["EscalationLevel"] = 1;
            row["StatusValue"] = 2;
            row["Opened"] = DateTime.Now;
            row["Closed"] = null;
            tbl2.Rows.Add(row);

            row = tbl2.NewRow();
            row["ServiceTicketID"] = rowPkeyGuid;
            row["CustomerID"] = 1;
            row["Title"] = "Titre DE";

            row["Description"] = "Description 2";
            row["EscalationLevel"] = 3;
            row["StatusValue"] = 2;
            row["Opened"] = DateTime.Now;
            row["Closed"] = null;
            tbl2.Rows.Add(row);

            var dmRow2 =tbl2.FindByKey(new object[] { rowPkeyGuid, 1, "Titre aer" });
            Assert.Null(dmRow2);
       
            tbl2.CaseSensitive = false;
            var dmRow3 = tbl2.FindByKey(new object[] { rowPkeyGuid, 1, "Titre aer" });
            Assert.NotNull(dmRow3);

        }

        [Fact]
        public void DmTable_ImportRow()
        {
            var tbl2 = new DmTable();
            var id = new DmColumn<Guid>("ServiceTicketID");
            tbl2.Columns.Add(id);
            var key = new DmKey(new DmColumn[] { id });
            tbl2.PrimaryKey = key;
            tbl2.Columns.Add(new DmColumn<string>("Title"));
            tbl2.Columns.Add(new DmColumn<string>("Description"));
            tbl2.Columns.Add(new DmColumn<int>("StatusValue"));
            tbl2.Columns.Add(new DmColumn<int>("EscalationLevel"));
            tbl2.Columns.Add(new DmColumn<DateTime>("Opened"));
            tbl2.Columns.Add(new DmColumn<DateTime>("Closed"));
            tbl2.Columns.Add(new DmColumn<int>("CustomerID"));

            var st = tbl2.NewRow();
            st["ServiceTicketID"] = Guid.NewGuid();
            st["Title"] = "Titre AER";
            st["Description"] = "Description 2";
            st["EscalationLevel"] = 1;
            st["StatusValue"] = 2;
            st["Opened"] = DateTime.Now;
            st["Closed"] = null;
            st["CustomerID"] = 1;
            tbl2.Rows.Add(st);

            // Importing into tbl
            var st2 = this.tbl.ImportRow(st);

            // acceptchanges to change the row state
            this.tbl.AcceptChanges();
            Assert.NotEqual(st.RowState, st2.RowState);

            // making change to be sure
            st2["CustomerID"] = 2;
            Assert.Equal(2, st2["CustomerID"]);
            Assert.Equal(1, st["CustomerID"]);

        }

        [Fact]
        public void DmTable_LoadDataRow()
        {
            // Trying with a key GUID (not auto incremented)
            var tbl2 = new DmTable();
            var id = new DmColumn<Guid>("ServiceTicketID");
            tbl2.Columns.Add(id);
            var key = new DmKey(new DmColumn[] { id });
            tbl2.PrimaryKey = key;
            tbl2.Columns.Add(new DmColumn<string>("Title"));
            tbl2.Columns.Add(new DmColumn<string>("Description"));
            tbl2.Columns.Add(new DmColumn<int>("StatusValue"));
            tbl2.Columns.Add(new DmColumn<int>("EscalationLevel"));
            tbl2.Columns.Add(new DmColumn<DateTime>("Opened"));
            tbl2.Columns.Add(new DmColumn<DateTime>("Closed"));
            tbl2.Columns.Add(new DmColumn<int>("CustomerID"));

            var st = tbl2.NewRow();
            st["ServiceTicketID"] = Guid.NewGuid();
            st["Title"] = "Titre AER";
            st["Description"] = "Description 2";
            st["EscalationLevel"] = 1;
            st["StatusValue"] = 2;
            st["Opened"] = DateTime.Now;
            st["Closed"] = null;
            st["CustomerID"] = 1;
            tbl2.Rows.Add(st);

            var array = new object[] { Guid.NewGuid(), "Titre 2", "Desc", 1, 2, DateTime.Now, null, 3 };
            tbl2.LoadDataRow(array, true);

            Assert.Equal(2, tbl2.Rows.Count);

            // Trying with a key Int auto incremented
            var tbl3 = new DmTable();
            var id3 = new DmColumn<Int32>("ServiceTicketID");
            id3.AutoIncrement = true;
            tbl3.Columns.Add(id3);
            var key3 = new DmKey(new DmColumn[] { id3 });
            tbl3.PrimaryKey = key3;
            tbl3.Columns.Add(new DmColumn<string>("Title"));
            tbl3.Columns.Add(new DmColumn<string>("Description"));
            tbl3.Columns.Add(new DmColumn<int>("StatusValue"));
            tbl3.Columns.Add(new DmColumn<int>("EscalationLevel"));
            tbl3.Columns.Add(new DmColumn<DateTime>("Opened"));
            tbl3.Columns.Add(new DmColumn<DateTime>("Closed"));
            tbl3.Columns.Add(new DmColumn<int>("CustomerID"));

            var st3 = tbl3.NewRow();
            st3["Title"] = "Titre AER";
            st3["Description"] = "Description 2";
            st3["EscalationLevel"] = 1;
            st3["StatusValue"] = 2;
            st3["Opened"] = DateTime.Now;
            st3["Closed"] = null;
            st3["CustomerID"] = 1;
            tbl3.Rows.Add(st3);

            var array3 = new object[] { "Titre 2", "Desc", 1, 2, DateTime.Now, null, 3 };
            tbl3.LoadDataRow(array3, true);

            Assert.Equal(1, tbl3.Rows[0][0]);
            Assert.Equal(2, tbl3.Rows[1][0]);
        }

        [Fact]
        public void DmTable_RejectChanges()
        {
            tbl.AcceptChanges();

            var dmRow = tbl.Rows[0];
            dmRow["CustomerID"] = 2;

            tbl.RejectChanges();

            Assert.Equal(1, dmRow["CustomerID"]);
            Assert.Equal(DmRowState.Unchanged, dmRow.RowState);

            // Try to reject changes after creating a table, with no acceptchanges
            var tbl3 = new DmTable();
            var id3 = new DmColumn<Int32>("ServiceTicketID");
            id3.AutoIncrement = true;
            tbl3.Columns.Add(id3);
            var key3 = new DmKey(new DmColumn[] { id3 });
            tbl3.PrimaryKey = key3;
            tbl3.Columns.Add(new DmColumn<string>("Title"));

            var st3 = tbl3.NewRow();
            st3["Title"] = "Titre AER";
            tbl3.Rows.Add(st3);

            // SInce we didn't AcceptChanges, raise an error
            Assert.Throws(typeof(Exception), () => tbl3.RejectChanges());

        }

        [Fact]
        public void DmTable_Merge()
        {
            var tbl2 = new DmTable();
            var id = new DmColumn<Guid>("ServiceTicketID");
            tbl2.Columns.Add(id);
            var key = new DmKey(new DmColumn[] { id });
            tbl2.PrimaryKey = key;
            tbl2.Columns.Add(new DmColumn<string>("Title"));
            tbl2.Columns.Add(new DmColumn<string>("Description"));
            tbl2.Columns.Add(new DmColumn<int>("StatusValue"));
            tbl2.Columns.Add(new DmColumn<int>("EscalationLevel"));
            tbl2.Columns.Add(new DmColumn<DateTime>("Opened"));
            tbl2.Columns.Add(new DmColumn<DateTime>("Closed"));
            tbl2.Columns.Add(new DmColumn<int>("CustomerID"));

            var st = tbl2.NewRow();
            st["ServiceTicketID"] = Guid.NewGuid();
            st["Title"] = "Titre AER";
            st["Description"] = "Description 2";
            st["EscalationLevel"] = 1;
            st["StatusValue"] = 2;
            st["Opened"] = DateTime.Now;
            st["Closed"] = null;
            st["CustomerID"] = 1;
            tbl2.Rows.Add(st);

            // Importing into tbl
            var actualRowsCount = this.tbl.Rows.Count;

            this.tbl.Merge(tbl2);

            Assert.Equal(actualRowsCount + 1, this.tbl.Rows.Count);


            var tbl3 = new DmTable();
            var id3 = new DmColumn<Guid>("ServiceTicketID");
            tbl3.Columns.Add(id3);
            var key3 = new DmKey(new DmColumn[] { id3 });
            tbl3.PrimaryKey = key3;
            tbl3.Columns.Add(new DmColumn<string>("Title"));
            tbl3.Columns.Add(new DmColumn<string>("Description"));
            tbl3.Columns.Add(new DmColumn<int>("StatusValue"));
            tbl3.Columns.Add(new DmColumn<int>("EscalationLevel"));
            tbl3.Columns.Add(new DmColumn<DateTime>("Opened"));
            tbl3.Columns.Add(new DmColumn<DateTime>("Closed"));
            tbl3.Columns.Add(new DmColumn<int>("CustomerID"));
            tbl3.Columns.Add(new DmColumn<int>("AAAAAAAAAAAAAA"));
            tbl3.Columns.Add(new DmColumn<string>("VVVVVVVVVVVVVVVVVVVV"));

            var st3 = tbl3.NewRow();
            st3["ServiceTicketID"] = Guid.NewGuid();
            st3["Title"] = "Titre AER";
            st3["Description"] = "Description 2";
            st3["EscalationLevel"] = 1;
            st3["StatusValue"] = 2;
            st3["Opened"] = DateTime.Now;
            st3["Closed"] = null;
            st3["CustomerID"] = 1;
            st3["AAAAAAAAAAAAAA"] = "A";
            st3["VVVVVVVVVVVVVVVVVVVV"] = "V";
            tbl3.Rows.Add(st3);


            // Importing into tbl
            actualRowsCount = this.tbl.Rows.Count;

            this.tbl.Merge(tbl3);

            Assert.Equal(actualRowsCount + 1, this.tbl.Rows.Count);
            Assert.Equal(10, this.tbl.Columns.Count);



        }

    }
}
