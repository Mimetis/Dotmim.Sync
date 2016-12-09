using Dotmim.Sync.Data;
using System;
using System.Collections.Generic;
using System.Text;
using Xunit;
using Xunit.Abstractions;

namespace Dotmim.Sync.Core.Test
{
    public class DmColumnTests
    {
        private readonly ITestOutputHelper output;

        public DmColumnTests(ITestOutputHelper output)
        {
            this.output = output;

        }

        [Fact]
        public void DmColumn_Create_AutoIncorement_Column()
        {
            var tbl = new DmTable("ServiceTickets");

            var id = new DmColumn<int>("ServiceTicketID");
            id.AutoIncrement = true;
            id.AutoIncrementSeed = 1;
            id.AutoIncrementStep = 1;

            tbl.Columns.Add(id);


            var key = new DmKey(new DmColumn[] { id });
            tbl.PrimaryKey = key;

            output.WriteLine("AutoIncrement is correctly initialized");
        }

        [Fact]
        public void DmColumn_Create_Bad_AutoIncorement_Column()
        {
            Assert.Throws(typeof(ArgumentException), () =>
            {
                var tbl = new DmTable("ServiceTickets");

                var id = new DmColumn<Guid>("ServiceTicketID");
                id.AutoIncrement = true;

                tbl.Columns.Add(id);
            });

        }


        [Fact]
        public void DmColumn_Create_Columns()
        {
            var tbl = new DmTable("ServiceTickets");

            var id = new DmColumn<Guid>("ServiceTicketID");
            tbl.Columns.Add(id);
            var key = new DmKey(new DmColumn[] { id });
            tbl.PrimaryKey = key;

            // if is PK, non null allowable
            Assert.Equal(false, id.AllowDBNull);
            Assert.Equal("ServiceTicketID", id.ColumnName);
            Assert.Equal(id.DataType, typeof(Guid));
            Assert.Equal(id.Ordinal, 0);
            Assert.Equal(id.Unique, true);

            var titleColumn = new DmColumn<string>("Title");
            tbl.Columns.Add(titleColumn);
            Assert.Equal("Title", titleColumn.ColumnName);
            Assert.Equal(titleColumn.DataType, typeof(string));
            Assert.Equal(titleColumn.Ordinal, 1);
            Assert.Equal(titleColumn.Unique, false);

            var sv = new DmColumn<int>("StatusValue");
            tbl.Columns.Add(sv);
            Assert.Equal("StatusValue", sv.ColumnName);
            Assert.Equal(sv.DataType, typeof(Int32));
            Assert.Equal(sv.Ordinal, 2);
            Assert.Equal(sv.Unique, false);

            var opened = new DmColumn<DateTime>("Opened");
            tbl.Columns.Add(opened);
            Assert.Equal("Opened", opened.ColumnName);
            Assert.Equal(opened.DataType, typeof(DateTime));
            Assert.Equal(opened.Ordinal, 3);
            Assert.Equal(opened.Unique, false);

            var closedColumn = DmColumn.CreateColumn("Closed", typeof(DateTime));
            tbl.Columns.Add(closedColumn);
            Assert.Equal("Closed", closedColumn.ColumnName);
            Assert.Equal(closedColumn.DataType, typeof(DateTime));
            Assert.Equal(closedColumn.Ordinal, 4);
            Assert.Equal(closedColumn.Unique, false);
        }

        [Fact]
        public void DmColumn_SetOrdinal()
        {
            var tbl = new DmTable("ServiceTickets");

            var id = new DmColumn<Guid>("ServiceTicketID");
            tbl.Columns.Add(id);
            var key = new DmKey(new DmColumn[] { id });
            tbl.PrimaryKey = key;

            var titleColumn = new DmColumn<string>("Title");
            tbl.Columns.Add(titleColumn);
            var sv = new DmColumn<int>("StatusValue");
            tbl.Columns.Add(sv);
            var opened = new DmColumn<DateTime>("Opened");
            tbl.Columns.Add(opened);
            var closedColumn = DmColumn.CreateColumn("Closed", typeof(DateTime));
            tbl.Columns.Add(closedColumn);

            Assert.Equal(id.Ordinal, 0);
            Assert.Equal(titleColumn.Ordinal, 1);
            Assert.Equal(sv.Ordinal, 2);
            Assert.Equal(opened.Ordinal, 3);
            Assert.Equal(closedColumn.Ordinal, 4);

            sv.SetOrdinal(1);

            Assert.Equal(id.Ordinal, 0);
            Assert.Equal(sv.Ordinal, 1);
            Assert.Equal(titleColumn.Ordinal, 2);
            Assert.Equal(opened.Ordinal, 3);
            Assert.Equal(closedColumn.Ordinal, 4);

            closedColumn.SetOrdinal(0);

            Assert.Equal(closedColumn.Ordinal, 0);
            Assert.Equal(id.Ordinal, 1);
            Assert.Equal(sv.Ordinal, 2);
            Assert.Equal(titleColumn.Ordinal, 3);
            Assert.Equal(opened.Ordinal, 4);
          

        }


    }
}
