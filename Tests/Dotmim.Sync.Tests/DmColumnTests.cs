using Dotmim.Sync.Data;
using System;
using System.Collections.Generic;
using System.Text;
using Xunit;
using Xunit.Abstractions;

namespace Dotmim.Sync.Test
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
            Assert.False(id.AllowDBNull);
            Assert.Equal("ServiceTicketID", id.ColumnName);
            Assert.Equal(typeof(Guid), id.DataType);
            Assert.Equal(0, id.Ordinal);
            Assert.True(id.Unique);

            var titleColumn = new DmColumn<string>("Title");
            tbl.Columns.Add(titleColumn);
            Assert.Equal("Title", titleColumn.ColumnName);
            Assert.Equal(typeof(string), titleColumn.DataType);
            Assert.Equal(1, titleColumn.Ordinal);
            Assert.False(titleColumn.Unique);

            var sv = new DmColumn<int>("StatusValue");
            tbl.Columns.Add(sv);
            Assert.Equal("StatusValue", sv.ColumnName);
            Assert.Equal(typeof(Int32), sv.DataType);
            Assert.Equal(2, sv.Ordinal);
            Assert.False(sv.Unique);

            var opened = new DmColumn<DateTime>("Opened");
            tbl.Columns.Add(opened);
            Assert.Equal("Opened", opened.ColumnName);
            Assert.Equal(typeof(DateTime), opened.DataType);
            Assert.Equal(3, opened.Ordinal);
            Assert.False(opened.Unique);

            var closedColumn = DmColumn.CreateColumn("Closed", typeof(DateTime));
            tbl.Columns.Add(closedColumn);
            Assert.Equal("Closed", closedColumn.ColumnName);
            Assert.Equal(typeof(DateTime), closedColumn.DataType);
            Assert.Equal(4, closedColumn.Ordinal);
            Assert.False(closedColumn.Unique);
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

            Assert.Equal(0, id.Ordinal);
            Assert.Equal(1, titleColumn.Ordinal);
            Assert.Equal(2, sv.Ordinal);
            Assert.Equal(3, opened.Ordinal);
            Assert.Equal(4, closedColumn.Ordinal);

            sv.SetOrdinal(1);

            Assert.Equal(0, id.Ordinal);
            Assert.Equal(1, sv.Ordinal);
            Assert.Equal(2, titleColumn.Ordinal);
            Assert.Equal(3, opened.Ordinal);
            Assert.Equal(4, closedColumn.Ordinal);

            closedColumn.SetOrdinal(0);

            Assert.Equal(0, closedColumn.Ordinal);
            Assert.Equal(1, id.Ordinal);
            Assert.Equal(2, sv.Ordinal);
            Assert.Equal(3, titleColumn.Ordinal);
            Assert.Equal(4, opened.Ordinal);
          

        }


    }
}
