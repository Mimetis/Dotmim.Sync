using Dotmim.Sync.Data;
using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace Dotmim.Sync.Core.Test
{
    public class DmRelationTests
    {
        private DmSet _set = null;
        private DmTable _mom = null;
        private DmTable _child = null;

        public DmRelationTests()
        {
            _set = new DmSet();
            _mom = new DmTable("Mom");
            _child = new DmTable("Child");
            _set.Tables.Add(_mom);
            _set.Tables.Add(_child);

            DmColumn Col = new DmColumn<String>("Name");
            DmColumn Col2 = new DmColumn<String>("ChildName");
            _mom.Columns.Add(Col);
            _mom.Columns.Add(Col2);

            DmColumn Col3 = new DmColumn<String>("Name");
            DmColumn Col4 = new DmColumn<Int16>("Age");
            _child.Columns.Add(Col3);
            _child.Columns.Add(Col4);
        }

        [Fact]
        public void Foreign()
        {
            DmRelation Relation = new DmRelation("Rel", _mom.Columns[1], _child.Columns[0]);
            _set.Relations.Add(Relation);

            DmRow Row = _mom.NewRow();
            Row[0] = "Teresa";
            Row[1] = "Jack";
            _mom.Rows.Add(Row);

            Row = _mom.NewRow();
            Row[0] = "Teresa";
            Row[1] = "Dick";
            _mom.Rows.Add(Row);

            Row = _mom.NewRow();
            Row[0] = "Mary";
            Row[1] = "Harry";

            Row = _child.NewRow();
            Row[0] = "Jack";
            Row[1] = 16;
            _child.Rows.Add(Row);

            Row = _child.NewRow();
            Row[0] = "Dick";
            Row[1] = 56;
            _child.Rows.Add(Row);

            Assert.Equal(2, _child.Rows.Count);

            Row = _mom.Rows[0];
            Row.Delete();

            Assert.Equal(1, _child.Rows.Count);

            Row = _mom.NewRow();
            Row[0] = "Teresa";
            Row[1] = "Dick";


            Row = _mom.NewRow();
            Row[0] = "Teresa";
            Row[1] = "Mich";
            _mom.Rows.Add(Row);
            Assert.Equal(1, _child.Rows.Count);

        }



    }
}
