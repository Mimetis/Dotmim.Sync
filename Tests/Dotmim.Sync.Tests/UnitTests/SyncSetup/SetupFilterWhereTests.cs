using Dotmim.Sync.Setup;
using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace Dotmim.Sync.Tests.UnitTests
{
    public class SetupFilterWhereTests
    {


        [Fact]
        public void SetupFilterWhere_Compare_TwoSetupFilterWhere_ShouldBe_Equals()
        {
            SetupFilterWhere where1 = new SetupFilterWhere();
            SetupFilterWhere where2 = new SetupFilterWhere();

            Assert.Equal(where1, where2);
            Assert.True(where1.Equals(where2));
            Assert.False(where1 == where2);

            where1 = new SetupFilterWhere();
            where2 = new SetupFilterWhere();

            where2.SchemaName = "";

            Assert.Equal(where1, where2);
            Assert.True(where1.Equals(where2));

            where1.TableName = "Product";
            where2.TableName = "Product";

            where1.SchemaName = string.Empty;

            Assert.Equal(where1, where2);
            Assert.True(where1.Equals(where2));

            where1.SchemaName = "dbo";
            where2.SchemaName = "dbo";

            Assert.Equal(where1, where2);
            Assert.True(where1.Equals(where2));

            where1 = new SetupFilterWhere();
            where2 = new SetupFilterWhere();

            where1.ParameterName = "@param1";
            where2.ParameterName = "@param1";

            Assert.Equal(where1, where2);
            Assert.True(where1.Equals(where2));

            where1.ColumnName = "ProductID";
            where2.ColumnName = "ProductID";

            Assert.Equal(where1, where2);
            Assert.True(where1.Equals(where2));
        }

        [Fact]
        public void SetupFilterWhere_Compare_TwoDifferentSetupFilterWhere_ShouldBe_Different()
        {
            SetupFilterWhere where1 = new SetupFilterWhere();
            SetupFilterWhere where2 = new SetupFilterWhere();

            Assert.Equal(where1, where2);
            Assert.True(where1.Equals(where2));

            where1.TableName = "Product1";
            where2.TableName = "Product2";

            Assert.NotEqual(where1, where2);
            Assert.False(where1.Equals(where2));

            where1.TableName = "Product1";
            where2.TableName = "Product1";
            where1.SchemaName = "SalesLT";
            where2.SchemaName = "dbo";

            Assert.NotEqual(where1, where2);
            Assert.False(where1.Equals(where2));

            where1.TableName = "Product1";
            where2.TableName = "Product1";
            where1.SchemaName = "SalesLT";
            where2.SchemaName = null;

            Assert.NotEqual(where1, where2);
            Assert.False(where1.Equals(where2));

            where1 = new SetupFilterWhere();
            where2 = new SetupFilterWhere();

            where1.ParameterName = "Param1";
            where2.ParameterName = "Param2";

            Assert.NotEqual(where1, where2);
            Assert.False(where1.Equals(where2));

            where1 = new SetupFilterWhere();
            where2 = new SetupFilterWhere();

            where1.ColumnName = "Col1";
            where2.ColumnName = "Col2";

            Assert.NotEqual(where1, where2);
            Assert.False(where1.Equals(where2));


        }
    }
}
