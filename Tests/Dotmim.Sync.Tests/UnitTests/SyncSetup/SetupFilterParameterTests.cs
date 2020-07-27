using Dotmim.Sync.Setup;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using Xunit;

namespace Dotmim.Sync.Tests.UnitTests
{
    public class SetupFilterParameterTests
    {

        [Fact]
        public void SetupFilterParameter_Compare_TwoSetupFilterParameter_ShouldBe_Equals()
        {
            SetupFilterParameter filterParam1 = new SetupFilterParameter();
            SetupFilterParameter filterParam2 = new SetupFilterParameter();

            Assert.Equal(filterParam1, filterParam2);
            Assert.True(filterParam1.Equals(filterParam2));
            Assert.False(filterParam1 == filterParam2);

            filterParam1 = new SetupFilterParameter();
            filterParam2 = new SetupFilterParameter();

            filterParam2.SchemaName = "";

            Assert.Equal(filterParam1, filterParam2);
            Assert.True(filterParam1.Equals(filterParam2));

            filterParam1.TableName = "Product";
            filterParam2.TableName = "Product";

            filterParam1.SchemaName = string.Empty;

            Assert.Equal(filterParam1, filterParam2);
            Assert.True(filterParam1.Equals(filterParam2));

            filterParam1.SchemaName = "dbo";
            filterParam2.SchemaName = "dbo";

            Assert.Equal(filterParam1, filterParam2);
            Assert.True(filterParam1.Equals(filterParam2));

            filterParam1 = new SetupFilterParameter();
            filterParam2 = new SetupFilterParameter();

            filterParam1.AllowNull = true;
            filterParam2.AllowNull = true;

            Assert.Equal(filterParam1, filterParam2);
            Assert.True(filterParam1.Equals(filterParam2));

            filterParam1.DbType = null;
            filterParam2.DbType = null;

            Assert.Equal(filterParam1, filterParam2);
            Assert.True(filterParam1.Equals(filterParam2));

            filterParam1.DbType = DbType.String;
            filterParam2.DbType = DbType.String;

            Assert.Equal(filterParam1, filterParam2);
            Assert.True(filterParam1.Equals(filterParam2));

            filterParam1.DefaultValue = "12";
            filterParam2.DefaultValue = "12";

            Assert.Equal(filterParam1, filterParam2);
            Assert.True(filterParam1.Equals(filterParam2));

            filterParam1.MaxLength = 100;
            filterParam2.MaxLength = 100;

            Assert.Equal(filterParam1, filterParam2);
            Assert.True(filterParam1.Equals(filterParam2));
        }

        [Fact]
        public void SetupFilterParameter_Compare_TwoSetupFilterParameter_ShouldBe_Different()
        {
            SetupFilterParameter filterParam1 = new SetupFilterParameter();
            SetupFilterParameter filterParam2 = new SetupFilterParameter();

            filterParam2.SchemaName = "dbo";

            Assert.NotEqual(filterParam1, filterParam2);
            Assert.False(filterParam1.Equals(filterParam2));

            filterParam1.TableName = "Product1";
            filterParam2.TableName = "Product2";

            filterParam1.SchemaName = string.Empty;

            Assert.NotEqual(filterParam1, filterParam2);
            Assert.False(filterParam1.Equals(filterParam2));

            filterParam1.SchemaName = "dbo";
            filterParam2.SchemaName = "SalesLT";

            Assert.NotEqual(filterParam1, filterParam2);
            Assert.False(filterParam1.Equals(filterParam2));

            filterParam1 = new SetupFilterParameter();
            filterParam2 = new SetupFilterParameter();

            filterParam1.AllowNull = false;
            filterParam2.AllowNull = true;
 
            Assert.NotEqual(filterParam1, filterParam2);
            Assert.False(filterParam1.Equals(filterParam2));

            filterParam1 = new SetupFilterParameter();
            filterParam2 = new SetupFilterParameter();
            filterParam1.DbType = null;
            filterParam2.DbType = DbType.String;

            Assert.NotEqual(filterParam1, filterParam2);
            Assert.False(filterParam1.Equals(filterParam2));

            filterParam1 = new SetupFilterParameter();
            filterParam2 = new SetupFilterParameter();
            filterParam1.DbType = DbType.String;
            filterParam2.DbType = DbType.Int32;

            Assert.NotEqual(filterParam1, filterParam2);
            Assert.False(filterParam1.Equals(filterParam2));

            filterParam1 = new SetupFilterParameter();
            filterParam2 = new SetupFilterParameter();
            filterParam1.DefaultValue = "12";
            filterParam2.DefaultValue = null;

            Assert.NotEqual(filterParam1, filterParam2);
            Assert.False(filterParam1.Equals(filterParam2));

            filterParam1 = new SetupFilterParameter();
            filterParam2 = new SetupFilterParameter();
            filterParam1.MaxLength = 100;
            filterParam2.MaxLength = 0;

            Assert.NotEqual(filterParam1, filterParam2);
            Assert.False(filterParam1.Equals(filterParam2));
        }

    }
}
