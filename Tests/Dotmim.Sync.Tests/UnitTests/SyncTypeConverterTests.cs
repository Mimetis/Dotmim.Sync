using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Reflection;
using System.Text;
using Xunit;
using Xunit.Abstractions;

namespace Dotmim.Sync.Tests.UnitTests
{
    public class SyncTypeConverterTests : IDisposable
    {
        // Current test running
        private ITest test;
        private Stopwatch stopwatch;
        public ITestOutputHelper Output { get; }

        public SyncTypeConverterTests(ITestOutputHelper output)
        {

            // Getting the test running
            this.Output = output;
            var type = output.GetType();
            var testMember = type.GetField("test", BindingFlags.Instance | BindingFlags.NonPublic);
            this.test = (ITest)testMember.GetValue(output);
            this.stopwatch = Stopwatch.StartNew();
        }

        public void Dispose()
        {
            this.stopwatch.Stop();

            var str = $"{test.TestCase.DisplayName} : {this.stopwatch.Elapsed.Minutes}:{this.stopwatch.Elapsed.Seconds}.{this.stopwatch.Elapsed.Milliseconds}";
            Console.WriteLine(str);
            Debug.WriteLine(str);
        }


        [Theory]
        [InlineData("12")]
        [InlineData(12)]
        [InlineData(12.177)]
        [InlineData((float)12.177)]
        public void Convert_ToInt16(object input)
        {
            var o1 = SyncTypeConverter.TryConvertTo<short>(input);
            Assert.IsType<short>(o1);
            Assert.Equal((short)12, o1);
        }

        [Theory]
        [InlineData("12")]
        [InlineData(12)]
        [InlineData(12.177)]
        [InlineData((float)12.177)]
        public void Convert_ToInt32(object input)
        {
            var o1 = SyncTypeConverter.TryConvertTo<int>(input);
            Assert.IsType<int>(o1);
            Assert.Equal((int)12, o1);
        }

        [Theory]
        [InlineData("12")]
        [InlineData(12)]
        [InlineData(12.177)]
        [InlineData((float)12.177)]
        public void Convert_ToInt64(object input)
        {
            var o1 = SyncTypeConverter.TryConvertTo<Int64>(input);
            Assert.IsType<Int64>(o1);
            Assert.Equal((Int64)12, o1);
        }

        [Theory]
        [InlineData("12")]
        [InlineData(12)]
        [InlineData(12.177)]
        [InlineData((float)12.177)]
        public void Convert_ToUInt16(object input)
        {
            var o1 = SyncTypeConverter.TryConvertTo<UInt16>(input);
            Assert.IsType<UInt16>(o1);
            Assert.Equal((UInt16)12, o1);
        }

        [Theory]
        [InlineData("12")]
        [InlineData(12)]
        [InlineData(12.177)]
        [InlineData((float)12.177)]
        public void Convert_ToUInt32(object input)
        {
            var o1 = SyncTypeConverter.TryConvertTo<UInt32>(input);
            Assert.IsType<UInt32>(o1);
            Assert.Equal((UInt32)12, o1);
        }

        [Theory]
        [InlineData("12")]
        [InlineData(12)]
        [InlineData(12.177)]
        [InlineData((float)12.177)]
        public void Convert_ToUInt64(object input)
        {
            var o1 = SyncTypeConverter.TryConvertTo<UInt64>(input);
            Assert.IsType<UInt64>(o1);
            Assert.Equal((UInt64)12, o1);
        }


        [Theory]
        [InlineData("10/02/2020")]
        [InlineData("2020/02/10")]
        [InlineData("2020-02-10")]
        [InlineData("10-02-2020")]
        [InlineData(637168896000000000)]
        public void Convert_ToDateTime(object input)
        {
            var o1 = SyncTypeConverter.TryConvertTo<DateTime>(input);
            Assert.IsType<DateTime>(o1);
            Assert.Equal(new DateTime(2020, 02, 10), o1);
        }

        [Theory]
        [InlineData("10/02/2020")]
        [InlineData("2020/02/10")]
        [InlineData("2020-02-10")]
        [InlineData("10-02-2020")]
        [InlineData(637168896000000000)]
        public void Convert_ToDateTimeOffset(object input)
        {
            var ti = new DateTimeOffset(new DateTime(2020, 02, 10)).Ticks;

            var o1 = SyncTypeConverter.TryConvertTo<DateTimeOffset>(input);
            Assert.IsType<DateTimeOffset>(o1);
            Assert.Equal(new DateTimeOffset(new DateTime(2020, 02, 10)), o1);
        }

        [Theory]
        [InlineData("12")]
        [InlineData(12)]
        [InlineData(12.177)]
        [InlineData((float)12.177)]
        public void Convert_ToByte(object input)
        {
            var o1 = SyncTypeConverter.TryConvertTo<byte>(input);
            Assert.IsType<byte>(o1);
            Assert.Equal((byte)12, o1);
        }

        [Theory]
        [InlineData("12")]
        [InlineData(12)]
        [InlineData(12.177)]
        [InlineData((float)12.177)]
        public void Convert_ToSByte(object input)
        {
            var o1 = SyncTypeConverter.TryConvertTo<sbyte>(input);
            Assert.IsType<sbyte>(o1);
            Assert.Equal((sbyte)12, o1);
        }

        [Theory]
        [InlineData("true")]
        [InlineData("TRUE")]
        [InlineData("True")]
        [InlineData("tRue")]
        [InlineData("1")]
        [InlineData(1)]
        [InlineData((float)1.0)]
        [InlineData(1.0)]
        [InlineData((byte)1)]
        [InlineData(true)]
        public void Convert_ToBoolean_True(object input)
        {
            var o1 = SyncTypeConverter.TryConvertTo<bool>(input);
            Assert.IsType<bool>(o1);
            Assert.True(o1);
        }


        [Theory]
        [InlineData("false")]
        [InlineData("FALSE")]
        [InlineData("False")]
        [InlineData("faLse")]
        [InlineData("0")]
        [InlineData(0)]
        [InlineData(0.0)]
        [InlineData((float)0.0)]
        [InlineData((byte)0)]
        [InlineData(false)]
        public void Convert_ToBoolean_False(object input)
        {
            var o1 = SyncTypeConverter.TryConvertTo<bool>(input);
            Assert.IsType<bool>(o1);
            Assert.False(o1);
        }


        [Theory]
        [InlineData("DDB67AC3-89DF-430E-AD65-CBE691D237D8")]
        [InlineData("ddb67ac3-89df-430e-ad65-cbe691d237d8")]
        public void Convert_ToGuid(object input)
        {
            var o1 = SyncTypeConverter.TryConvertTo<Guid>(input);
            Assert.IsType<Guid>(o1);
            Assert.Equal(new Guid("ddb67ac3-89df-430e-ad65-cbe691d237d8"), o1);
        }

        [Fact]
        public void Convert_Byte_ArrayToGuid()
        {
            var bytearray = new Guid("ddb67ac3-89df-430e-ad65-cbe691d237d8").ToByteArray();

            var o1 = SyncTypeConverter.TryConvertTo<Guid>(bytearray);
            Assert.IsType<Guid>(o1);
            Assert.Equal(new Guid("ddb67ac3-89df-430e-ad65-cbe691d237d8"), o1);
        }

        [Theory]
        [InlineData("a")]
        [InlineData('a')]
        public void Convert_ToChar(object input)
        {
            var o1 = SyncTypeConverter.TryConvertTo<char>(input);
            Assert.IsType<char>(o1);
            Assert.Equal('a', o1);
        }

        [Theory]
        [InlineData("12.177")]
        [InlineData(12.177)]
        [InlineData((float)12.177)]
        public void Convert_ToDecimal(object input)
        {
            var o1 = SyncTypeConverter.TryConvertTo<decimal>(input);
            Assert.IsType<decimal>(o1);
            Assert.Equal((decimal)12.177, o1);
        }

        [Theory]
        [InlineData("12,177")]
        public void Convert_ToDecimal_WithNfi(object input)
        {

            NumberFormatInfo nfi = new NumberFormatInfo();
            nfi.NumberDecimalSeparator = ",";

            var o1 = SyncTypeConverter.TryConvertTo<decimal>(input, nfi);
            Assert.IsType<decimal>(o1);
            Assert.Equal((decimal)12.177, o1);

            SyncGlobalization.DataSourceNumberDecimalSeparator = ",";
            var o2 = SyncTypeConverter.TryConvertTo<decimal>(input);
            Assert.IsType<decimal>(o2);
            Assert.Equal((decimal)12.177, o2);
            SyncGlobalization.DataSourceNumberDecimalSeparator = CultureInfo.InvariantCulture.NumberFormat.NumberDecimalSeparator;
        }

        [Theory]
        [InlineData("12.177")]
        [InlineData(12.177)]
        public void Convert_ToDouble(object input)
        {
            var o1 = SyncTypeConverter.TryConvertTo<double>(input);
            Assert.IsType<double>(o1);
            Assert.Equal((double)12.177, o1);
        }

        [Theory]
        [InlineData("12,177")]
        public void Convert_ToDouble_WithNfi(object input)
        {

            NumberFormatInfo nfi = new NumberFormatInfo();
            nfi.NumberDecimalSeparator = ",";

            var o1 = SyncTypeConverter.TryConvertTo<double>(input, nfi);
            Assert.IsType<double>(o1);
            Assert.Equal((double)12.177, o1);

            SyncGlobalization.DataSourceNumberDecimalSeparator = ",";
            var o2 = SyncTypeConverter.TryConvertTo<double>(input);
            Assert.IsType<double>(o2);
            Assert.Equal((double)12.177, o2);
            SyncGlobalization.DataSourceNumberDecimalSeparator = CultureInfo.InvariantCulture.NumberFormat.NumberDecimalSeparator;
        }
        [Theory]
        [InlineData("12.177")]
        [InlineData(12.177d)]
        [InlineData((float)12.177)]
        public void Convert_ToFloat(object input)
        {
            var o1 = SyncTypeConverter.TryConvertTo<float>(input);
            Assert.IsType<float>(o1);
            Assert.Equal((float)12.177, o1);
        }

        [Theory]
        [InlineData("12,177")]
        public void Convert_ToFloat_WithNfi(object input)
        {

            NumberFormatInfo nfi = new NumberFormatInfo();
            nfi.NumberDecimalSeparator = ",";

            var o1 = SyncTypeConverter.TryConvertTo<float>(input, nfi);
            Assert.IsType<float>(o1);
            Assert.Equal((float)12.177, o1);

            SyncGlobalization.DataSourceNumberDecimalSeparator = ",";
            var o2 = SyncTypeConverter.TryConvertTo<float>(input);
            Assert.IsType<float>(o2);
            Assert.Equal((float)12.177, o2);
            SyncGlobalization.DataSourceNumberDecimalSeparator = CultureInfo.InvariantCulture.NumberFormat.NumberDecimalSeparator;
        }

        [Theory]
        [InlineData("00:00:00.0100000")]
        [InlineData(100000)]
        public void Convert_ToTimeSpan(object input)
        {
            TimeSpan ts = new TimeSpan(100000);

            var o1 = SyncTypeConverter.TryConvertTo<TimeSpan>(input);
            Assert.IsType<TimeSpan>(o1);
            Assert.Equal(ts, o1);
        }

        [Fact]
        public void Convert_Base64String_ToByteArray()
        {
            string s = "I'm a drummer";
            byte[] sByt = Encoding.UTF8.GetBytes(s);
            string sBase64= Convert.ToBase64String(sByt);

            var o1 = SyncTypeConverter.TryConvertTo<byte[]>(sBase64);
            Assert.IsType<byte[]>(o1);
            Assert.Equal(sByt, o1);

            var b = Encoding.UTF8.GetString(o1);
            Assert.Equal(s, b);

        }

        [Theory]
        [InlineData(100000)]
        [InlineData(true)]
        [InlineData(false)]
        [InlineData(12.43)]
        [InlineData((float)12.43)]
        public void Convert_ToByteArray(object input)
        {

            var o1 = SyncTypeConverter.TryConvertTo<byte[]>(input);
            var expected = BitConverter.GetBytes((dynamic)input);


            Assert.Equal(expected, o1);
        }


    }
}
