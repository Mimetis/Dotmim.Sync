using Dotmim.Sync.Batch;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Serialization;
using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Dotmim.Sync.Tests.UnitTests
{
    public class BatchInfosTests : IDisposable
    {
        // Current test running
        private ITest test;
        private Stopwatch stopwatch;

        public ITestOutputHelper Output { get; }

        public BatchInfosTests(ITestOutputHelper output)
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

            var str = $"{this.test.TestCase.DisplayName} : {this.stopwatch.Elapsed.Minutes}:{this.stopwatch.Elapsed.Seconds}.{this.stopwatch.Elapsed.Milliseconds}";
            Console.WriteLine(str);
            Debug.WriteLine(str);
        }

        public static SyncTable GetSimpleSyncTable(int rowsCount = 1)
        {
            SyncTable tCustomer = new SyncTable("Customer");
            tCustomer.Columns.Add(new SyncColumn("ID", typeof(Guid)));
            tCustomer.Columns.Add(new SyncColumn("Name", typeof(string)));
            tCustomer.Columns.Add(new SyncColumn("Column_Int16", typeof(short)));
            tCustomer.Columns.Add(new SyncColumn("Column_Int32", typeof(int)));
            tCustomer.Columns.Add(new SyncColumn("Column_Int64", typeof(long)));
            tCustomer.Columns.Add(new SyncColumn("Column_UInt16", typeof(ushort)));
            tCustomer.Columns.Add(new SyncColumn("Column_UInt32", typeof(uint)));
            tCustomer.Columns.Add(new SyncColumn("Column_UInt64", typeof(ulong)));
            tCustomer.Columns.Add(new SyncColumn("Column_DateTime", typeof(DateTime)));
            tCustomer.Columns.Add(new SyncColumn("Column_DateTimeOffset", typeof(DateTimeOffset)));
            tCustomer.Columns.Add(new SyncColumn("Column_Byte", typeof(byte)));
            tCustomer.Columns.Add(new SyncColumn("Column_Boolean", typeof(bool)));
            tCustomer.Columns.Add(new SyncColumn("Column_Char", typeof(char)));
            tCustomer.Columns.Add(new SyncColumn("Column_Decimal", typeof(decimal)));
            tCustomer.Columns.Add(new SyncColumn("Column_Double", typeof(double)));
            tCustomer.Columns.Add(new SyncColumn("Column_Float", typeof(float)));
            tCustomer.Columns.Add(new SyncColumn("Column_SByte", typeof(sbyte)));
            tCustomer.Columns.Add(new SyncColumn("Column_TimeSpan", typeof(TimeSpan)));
            tCustomer.Columns.Add(new SyncColumn("Column_ByteArray", typeof(byte[])));

            tCustomer.PrimaryKeys.Add("ID");

            for (int i = 0; i < rowsCount; i++)
            {
                SyncRow tCustomerRow = new SyncRow(tCustomer);
                tCustomerRow["ID"] = Guid.NewGuid();
                tCustomerRow["Name"] = "John Snow";
                tCustomerRow["Column_Int16"] = (short)11;
                tCustomerRow["Column_Int32"] = 222;
                tCustomerRow["Column_Int64"] = 3333L;
                tCustomerRow["Column_UInt16"] = (ushort)11;
                tCustomerRow["Column_UInt32"] = 222U;
                tCustomerRow["Column_UInt64"] = 3333UL;
                tCustomerRow["Column_DateTime"] = DateTime.Now;
                tCustomerRow["Column_DateTimeOffset"] = DateTimeOffset.Now;
                tCustomerRow["Column_Byte"] = (byte)255;
                tCustomerRow["Column_Boolean"] = true;
                tCustomerRow["Column_Char"] = 'B';
                tCustomerRow["Column_Decimal"] = 10.12M;
                tCustomerRow["Column_Double"] = 10.12;
                tCustomerRow["Column_Float"] = 10.12F;
                tCustomerRow["Column_SByte"] = (sbyte)12;
                tCustomerRow["Column_TimeSpan"] = TimeSpan.FromSeconds(10);
                tCustomerRow["Column_ByteArray"] = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };

                tCustomer.Rows.Add(tCustomerRow);
            }

            tCustomer.EnsureTable(new SyncSet());

            return tCustomer;
        }

        private static async Task<(BatchInfo BatchInfo, SyncTable SyncTable)> GenerateBatchInfoAsync(int rowsCount = 1, SyncRowState syncRowState = SyncRowState.None)
        {
            var tCustomer = GetSimpleSyncTable(rowsCount);
            var batchInfo = new BatchInfo();

            // get a new filename and filepath
            var (filePath, fileName) = batchInfo.GetNewBatchPartInfoPath(tCustomer, 1, "json", string.Empty);

            // create a new part for this batch info
            var batchPartInfo = new BatchPartInfo(fileName, tCustomer.TableName, tCustomer.SchemaName,
                syncRowState, tCustomer.Rows.Count, 0);

            batchInfo.BatchPartsInfo.Add(batchPartInfo);

            // using a serializer to serialize the table data on disk
            using var localSerializer = new LocalJsonSerializer();

            // open it
            await localSerializer.OpenFileAsync(filePath, tCustomer, syncRowState);

            foreach (var row in tCustomer.Rows)
                await localSerializer.WriteRowToFileAsync(row, tCustomer);

            return (batchInfo, tCustomer);
        }

        [Fact]
        public async Task CreateBatchInfo()
        {
            var (bi, st) = await GenerateBatchInfoAsync(0, SyncRowState.None);
            var filePath = bi.GetBatchPartInfoFullPath(bi.BatchPartsInfo[0]);

            var fileInfo = new FileInfo(filePath);

            Assert.NotNull(fileInfo);
            Assert.True(fileInfo.Exists);

            var jsonString = await File.ReadAllTextAsync(filePath);
            using var jsonDoc = JsonDocument.Parse(jsonString);
            var root = jsonDoc.RootElement;

            var customerToken = root.GetProperty("t")[0];

            Assert.Equal("Customer", customerToken.GetProperty("n").GetString());
            Assert.Empty(customerToken.GetProperty("s").GetString());
            Assert.Equal(2, customerToken.GetProperty("st").GetInt32());
            var columns = customerToken.GetProperty("c").EnumerateArray().ToList();
            Assert.Equal(19, columns.Count);

            Assert.Equal("ID", columns[0].GetProperty("n").GetString());
            Assert.Equal("16", columns[0].GetProperty("t").GetString());

            Assert.Equal("Name", columns[1].GetProperty("n").GetString());
            Assert.Equal("17", columns[1].GetProperty("t").GetString());

            Assert.Equal("Column_Int16", columns[2].GetProperty("n").GetString());
            Assert.Equal("8", columns[2].GetProperty("t").GetString());

            Assert.Equal("Column_Int32", columns[3].GetProperty("n").GetString());
            Assert.Equal("6", columns[3].GetProperty("t").GetString());

            Assert.Equal("Column_Int64", columns[4].GetProperty("n").GetString());
            Assert.Equal("7", columns[4].GetProperty("t").GetString());

            Assert.Equal("Column_UInt16", columns[5].GetProperty("n").GetString());
            Assert.Equal("11", columns[5].GetProperty("t").GetString());

            Assert.Equal("Column_UInt32", columns[6].GetProperty("n").GetString());
            Assert.Equal("9", columns[6].GetProperty("t").GetString());

            Assert.Equal("Column_UInt64", columns[7].GetProperty("n").GetString());
            Assert.Equal("10", columns[7].GetProperty("t").GetString());

            Assert.Equal("Column_DateTime", columns[8].GetProperty("n").GetString());
            Assert.Equal("13", columns[8].GetProperty("t").GetString());

            Assert.Equal("Column_DateTimeOffset", columns[9].GetProperty("n").GetString());
            Assert.Equal("14", columns[9].GetProperty("t").GetString());

            Assert.Equal("Column_Byte", columns[10].GetProperty("n").GetString());
            Assert.Equal("2", columns[10].GetProperty("t").GetString());

            Assert.Equal("Column_Boolean", columns[11].GetProperty("n").GetString());
            Assert.Equal("1", columns[11].GetProperty("t").GetString());

            Assert.Equal("Column_Char", columns[12].GetProperty("n").GetString());
            Assert.Equal("3", columns[12].GetProperty("t").GetString());

            Assert.Equal("Column_Decimal", columns[13].GetProperty("n").GetString());
            Assert.Equal("15", columns[13].GetProperty("t").GetString());

            Assert.Equal("Column_Double", columns[14].GetProperty("n").GetString());
            Assert.Equal("4", columns[14].GetProperty("t").GetString());

            Assert.Equal("Column_Float", columns[15].GetProperty("n").GetString());
            Assert.Equal("5", columns[15].GetProperty("t").GetString());

            Assert.Equal("Column_SByte", columns[16].GetProperty("n").GetString());
            Assert.Equal("18", columns[16].GetProperty("t").GetString());

            Assert.Equal("Column_TimeSpan", columns[17].GetProperty("n").GetString());
            Assert.Equal("19", columns[17].GetProperty("t").GetString());

            Assert.Equal("Column_ByteArray", columns[18].GetProperty("n").GetString());
            Assert.Equal("12", columns[18].GetProperty("t").GetString());
        }

        [Fact]
        public async Task ReadSyncTableFromBatchInfo()
        {
            var (bi, st) = await GenerateBatchInfoAsync(10, SyncRowState.ApplyModifiedFailed);

            var filePath = bi.GetBatchPartInfoFullPath(bi.BatchPartsInfo[0]);

            var (schemaTable, rowsCount, state) =
                LocalJsonSerializer.GetSchemaTableFromFile(filePath);

            Assert.Equal(st.TableName, schemaTable.TableName);
            Assert.Equal(st.SchemaName, schemaTable.SchemaName);
            Assert.Equal(st.Rows.Count, rowsCount);
            Assert.Equal(SyncRowState.ApplyModifiedFailed, state);
        }

        [Fact]
        public async Task ReadHugeSyncTableFromBatchInfo()
        {
            var (bi, st) = await GenerateBatchInfoAsync(100000, SyncRowState.ApplyModifiedFailed);

            var filePath = bi.GetBatchPartInfoFullPath(bi.BatchPartsInfo[0]);

            var (schemaTable, rowsCount, state) =
              LocalJsonSerializer.GetSchemaTableFromFile(filePath);

            var localSerializer = new LocalJsonSerializer();

            var rows = localSerializer.GetRowsFromFile(filePath, st).ToList();

            Assert.Equal(st.Rows.Count, rows.Count);
        }

        [Fact]
        public async Task ReadHugeContainerTableeFromBatchInfo()
        {
            var rowsNumberToGenerate = 1000000;

            var tCustomer = GetSimpleSyncTable(rowsNumberToGenerate);

            var containerTable = new ContainerTable(tCustomer);

            foreach (var row in tCustomer.Rows)
                containerTable.Rows.Add(row.ToArray());

            var jsonObjectSerializer = new JsonObjectSerializer();

            var bytes = jsonObjectSerializer.Serialize(containerTable);
            var res = await jsonObjectSerializer.DeserializeAsync<ContainerTable>(new MemoryStream(bytes));

            Assert.Equal(rowsNumberToGenerate, res.Rows.Count);
        }

        [Fact]
        public async Task ReadSyncRowsFromBatchInfo()
        {
            var (bi, st) = await GenerateBatchInfoAsync(10);

            var filePath = bi.GetBatchPartInfoFullPath(bi.BatchPartsInfo[0]);

            using var localSerializer = new LocalJsonSerializer();

            var rows = localSerializer.GetRowsFromFile(filePath, st).ToList();

            Assert.Equal(st.Rows.Count, rows.Count);

            var row = rows[0];

            Assert.Equal(st.Columns.Count, row.Length);

            for (var i = 0; i < st.Columns.Count; i++)
            {
                var column = st.Columns[i];
                var value = row[column.ColumnName];
                Assert.NotNull(value);
                Assert.Equal(value.GetType(), column.GetDataType());
            }
        }

        [Fact]
        public async Task ReadEmptySyncTableFromBatchInfo()
        {
            var (bi, st) = await GenerateBatchInfoAsync(0, SyncRowState.Modified);

            var filePath = bi.GetBatchPartInfoFullPath(bi.BatchPartsInfo[0]);

            var (schemaTable, rowsCount, state) =
                LocalJsonSerializer.GetSchemaTableFromFile(filePath);

            Assert.Equal(st.TableName, schemaTable.TableName);
            Assert.Equal(st.SchemaName, schemaTable.SchemaName);
            Assert.Equal(0, rowsCount);
            Assert.Equal(SyncRowState.Modified, state);
        }
    }
}