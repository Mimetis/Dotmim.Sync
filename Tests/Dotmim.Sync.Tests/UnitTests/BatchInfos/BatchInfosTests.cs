using Dotmim.Sync.Batch;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Serialization;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
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

            var str = $"{test.TestCase.DisplayName} : {this.stopwatch.Elapsed.Minutes}:{this.stopwatch.Elapsed.Seconds}.{this.stopwatch.Elapsed.Milliseconds}";
            Console.WriteLine(str);
            Debug.WriteLine(str);
        }

        private SyncTable GetSimpleSyncTable(int rowsCount = 1)
        {
            SyncTable tCustomer = new SyncTable("Customer");
            tCustomer.Columns.Add(new SyncColumn("ID", typeof(Guid)));
            tCustomer.Columns.Add(new SyncColumn("Name", typeof(string)));
            tCustomer.PrimaryKeys.Add("ID");

            for (int i = 0; i < rowsCount; i++)
            {
                SyncRow tCustomerRow = new SyncRow(tCustomer);
                tCustomerRow["ID"] = "A";
                tCustomerRow["Name"] = "B";
                tCustomer.Rows.Add(tCustomerRow);
            }

            tCustomer.EnsureTable(new SyncSet());

            return tCustomer;
        }

        private async Task<(BatchInfo, SyncTable)> GenerateBatchInfoAsync(int rowsCount = 1)
        {
            var tCustomer = GetSimpleSyncTable(rowsCount);
            var batchInfo = new BatchInfo();

            // get a new filename and filepath
            var (filePath, fileName) = batchInfo.GetNewBatchPartInfoPath(tCustomer, 1, "json", "");

            // create a new part for this batch info
            var batchPartInfo = new BatchPartInfo(fileName, tCustomer.TableName, tCustomer.SchemaName,
                SyncRowState.None, tCustomer.Rows.Count, 0);

            batchInfo.BatchPartsInfo.Add(batchPartInfo);

            // using a serializer to serialize the table data on disk
            var localSerializer = new LocalJsonSerializer();

            // open it
            await localSerializer.OpenFileAsync(filePath, tCustomer, SyncRowState.Modified);

            foreach (var row in tCustomer.Rows)
                await localSerializer.WriteRowToFileAsync(row, tCustomer);

            await localSerializer.CloseFileAsync();

            return (batchInfo, tCustomer);
        }

        [Fact]
        public async Task CreateBatchInfo()
        {
            var tCustomer = GetSimpleSyncTable();
            var batchInfo = new BatchInfo();

            // get a new filename and filepath
            var (filePath, fileName) = batchInfo.GetNewBatchPartInfoPath(tCustomer, 1, "json", "");

            // create a new part for this batch info
            var batchPartInfo = new BatchPartInfo(fileName, tCustomer.TableName, tCustomer.SchemaName,
                Enumerations.SyncRowState.None, tCustomer.Rows.Count, 0);

            batchInfo.BatchPartsInfo.Add(batchPartInfo);

            // using a serializer to serialize the table data on disk
            var localSerializer = new LocalJsonSerializer();

            // open it
            await localSerializer.OpenFileAsync(filePath, tCustomer, SyncRowState.None);

            foreach (var row in tCustomer.Rows)
                await localSerializer.WriteRowToFileAsync(row, tCustomer);

            await localSerializer.CloseFileAsync();

            var fileInfo = new FileInfo(filePath);

            Assert.NotNull(fileInfo);
            Assert.True(fileInfo.Exists);

            // using Json.Net to validate the data
            using var streamReader = new StreamReader(filePath);
            var str = streamReader.ReadToEnd();
            var o = JObject.Parse(str);

            Assert.NotNull(o);
            Assert.Equal("Customer", (string)o["t"]);
            Assert.Empty((string)o["s"]);
            Assert.Equal(2, (Int32)o["st"]);
            JArray columns = o["c"] as JArray;
            Assert.Equal(2, columns.Count);
            Assert.Equal("ID", columns[0]["n"]);
            Assert.Equal("16", columns[0]["t"]);
            Assert.Equal("Name", columns[1]["n"]);
            Assert.Equal("17", columns[1]["t"]);
        }


        [Fact]
        public async Task ReadSyncTableFromBatchInfo()
        {
            var (bi, st) = await GenerateBatchInfoAsync(10);

            var filePath = bi.GetBatchPartInfoPath(bi.BatchPartsInfo[0]);

            var (schemaTable, rowsCount, state) =
                LocalJsonSerializer.GetSchemaTableFromFile(filePath);

            Assert.Equal(st.TableName, schemaTable.TableName);
            Assert.Equal(st.SchemaName, schemaTable.SchemaName);
            Assert.Equal(st.Rows.Count, rowsCount);
            Assert.Equal(SyncRowState.Modified, state);
        }

        [Fact]
        public async Task ReadEmptySyncTableFromBatchInfo()
        {
            var (bi, st) = await GenerateBatchInfoAsync(0);

            var filePath = bi.GetBatchPartInfoPath(bi.BatchPartsInfo[0]);

            var (schemaTable, rowsCount, state) =
                LocalJsonSerializer.GetSchemaTableFromFile(filePath);

            Assert.Equal(st.TableName, schemaTable.TableName);
            Assert.Equal(st.SchemaName, schemaTable.SchemaName);
            Assert.Equal(0, rowsCount);
            Assert.Equal(SyncRowState.Modified, state);
        }

    }
}
