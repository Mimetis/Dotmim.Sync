using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.FX.Tests.SqlUtils
{
    public class InlineConfigurations : IEnumerable<object[]>
    {
        private readonly List<object[]> confs = new List<object[]>();

        public InlineConfigurations()
        {
            confs.Add(new object[] { new SyncConfiguration{
                DownloadBatchSizeInKB = 100,
                UseBulkOperations = true,
                SerializationFormat= Enumerations.SerializationFormat.Json
            } });

            confs.Add(new object[] { new SyncConfiguration{
                DownloadBatchSizeInKB = 100,
                UseBulkOperations = false,
                SerializationFormat= Enumerations.SerializationFormat.Json
            } });

            confs.Add(new object[] { new SyncConfiguration{
                DownloadBatchSizeInKB = 0,
                UseBulkOperations = true,
                SerializationFormat= Enumerations.SerializationFormat.Json
            } });

            confs.Add(new object[] { new SyncConfiguration{
                DownloadBatchSizeInKB = 0,
                UseBulkOperations = false,
                SerializationFormat= Enumerations.SerializationFormat.Json
            } });

            //confs.Add(new object[] { new SyncConfiguration{
            //    DownloadBatchSizeInKB = 100,
            //    UseBulkOperations = true,
            //    SerializationFormat= Enumerations.SerializationFormat.Binary
            //} });

            //confs.Add(new object[] { new SyncConfiguration{
            //    DownloadBatchSizeInKB = 100,
            //    UseBulkOperations = false,
            //    SerializationFormat= Enumerations.SerializationFormat.Binary
            //} });

            //confs.Add(new object[] { new SyncConfiguration{
            //    DownloadBatchSizeInKB = 0,
            //    UseBulkOperations = true,
            //    SerializationFormat= Enumerations.SerializationFormat.Binary
            //} });

            //confs.Add(new object[] { new SyncConfiguration{
            //    DownloadBatchSizeInKB = 0,
            //    UseBulkOperations = false,
            //    SerializationFormat= Enumerations.SerializationFormat.Binary
            //} });

        }
        public IEnumerator<object[]> GetEnumerator() => confs.GetEnumerator();
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    }
}
