using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Core.Test.SqlUtils
{
    public class InlineConfigurations : IEnumerable<object[]>
    {
        private readonly List<object[]> confs = new List<object[]>();

        public InlineConfigurations()
        {
            confs.Add(new object[] { new ServiceConfiguration() });

            confs.Add(new object[] { new ServiceConfiguration{
                DownloadBatchSizeInKB = 100,
                UseBulkOperations = true,
                SerializationConverter= Enumerations.SerializationFormat.Json
            } });

            confs.Add(new object[] { new ServiceConfiguration{
                DownloadBatchSizeInKB = 100,
                UseBulkOperations = false,
                SerializationConverter= Enumerations.SerializationFormat.Json
            } });

            confs.Add(new object[] { new ServiceConfiguration{
                DownloadBatchSizeInKB = 0,
                UseBulkOperations = true,
                SerializationConverter= Enumerations.SerializationFormat.Json
            } });

            confs.Add(new object[] { new ServiceConfiguration{
                DownloadBatchSizeInKB = 0,
                UseBulkOperations = false,
                SerializationConverter= Enumerations.SerializationFormat.Json
            } });
            confs.Add(new object[] { new ServiceConfiguration{
                DownloadBatchSizeInKB = 100,
                UseBulkOperations = true,
                SerializationConverter= Enumerations.SerializationFormat.DmSerializer
            } });

            confs.Add(new object[] { new ServiceConfiguration{
                DownloadBatchSizeInKB = 100,
                UseBulkOperations = false,
                SerializationConverter= Enumerations.SerializationFormat.DmSerializer
            } });

            confs.Add(new object[] { new ServiceConfiguration{
                DownloadBatchSizeInKB = 0,
                UseBulkOperations = true,
                SerializationConverter= Enumerations.SerializationFormat.DmSerializer
            } });

            confs.Add(new object[] { new ServiceConfiguration{
                DownloadBatchSizeInKB = 0,
                UseBulkOperations = false,
                SerializationConverter= Enumerations.SerializationFormat.DmSerializer
            } });

        }
        public IEnumerator<object[]> GetEnumerator() => confs.GetEnumerator();
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    }
}
