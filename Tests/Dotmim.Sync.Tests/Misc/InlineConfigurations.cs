using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Test.Misc
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

            confs.Add(new object[] { new SyncConfiguration{
                DownloadBatchSizeInKB = 100,
                UseBulkOperations = true,
                SerializationFormat= Enumerations.SerializationFormat.Binary
            } });

            confs.Add(new object[] { new SyncConfiguration{
                DownloadBatchSizeInKB = 100,
                UseBulkOperations = false,
                SerializationFormat= Enumerations.SerializationFormat.Binary
            } });

            confs.Add(new object[] { new SyncConfiguration{
                DownloadBatchSizeInKB = 0,
                UseBulkOperations = true,
                SerializationFormat= Enumerations.SerializationFormat.Binary
            } });

            confs.Add(new object[] { new SyncConfiguration{
                DownloadBatchSizeInKB = 0,
                UseBulkOperations = false,
                SerializationFormat= Enumerations.SerializationFormat.Binary
            } });

        }
        public IEnumerator<object[]> GetEnumerator() => confs.GetEnumerator();
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    }


    public static class TestConfigurations
    {

        /// <summary>
        /// Always return a new list of configurations.
        /// To be sure that no tests will update a property that will be used (instead of default property) in the next test
        /// </summary>
        public static List<SyncConfiguration> GetConfigurations() {

            var Configurations = new List<SyncConfiguration>();

      
            Configurations.Add(new SyncConfiguration
            {

                DownloadBatchSizeInKB = 100,
                UseBulkOperations = true,
                SerializationFormat = Enumerations.SerializationFormat.Json
            });

            Configurations.Add(new SyncConfiguration
            {
                DownloadBatchSizeInKB = 100,
                UseBulkOperations = false,
                SerializationFormat = Enumerations.SerializationFormat.Json
            });

            Configurations.Add(new SyncConfiguration
            {
                DownloadBatchSizeInKB = 0,
                UseBulkOperations = true,
                SerializationFormat = Enumerations.SerializationFormat.Json
            });

            Configurations.Add(new SyncConfiguration
            {
                DownloadBatchSizeInKB = 0,
                UseBulkOperations = false,
                SerializationFormat = Enumerations.SerializationFormat.Json
            });

            Configurations.Add(new SyncConfiguration
            {
                DownloadBatchSizeInKB = 100,
                UseBulkOperations = true,
                SerializationFormat = Enumerations.SerializationFormat.Binary
            });

            Configurations.Add(new SyncConfiguration
            {
                DownloadBatchSizeInKB = 100,
                UseBulkOperations = false,
                SerializationFormat = Enumerations.SerializationFormat.Binary
            });

            Configurations.Add(new SyncConfiguration
            {
                DownloadBatchSizeInKB = 0,
                UseBulkOperations = true,
                SerializationFormat = Enumerations.SerializationFormat.Binary
            });

            Configurations.Add(new SyncConfiguration
            {
                DownloadBatchSizeInKB = 0,
                UseBulkOperations = false,
                SerializationFormat = Enumerations.SerializationFormat.Binary
            });

            return Configurations;
        }
    }

}