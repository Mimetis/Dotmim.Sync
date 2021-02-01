namespace XamSyncSample.Services
{
    public interface ISettingServices
    {
        string DataSource { get; }
        string DataSourcePath { get; }
        string DataSourceName { get; }
        string SyncApiUrl { get; }
        int SyncBatchSize { get; }
    }
}