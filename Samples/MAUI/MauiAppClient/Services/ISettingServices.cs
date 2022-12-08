namespace MauiAppClient.Services
{
    public interface ISettingServices
    {
        string DataSource { get; }
        string DataSourcePath { get; }
        string DataSourceName { get; }
        string BatchDirectoryPath { get; }
        string BatchDirectoryName { get; }
        string SyncApiUrl { get; }
        int BatchSize { get; }
    }
}