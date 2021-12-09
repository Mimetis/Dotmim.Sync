using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.Serialization
{
    public interface ILocalSerializer
    {
        Task CloseFileAsync(string path, SyncTable shemaTable);
        Task OpenFileAsync(string path, SyncTable shemaTable);
        Task WriteRowToFileAsync(SyncRow row, SyncTable shemaTable);
        Task<long> GetCurrentFileSizeAsync();
        IEnumerable<SyncRow> ReadRowsFromFile(string path, SyncTable shemaTable);
        string Extension { get; }
    }
}
