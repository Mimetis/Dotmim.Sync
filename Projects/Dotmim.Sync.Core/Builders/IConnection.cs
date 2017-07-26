using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.Core.Builders
{
    public interface IConnection
    {
        Task OpenAsync();
        void BeginTransaction(IsolationLevel isolationLevel);
        void Close();
        ConnectionState State { get; }
    }
}
