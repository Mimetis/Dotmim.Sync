using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Tests.Core;
using Dotmim.Sync.Tests.Models;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;
using System.Transactions;
using static Microsoft.EntityFrameworkCore.DbLoggerCategory.Database;

namespace Dotmim.Sync.Tests.Fixtures
{

    public class DatabaseServerFixture : IDisposable
    {
        public Stopwatch OverallStopwatch { get; }

        public DatabaseServerFixture() => this.OverallStopwatch = Stopwatch.StartNew();

        public void Dispose() => this.OverallStopwatch.Stop();

    }
}
