using BenchmarkDotNet.Attributes;
using Dotmim.Sync;
using Dotmim.Sync.Sqlite;
using Dotmim.Sync.SqlServer;

namespace Benchmarks
{
    [MemoryDiagnoser]
    [Config(typeof(AntiVirusFriendlyConfig))]
    public class SchemaBenchmarks
    {
        private const int IterationsNum = 1;

        private const string ServerConnectionString = "Data Source=(localdb)\\mssqllocaldb; Initial Catalog=AdventureWorks; Integrated Security=true;MultipleActiveResultSets=False;";
        private const string ClientConnectionString = "Data Source=(localdb)\\mssqllocaldb; Initial Catalog=Client; Integrated Security=true;MultipleActiveResultSets=False;";

        private SqliteSyncProvider sqliteClientProvider = new SqliteSyncProvider(Path.GetRandomFileName().Replace(".", "").ToLowerInvariant() + ".db");
        private SqlSyncProvider serverProvider = new SqlSyncProvider(ServerConnectionString);

        private SyncSetup setup = new SyncSetup("ProductCategory");

        private ScopeInfo? serverScopeInfo;



        [GlobalSetup]
        public async Task GlobalSetup()
        {
            var remoteOrchestrator = new RemoteOrchestrator(serverProvider);
            await remoteOrchestrator.DeprovisionAsync(setup).ConfigureAwait(false);
            serverScopeInfo = await remoteOrchestrator.ProvisionAsync(setup).ConfigureAwait(false);
            await remoteOrchestrator.DeprovisionAsync(setup).ConfigureAwait(false);

        }

        [Benchmark]
        [IterationCount(IterationsNum)]
        public async Task ProvisionDeprovisionSqlServer()
        {
            var remoteOrchestrator = new RemoteOrchestrator(serverProvider);

            await remoteOrchestrator.ProvisionAsync(setup).ConfigureAwait(false);
            await remoteOrchestrator.DeprovisionAsync(setup).ConfigureAwait(false);
        }

        [Benchmark]
        [IterationCount(IterationsNum)]
        public async Task ProvisionDeprovisionSqlite()
        {
            var localOrchestrator = new LocalOrchestrator(sqliteClientProvider);

            await localOrchestrator.ProvisionAsync(serverScopeInfo).ConfigureAwait(false);
            await localOrchestrator.DeprovisionAsync(setup).ConfigureAwait(false);

        }
    }
}