using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Running;

namespace Benchmarks
{
    public class Program
    {
#if DEBUG
        private static void Main(string[] args) => BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args, new DebugInProcessConfig());
#else
        static void Main(string[] args)
        {
            _ = BenchmarkRunner.Run<SchemaBenchmarks>();
        }
#endif
    }
}