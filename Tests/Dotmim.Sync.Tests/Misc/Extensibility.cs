using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Tests.Misc
{
    internal static class Extensibility
    {

        public static void LogErrors(this SyncAgent agent)
        {
            agent.LocalOrchestrator.OnApplyChangesErrorOccured(args => Console.WriteLine($"LocalOrchestrator. {args.Message}"));
            agent.LocalOrchestrator.OnRowsChangesFallbackFromBatchToSingleRowApplying(args => Console.WriteLine($"LocalOrchestrator. {args.Message}"));
            agent.LocalOrchestrator.OnTransientErrorOccured(args => Console.WriteLine($"LocalOrchestrator. {args.Message}"));
            agent.RemoteOrchestrator.OnApplyChangesErrorOccured(args => Console.WriteLine($"RemoteOrchestrator. {args.Message}"));
            agent.RemoteOrchestrator.OnRowsChangesFallbackFromBatchToSingleRowApplying(args => Console.WriteLine($"RemoteOrchestrator. {args.Message}"));
            agent.RemoteOrchestrator.OnTransientErrorOccured(args => Console.WriteLine($"RemoteOrchestrator. {args.Message}"));

        }

    }
}
