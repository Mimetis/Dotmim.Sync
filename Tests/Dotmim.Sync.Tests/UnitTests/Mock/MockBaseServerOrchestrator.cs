using Dotmim.Sync.Enumerations;
using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Tests.UnitTests
{
    public class MockBaseServerOrchestrator : RemoteOrchestrator
    {
        public MockBaseServerOrchestrator(CoreProvider provider, SyncOptions options, SyncSetup setup, string scopeName = "DefaultScope")
            : base(provider, options, setup, scopeName)
        {
        }

        public override SyncSide Side => SyncSide.ServerSide;

    }

    public class MockBaseClientOrchestrator : LocalOrchestrator
    {
        public MockBaseClientOrchestrator(CoreProvider provider, SyncOptions options, SyncSetup setup, string scopeName = "DefaultScope")
            : base(provider, options, setup, scopeName)
        {
        }

        public override SyncSide Side => SyncSide.ClientSide;

    }


}
