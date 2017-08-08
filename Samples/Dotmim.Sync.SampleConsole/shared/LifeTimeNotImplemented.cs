using Microsoft.AspNetCore.Hosting;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace Dotmim.Sync.SampleConsole.shared
{
    public class LifetimeNotImplemented : IApplicationLifetime
    {
        public CancellationToken ApplicationStarted
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        public CancellationToken ApplicationStopped
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        public CancellationToken ApplicationStopping
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        public void StopApplication()
        {
            throw new NotImplementedException();
        }
    }
}
