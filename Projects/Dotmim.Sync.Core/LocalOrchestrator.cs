using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    public class LocalOrchestrator : ILocalOrchestrator<IProvider>
    {
        public Task<string> ApplyChangesAsync(object remoteChanges)
        {
            throw new NotImplementedException();
        }

        public Task<object> GetChangesAsync(object localChanges)
        {
            throw new NotImplementedException();
        }

        public void On(InterceptorBase interceptor)
        {
            throw new NotImplementedException();
        }

        public void On<T>(Action<T> interceptorAction) where T : ProgressArgs
        {
            throw new NotImplementedException();
        }

        public void SetConfiguration(Action<SyncConfiguration> configuration)
        {
            throw new NotImplementedException();
        }

        public void SetOptions(Action<SyncOptions> options)
        {
            throw new NotImplementedException();
        }

        public void SetProgress(IProgress<ProgressArgs> progress)
        {
            throw new NotImplementedException();
        }

        public void SetProvider(IProvider coreProvider)
        {
            throw new NotImplementedException();
        }
    }
}
