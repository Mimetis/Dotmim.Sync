using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Core.Test.Misc
{
    public class RaisingClass<T> where T: EventArgs
    {
        public void RaiseWithArgs(T args)
        {
            Completed.Invoke(this, args);
        }

        public event EventHandler<T> Completed;
    }
}
