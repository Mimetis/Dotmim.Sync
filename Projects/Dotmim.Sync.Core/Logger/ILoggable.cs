using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync
{
    public interface ILoggable
    {
        /// <summary>
        /// Get log of the current instance
        /// </summary>
        (string Message, object[] Args) GetLog();
    }
}
