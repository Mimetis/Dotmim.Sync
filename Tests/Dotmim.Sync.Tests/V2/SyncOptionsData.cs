using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Tests.V2
{
    public class SyncOptionsData : IEnumerable<object[]>
    {

        public IEnumerator<object[]> GetEnumerator()
        {
            yield return new object[] { new SyncOptions { BatchSize = 0 } };
            yield return new object[] { new SyncOptions { BatchSize = 500 } };
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}
