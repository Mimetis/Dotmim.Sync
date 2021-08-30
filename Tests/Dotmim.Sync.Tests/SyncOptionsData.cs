using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Tests
{
    public class SyncOptionsData : IEnumerable<object[]>
    {

        public IEnumerator<object[]> GetEnumerator()
        {
            yield return new object[] { new SyncOptions { BatchSize = 50 } };
            //yield return new object[] { new SyncOptions { BatchSize = 0 , UseBulkOperations = false} };
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
