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
            yield return new object[] { new SyncOptions { BatchSize = 100 , TransactionMode = Enumerations.TransactionMode.AllOrNothing } };
            yield return new object[] { new SyncOptions { BatchSize = 5000, TransactionMode = Enumerations.TransactionMode.PerBatch } };
            yield return new object[] { new SyncOptions { BatchSize = 5000, TransactionMode = Enumerations.TransactionMode.None } };
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
