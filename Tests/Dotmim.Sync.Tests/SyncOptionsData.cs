using Dotmim.Sync.Enumerations;
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
            yield return new object[] { new SyncOptions { BatchSize = 100, TransactionMode = TransactionMode.AllOrNothing, DisableConstraintsOnApplyChanges = true } };
            yield return new object[] { new SyncOptions { BatchSize = 5000, TransactionMode = TransactionMode.PerBatch, DisableConstraintsOnApplyChanges = true, ErrorResolutionPolicy = ErrorResolution.RetryOneMoreTimeAndThrowOnError } };
            yield return new object[] { new SyncOptions { BatchSize = 5000, TransactionMode = TransactionMode.None, DisableConstraintsOnApplyChanges = true, ErrorResolutionPolicy = ErrorResolution.RetryOneMoreTimeAndThrowOnError } };
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }


}
