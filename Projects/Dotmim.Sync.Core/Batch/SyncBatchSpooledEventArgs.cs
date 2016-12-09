using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.Core.Batch
{
    public class SyncBatchSpooledEventArgs
    {
        int _currentBatchNumber;
        int _totalBatchesSpooled;
        long _dataCacheSize;
        string _batchFileName;
        Dictionary<string, ulong> _currentBatchTableWatermarks;

        /// <summary>
        /// Gets or sets the name of the file to which spooled changes are written.
        /// </summary>
        public string BatchFileName
        {
            get
            {
                return this._batchFileName;
            }
            set
            {
                this._batchFileName = value;
            }
        }

        /// <summary>
        /// Gets the number of the batch that was most recently written to the spooling file.
        /// </summary>
        public int CurrentBatchNumber
        {
            get
            {
                return this._currentBatchNumber;
            }
        }

        /// <summary>
        /// Gets the table name and maximum tickcount value for each table that has changes in the current batch.
        /// </summary>
        public Dictionary<string, ulong> CurrentBatchTableWatermarks
        {
            get
            {
                return this._currentBatchTableWatermarks;
            }
        }

        /// <summary>Gets the size of the current batch.</summary>
        public long DataCacheSize
        {
            get
            {
                return this._dataCacheSize;
            }
        }

        /// <summary>Gets or sets the total number of change batches that were spooled to disk when the <see cref="E:Microsoft.Synchronization.Data.RelationalSyncProvider.BatchSpooled" /> event was raised.</summary>
        public int TotalBatchesSpooled
        {
            get
            {
                return this._totalBatchesSpooled;
            }
            internal set
            {
                this._totalBatchesSpooled = value;
            }
        }

        internal SyncBatchSpooledEventArgs(int number, int totalBatchSize, long cacheSize, Dictionary<string, ulong> watermarks, string batchFileName)
        {
            this._currentBatchNumber = number;
            this._totalBatchesSpooled = totalBatchSize;
            this._currentBatchTableWatermarks = watermarks;
            this._dataCacheSize = cacheSize;
            this._batchFileName = batchFileName;
        }
    }
}
