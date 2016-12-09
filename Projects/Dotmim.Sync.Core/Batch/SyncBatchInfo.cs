using Dotmim.Sync.Data;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.Core.Batch
{
    /// <summary>
    /// Represents a batch header file that contains metadata about the data contained in a 
    /// synchronization batch.
    /// </summary>
    public class SyncBatchInfo : IDisposable
    {

        public static Version DbProviderDataRetrieverVersion = new Version(3, 1);

        internal byte[] _learnedKnowledge;

        /// <summary>
        /// Gets or sets the in-memory size of the current batch.
        /// </summary>
        public long DataCacheSize { get; set; }

        /// <summary>Gets or sets a DmSet object, 
        /// which contains the in-memory data set that represents the batch.
        /// </summary>
        public DmSet DmSet { get; set; }

        /// <summary>Gets or sets an ID that uniquely identifies the batch.</summary>
        public string Id { get; set; }

        /// <summary>
        /// Gets or sets whether the current batch is the last batch of the synchronization session.
        /// </summary>
        public bool IsLastBatch { get; set; }

        /// <summary>
        /// Gets or sets the sequence number of the batch at the source provider so that 
        /// the destination provider processes batches in the correct order.
        /// </summary>
        public uint SequenceNumber { get; set; }

        /// <summary>
        /// Represents the version of Sync Framework that generated the batch file.
        /// </summary>
        public Version Version { get; set; }

        internal Dictionary<string, ulong> MaxEnumeratedTimestamps { get; set; }

        /// <summary>Initializes a new instance of the <see cref="T:Microsoft.Synchronization.Data.DbSyncBatchInfo" /> class.</summary>
        public SyncBatchInfo()
        {
        }

        internal string ConvertTimestampDictionaryToString()
        {
            StringBuilder stringBuilder = new StringBuilder();
            if (this.MaxEnumeratedTimestamps != null)
            {
                foreach (KeyValuePair<string, ulong> _maxEnumeratedTimestamp in this.MaxEnumeratedTimestamps)
                {
                    object[] key = { "[", _maxEnumeratedTimestamp.Key, ",", _maxEnumeratedTimestamp.Value, "]," };
                    stringBuilder.Append(string.Concat(key));
                }
            }
            return stringBuilder.ToString();
        }

        /// <summary>
        /// Releases all resources used by the <see cref="T:Microsoft.Synchronization.Data.DbSyncBatchInfo" />.
        /// </summary>
        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Releases the unmanaged resources used 
        /// by the <see cref="T:Microsoft.Synchronization.Data.DbSyncBatchInfo" /> and optionally releases the managed resources.
        /// </summary>
        protected virtual void Dispose(bool cleanup)
        {
            if (this.DmSet != null)
            {
                this.DmSet.Clear();
                this.DmSet = null;
            }

            this.MaxEnumeratedTimestamps = null;
        }

        /// <summary>
        /// Gets the synchronization knowledge that is learned by the destination after it applies this batch.
        /// </summary>
        public byte[] GetLearnedKnowledge() => this._learnedKnowledge;

        /// <summary>
        /// Sets the synchronization knowledge that is learned by the destination after it applies this batch.
        /// </summary>
        public void SetLearnedKnowledge(byte[] knowledgeBytes)
        {
            if (knowledgeBytes == null)
                throw new ArgumentNullException(nameof(knowledgeBytes));

            this._learnedKnowledge = knowledgeBytes;
        }

        /// <summary>Returns a string that represents the <see cref="T:Microsoft.Synchronization.Data.DbSyncBatchInfo" /> object.</summary>
        /// <returns>A string that represents the <see cref="T:Microsoft.Synchronization.Data.DbSyncBatchInfo" /> object.</returns>
        public override string ToString()
        {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.Append("\n\t\tVersion                   :").Append(this.Version);
            stringBuilder.Append("\n\t\tBatchId                   :").Append(this.Id);
            stringBuilder.Append("\n\t\tBatch Number              :").Append(this.SequenceNumber);
            stringBuilder.Append("\n\t\tIs Last Batch             :").Append(this.IsLastBatch);
            stringBuilder.Append("\n\t\tData Cache Size           :").Append(this.DataCacheSize);
            stringBuilder.Append("\n\t\tTable Watermarks          :").Append(this.ConvertTimestampDictionaryToString());
            return stringBuilder.ToString();
        }
    }
}
