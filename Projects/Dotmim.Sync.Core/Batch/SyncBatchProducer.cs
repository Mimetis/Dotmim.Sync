//using Dotmim.Sync.Core.Log;
//using Dotmim.Sync.Data;
//using System;
//using System.Collections.Generic;
//using System.Diagnostics;
//using System.IO;
//using System.Linq;
//using System.Text;
//using System.Threading;
//using System.Threading.Tasks;

//namespace Dotmim.Sync.Core.Batch
//{
//    internal class SyncBatchProducer
//    {
//        internal const string BatchFileExtension = ".batch";
//        private const string BatchHeaderFileName = "SyncBatchHeaderFile.sync";
//        private const string RepliacBatchDirectoryPrefix = "sync_";
//        private BinaryFormatter _formatter;
//        internal string _currentSessionSpoolDirectory;
//        private uint _memoryBatchSizeInKb;
//        private uint _batchNumber = 1;
//        private byte[] _madeWithKnowledgeBytes;
//        private byte[] _destinationKnowledgeBytes;
//        private Dictionary<string, ulong> _tableWatermarks;
//        private Dictionary<string, ulong> _resumingTableWatermarks;
//        private Queue<object> _batchQueue = new Queue<object>();
//        private Queue<SyncProgressEventArgs> _tableProgressEventArgs = new Queue<DbSyncProgressEventArgs>();
//        private AutoResetEvent _consumerEvent = new AutoResetEvent(false);
//        private AutoResetEvent _waitForCancellationEvent = new AutoResetEvent(false);
//        private ManualResetEvent _lastBatchQueuedEvent = new ManualResetEvent(false);
//        private SyncBatchDirectoryHeader _batchDirectoryHeader;
//        private bool _cancelBackgroundEnumeration;
//        private EventHandler<SyncBatchSpooledEventArgs> _batchSpooledEventHandler;
//        private object _lockObject = new object();
//        private int _totalBatchesSpooled;

//        public Queue<object> BatchQueue
//        {
//            get
//            {
//                Queue<object> objs;
//                lock (this._lockObject)
//                {
//                    objs = this._batchQueue;
//                }
//                return objs;
//            }
//            set
//            {
//                lock (this._lockObject)
//                {
//                    this._batchQueue = value;
//                }
//            }
//        }
//        public bool CancelBackgroundEnumeration
//        {
//            get
//            {
//                return this._cancelBackgroundEnumeration;
//            }
//        }
//        internal uint MaxMemorySizeInKb
//        {
//            get
//            {
//                return this._memoryBatchSizeInKb;
//            }
//        }
//        public Dictionary<string, ulong> ResumingTableWatermarks
//        {
//            get
//            {
//                return this._resumingTableWatermarks;
//            }
//        }
//        public int TotalBatchesSpooled
//        {
//            get
//            {
//                return this._totalBatchesSpooled;
//            }
//        }
//        public AutoResetEvent WaitForCancellationEvent
//        {
//            get
//            {
//                return this._waitForCancellationEvent;
//            }
//        }

//        public SyncBatchProducer(string batchRequestingReplicaId, string spoolDirectory, uint memoryBatchSize,
//            EventHandler<SyncBatchSpooledEventArgs> handler)
//        {
//            this._batchSpooledEventHandler = handler;
//            this._currentSessionSpoolDirectory = Path.Combine(spoolDirectory, string.Concat("sync_", batchRequestingReplicaId));
//            this._formatter = new BinaryFormatter();
//            this._memoryBatchSizeInKb = memoryBatchSize;
//            this._tableWatermarks = new Dictionary<string, ulong>();

//            this.CheckAndCreateSpoolDirectory(batchRequestingReplicaId);
//        }

//        private bool BackgroundEnumerationComplete()
//        {
//            return this._lastBatchQueuedEvent.WaitOne(0);
//        }

//        private static bool BatchFileIsValid(string batchFileName)
//        {
//            bool flag = false;
//            try
//            {
//                if (File.Exists(batchFileName))
//                {
//                    DbSyncBatchInfoSerializer.Deserialize(batchFileName).Dispose();
//                    flag = true;
//                }
//            }
//            catch (Exception exception)
//            {
//                //if (SyncExpt.IsFatal(exception))
//                //{
//                //    throw;
//                //}
//                Debug.WriteLine("SyncBatchProducer: Unable to deserialize Batch file {0}.", batchFileName);
//            }
//            return flag;
//        }

//        public void CancelBackgroundEnumerationThread()
//        {
//            this.CancelBackgroundEnumerationThread(true);
//        }

//        public void CancelBackgroundEnumerationThread(bool waitForCancellation)
//        {
//            Debug.WriteLine("SyncBatchProducer: Canceling Background enumeration thread. Waiting for cancellation event: {0}", waitForCancellation);
//            this._cancelBackgroundEnumeration = true;

//            if (waitForCancellation)
//            {
//                this._waitForCancellationEvent.WaitOne();
//                Debug.WriteLine("SyncBatchProducer: Canceling Background enumeration thread. Cancellation event fired.");
//            }
//        }

//        private void CheckAndCreateSpoolDirectory(string replicaId)
//        {
//            try
//            {
//                if (Directory.Exists(this._currentSessionSpoolDirectory))
//                {
//                    this.CheckAndTryEnumRestart();
//                }
//                else
//                {
//                    Debug.WriteLine("SyncBatchProducer: Batch spooling directory {0} doesn't exist. Creating.", _currentSessionSpoolDirectory);
//                    Directory.CreateDirectory(this._currentSessionSpoolDirectory);
//                }
//            }
//            catch (Exception exception1)
//            {
//                throw new Exception($"SpoolDirectoryAccessError {_currentSessionSpoolDirectory} , {replicaId}", exception1);
//            }
//        }

//        private void CheckAndDeleteBatchingDirectory(object batchItem)
//        {
//            if (this._batchDirectoryHeader == null && (batchItem is DmSet || batchItem is Exception))
//            {
//                Debug.WriteLine("SyncBatchProducer: Deleting spooling directory as we reverted to non batched mode");

//                if (Directory.Exists(this._currentSessionSpoolDirectory))
//                {
//                    try
//                    {
//                        Directory.Delete(this._currentSessionSpoolDirectory);
//                    }
//                    catch
//                    {
//                    }
//                }
//            }
//        }

//        private void CheckAndEnqueueTableProgressEvents(object batchItem)
//        {
//            bool dataSetExist = false;
//            DmSet dataSet = batchItem as DmSet;

//            if (dataSet != null)
//                dataSetExist = true;

//            if (!dataSetExist)
//                if (!(batchItem is string))
//                    return;

//            while (this._tableProgressEventArgs.Count > 0)
//            {
//                SyncProgressEventArgs item = this._tableProgressEventArgs.Dequeue();
//                if (dataSetExist)
//                {
//                    item.TableProgress.BatchFileName = null;
//                    item.TableProgress.DmTable = dataSet.Tables[item.TableProgress.TableName];
//                }
//                this.EnqueueBatchItem(item, false);
//            }
//        }

//        /// <summary>
//        /// Check if an header already exists
//        /// </summary>
//        private void CheckAndRetrieveBatchDirectoryHeader()
//        {
//            if (this._batchDirectoryHeader != null)
//                return;

//            string str = Path.Combine(this._currentSessionSpoolDirectory, "SyncBatchHeaderFile.sync");
//            if (!File.Exists(str))
//                return;

//            FileStream fileStream = new FileStream(str, FileMode.Open);
//            try
//            {
//                this._batchDirectoryHeader = (SyncBatchDirectoryHeader)this._formatter.Deserialize(fileStream);
//            }
//            finally
//            {
//                fileStream.Close();
//            }

//        }

//        private void CheckAndTryEnumRestart()
//        {
//            bool shouldCleanup = false;
//            try
//            {

//                Debug.WriteLine("SyncBatchProducer: Directory exists. Checking if it contains header file.");
//                // Check the header
//                this.CheckAndRetrieveBatchDirectoryHeader();

//                // If no header, delete all files
//                if (this._batchDirectoryHeader == null)
//                {
//                    this.CleanupAllBatchFiles();
//                    return;
//                }

//                Logger.Current.Info("SyncBatchProducer: Directory header exists. Checking if the batch size. Header batch size: {0}, Current Batch Size: {1}", this._batchDirectoryHeader.DataCacheSizeInBytes, this._memoryBatchSizeInKb * 1024);
//                if (this._batchDirectoryHeader.DataCacheSizeInBytes <= this._memoryBatchSizeInKb * 1024)
//                {
//                    Logger.Current.Info("SyncBatchProducer: Directory header exists. Check to see if it can be reused based on current/saved destination knowledge.");
//                    if (this.IsExistingBatchReusable())
//                    {
//                        Logger.Current.Info("SyncBatchProducer: Checking if it contains last batch file name.");
//                        Logger.Current.Info($"SyncBatchProducer: Directory header details. {this._batchDirectoryHeader.ToString()}");

//                        if (this._batchDirectoryHeader.EndBatchFileName == null || !SyncBatchProducer.BatchFileIsValid(Path.Combine(this._currentSessionSpoolDirectory, this._batchDirectoryHeader.EndBatchFileName)))
//                        {
//                            Logger.Current.Info("SyncBatchProducer: Last batch file doesn't exist on disk or is not valid. Deleting all batch files and restarting enumeration.");
//                            this.CleanupAllBatchFiles();
//                        }
//                        else
//                        {
//                            Logger.Current.Info("SyncBatchProducer: Directory header contains last batch file name. Re-queuing file list from header file.");
//                            this.ReEnqueueAlreadyEnumeratedBatches();
//                            Logger.Current.Info("SyncBatchProducer: Resetting last batch file name on header.");
//                            this._batchDirectoryHeader.EndBatchFileName = null;
//                            return;
//                        }
//                    }
//                    else
//                    {
//                        this.CleanupAllBatchFiles();
//                        return;
//                    }
//                }
//                else
//                {
//                    Logger.Current.Info("SyncBatchProducer: Directory header exists. Old batch size bigger than current requested batch size. Deleting old batches and restarting new enumeration.");
//                    this.CleanupAllBatchFiles();
//                    return;
//                }

//            }
//            catch (Exception exception1)
//            {
//                //if (SyncExpt.IsFatal(exception1))
//                //{
//                //    throw;
//                //}
//                shouldCleanup = true;
//                Trace.TraceError($"SyncBatchProducer: Unexpected error during enumeration restart attemp. Error: {exception1}");
//            }
//            finally
//            {
//                if (shouldCleanup)
//                {
//                    Logger.Current.Info("SyncBatchProducer: Deleting all files from spooling directory and restarting a new enumeration.");
//                    this.CleanupAllBatchFiles();
//                }
//            }
//        }

//        private void CleanupAllBatchFiles()
//        {
//            if (Directory.Exists(this._currentSessionSpoolDirectory))
//            {
//                string[] files = Directory.GetFiles(this._currentSessionSpoolDirectory);
//                for (int i = 0; i < files.Length; i++)
//                {
//                    string str = files[i];
//                    Logger.Current.Info("SyncBatchProducer: Deleting batch file {0}", str);
//                    File.Delete(str);
//                }
//            }
//            this._batchDirectoryHeader = null;
//            this.BatchQueue.Clear();
//            this._totalBatchesSpooled = 0;
//            this._batchNumber = 1;
//            this._resumingTableWatermarks = null;
//            this._tableWatermarks = new Dictionary<string, ulong>();
//            this._consumerEvent.Reset();
//        }

//        /// <summary>
//        /// Get the last row, extract the sync row timestamp and save the timestamp for this datatable in a dictionary
//        /// </summary>
//        public void ComputeTableWatermarks(DmTable table)
//        {
//            ulong rowTimestamp;
//            if (table.Rows.Count > 0)
//            {
//                DmRow lastRow = table.Rows[table.Rows.Count - 1];
//                bool isRowStateDeleted = lastRow.RowState == DmRowState.Deleted;

//                if (isRowStateDeleted)
//                    lastRow.RejectChanges();

//                SyncUtil.ParseTimestamp(lastRow["sync_row_timestamp"], out rowTimestamp);
//                Logger.Current.Info("SyncBatchProducer: Read last row's Sync_row_timestamp value for table {0} as {1}.", table.TableName, rowTimestamp);

//                if (isRowStateDeleted)
//                    lastRow.Delete();

//                this._tableWatermarks[table.TableName] = rowTimestamp;
//            }
//        }

//        /// <summary>
//        /// Dequeue the batch
//        /// </summary>
//        /// <returns>could be a Dataset or an EventArgs (or string file name)</returns>
//        public object DequeueBatch()
//        {
//            if (this.IsQueueEmpty())
//            {
//                Logger.Current.Info("SyncBatchProducer: Producer queue empty.  Waiting for queue availablility event");
//                this._consumerEvent.WaitOne();
//                Logger.Current.Info("SyncBatchProducer: Queue availablility event fired");
//            }
//            Logger.Current.Info("SyncBatchProducer: Resetting Queue availablility event.");
//            this._consumerEvent.Reset();

//            if (this.BatchQueue.Peek() is Exception)
//                throw (Exception)this.BatchQueue.Dequeue();

//            return this.BatchQueue.Dequeue();
//        }

//        private void EnqueueBatchAndRaiseEvent(SyncBatchInfo batchInfo)
//        {
//            Logger.Current.Info($"SyncBatchProducer: Enqueuing Batch. Batch Details: {batchInfo}");

//            if (batchInfo.IsLastBatch)
//                this._batchDirectoryHeader.EndBatchFileName = string.Concat(batchInfo.Id, ".batch");

//            this._batchDirectoryHeader.BatchFileNames.Add(string.Concat(batchInfo.Id, ".batch"));
//            this.SaveBatchDirectoryHeader();
//            string str = this.SaveBatchFile(batchInfo);

//            this._totalBatchesSpooled = this._totalBatchesSpooled + 1;
//            Logger.Current.Info($"SyncBatchProducer: Enqueuing Batch. Total Batches Enqueued: {this._totalBatchesSpooled}. Details: {batchInfo}");

//            this.EnqueueBatchSpooledEvent(batchInfo, str);

//            this.EnqueueBatchItem(str, batchInfo.IsLastBatch);

//            batchInfo.Dispose();
//        }

//        internal void EnqueueBatchItem(object batchItem, bool isLastBatch)
//        {
//            this.CheckAndDeleteBatchingDirectory(batchItem);

//            lock (this._lockObject)
//            {
//                this.CheckAndEnqueueTableProgressEvents(batchItem);
//                this.BatchQueue.Enqueue(batchItem);
//                if (isLastBatch)
//                {
//                    this._lastBatchQueuedEvent.Set();
//                }
//                this.SignalBatchAvailableEvent();
//            }
//        }

//        private void EnqueueBatchSpooledEvent(SyncBatchInfo batchInfo, string batchFileName)
//        {
//            if (this._batchSpooledEventHandler != null)
//            {
//                Logger.Current.Info("SyncBatchProducer: Enqueuing DbBatchSpooled event.");
//                this.EnqueueBatchItem(new SyncBatchSpooledEventArgs(this._totalBatchesSpooled, -1, batchInfo.DataCacheSize, batchInfo.MaxEnumeratedTimestamps, batchFileName), false);
//            }
//        }

//        public void EnqueueTableProgressEvent(SyncProgressEventArgs tableProgressArgs)
//        {
//            this._tableProgressEventArgs.Enqueue(tableProgressArgs);
//        }

//        public string GetBatchFileName(string batchId)
//        {
//            return Path.Combine(this._currentSessionSpoolDirectory, string.Concat(batchId, ".batch"));
//        }

//        private bool IsExistingBatchReusable()
//        {
//            return false;
//            //SyncKnowledge syncKnowledge = SyncKnowledge.Deserialize(this._batchDirectoryHeader.IdFormat, this._batchDirectoryHeader.DestinationKnowledgeBytes);
//            //SyncKnowledge syncKnowledge1 = SyncKnowledge.Deserialize(this._batchDirectoryHeader.IdFormat, this._destinationKnowledgeBytes);

//            //Logger.Current.Info("SyncBatchProducer: Destination knowledge read from preexisting batch header file: {0}", syncKnowledge.ToString());
//            //Logger.Current.Info("SyncBatchProducer: Current Destination knowledge: {0}", syncKnowledge1.ToString());

//            //Guid syncId = syncKnowledge.ReplicaKeyMap.LookupReplicaId(0);
//            //Guid syncId1 = syncKnowledge1.ReplicaKeyMap.LookupReplicaId(0);

//            //if (syncId != syncId1)
//            //{
//            //    Logger.Current.Info("SyncBatchProducer: Local replica ids do not match.  Batch cannot be reused.");
//            //    return false;
//            //}
//            //ulong num = syncKnowledge.FindMinTickCountForReplica(syncId);
//            //ulong num1 = syncKnowledge1.FindMinTickCountForReplica(syncId1);
//            //if (num < num1)
//            //{
//            //    SyncUtil.SetLocalTickCountRanges(syncKnowledge, num1);
//            //}
//            //bool flag = syncKnowledge.Contains(syncKnowledge1);
//            //if (!flag)
//            //{
//            //    Logger.Current.Info("SyncBatchProducer: Knowledge in batch does not contain current destination knowledge.  Batch cannot be reused.");
//            //}
//            //return flag;
//        }

//        public bool IsLastBatchDequeued()
//        {
//            if (!this.BackgroundEnumerationComplete())
//            {
//                return false;
//            }
//            return this.BatchQueue.Count == 0;
//        }

//        public bool IsQueueEmpty()
//        {
//            Logger.Current.Info("SyncBatchProducer: Producer queue count = {0}", this.BatchQueue.Count);
//            return this.BatchQueue.Count == 0;
//        }

//        public void PrepareAndQueueBatch(Guid batchId, DmSet batchDmSet, long dataCacheSize)
//        {
//            this.PrepareAndQueueBatch(batchId, batchDmSet, dataCacheSize, null, false);
//        }

//        public void PrepareAndQueueBatch(Guid batchId, DmSet batchDmSet, long dataCacheSize, Exception exception, bool isLastBatch)
//        {
//            try
//            {
//                this.CheckAndRetrieveBatchDirectoryHeader();
//                if (exception != null)
//                {
//                    Trace.TraceError("SyncBatchProducer: Background enumeration encountered an error.");
//                    this.EnqueueBatchItem(exception, true);
//                    return;
//                }

//                if (this._batchDirectoryHeader == null)
//                {
//                    if (batchDmSet != null && isLastBatch)
//                    {
//                        Logger.Current.Info("SyncBatchProducer: Received 1st and last DmSet. Skip to Non batched mode.");
//                        this.EnqueueBatchItem(batchDmSet, true);
//                        return;
//                    }

//                    this._batchDirectoryHeader = new SyncBatchDirectoryHeader()
//                    {
//                        DataCacheSizeInBytes = (ulong)(this._memoryBatchSizeInKb * 1024),
//                        BatchFileNames = new List<string>(),
//                        StartBatchFileName = string.Concat(batchId.ToString(), ".batch")
//                    };
//                }
//                // Create the batch info
//                SyncBatchInfo dbSyncBatchInfo = new SyncBatchInfo()
//                {
//                    Version = SyncBatchInfo.DbProviderDataRetrieverVersion,
//                    Id = batchId.ToString()
//                };
//                // set the sequence number and then increment the batch number
//                dbSyncBatchInfo.SequenceNumber = this._batchNumber;
//                this._batchNumber += 1;

//                dbSyncBatchInfo.MaxEnumeratedTimestamps = new Dictionary<string, ulong>(this._tableWatermarks);
//                dbSyncBatchInfo.DmSet = batchDmSet;
//                dbSyncBatchInfo.DataCacheSize = dataCacheSize;
//                dbSyncBatchInfo.IsLastBatch = isLastBatch;

//                this.EnqueueBatchAndRaiseEvent(dbSyncBatchInfo);

//            }
//            catch (Exception ex)
//            {
//                Trace.TraceError("SyncBatchProducer: Exception in PrepareAndQueueBatch. {0}", ex.Message);
//                this.EnqueueBatchItem(new ApplicationException("PrepareAndQueueBatchError", ex), true);
//            }
//        }

//        private void ReEnqueueAlreadyEnumeratedBatches()
//        {
//            string str = null;
//            for (int i = 0; i < this._batchDirectoryHeader.BatchFileNames.Count; i++)
//            {
//                str = Path.Combine(this._currentSessionSpoolDirectory, this._batchDirectoryHeader.BatchFileNames[i]);
//                SyncBatchInfo dbSyncBatchInfo = SyncBatchInfoSerializer.Deserialize(str);
//                dbSyncBatchInfo.IsLastBatch = false;
//                if (i == this._batchDirectoryHeader.BatchFileNames.Count - 1)
//                {
//                    Logger.Current.Info("SyncBatchProducer: Resuming from a previous enumeration. Reading table watermarks as {0}", dbSyncBatchInfo.ConvertTimestampDictionaryToString());
//                    this._resumingTableWatermarks = dbSyncBatchInfo.MaxEnumeratedTimestamps;
//                    this._tableWatermarks = dbSyncBatchInfo.MaxEnumeratedTimestamps;
//                }
//                this.ReEnqueueBatchAndRaiseEvent(dbSyncBatchInfo, str);
//                SyncBatchProducer dbSyncBatchProducer = this;
//                dbSyncBatchProducer._batchNumber = dbSyncBatchProducer._batchNumber + 1;
//            }
//        }

//        private void ReEnqueueBatchAndRaiseEvent(SyncBatchInfo batchInfo, string batchFileName)
//        {
//            Logger.Current.Info("SyncBatchProducer: ReEnqueuing already Batch. Batch Details: {0}", batchInfo);
//            SyncBatchProducer dbSyncBatchProducer = this;
//            dbSyncBatchProducer._totalBatchesSpooled = dbSyncBatchProducer._totalBatchesSpooled + 1;
//            Logger.Current.Info("SyncBatchProducer: Enqueuing Batch. Total Batches Enqueued: {0}. Details: {1}", this._totalBatchesSpooled, batchInfo);

//            this.EnqueueBatchSpooledEvent(batchInfo, batchFileName);
//            this.EnqueueBatchItem(batchFileName, false);
//            batchInfo.Dispose();
//        }

//        private void SaveBatchDirectoryHeader()
//        {
//            string str = Path.Combine(this._currentSessionSpoolDirectory, "SyncBatchHeaderFile.sync");
//            using (FileStream fileStream = new FileStream(str, FileMode.Create))
//            {
//                try
//                {
//                    if (this._batchDirectoryHeader.Version != null)
//                        this._batchDirectoryHeader.Version = DbSyncBatchInfo.DbProviderDataRetrieverVersion;

//                    this._formatter.Serialize(fileStream, this._batchDirectoryHeader);
//                }
//                finally
//                {
//                    fileStream.Flush();
//                    fileStream.Close();
//                }
//            }
//        }

//        private string SaveBatchFile(DbSyncBatchInfo dbSyncBatchInfo)
//        {
//            string batchFileName = this.GetBatchFileName(dbSyncBatchInfo.Id);
//            DbSyncBatchInfoSerializer.Serialize(dbSyncBatchInfo, batchFileName);
//            return batchFileName;
//        }

//        private void SignalBatchAvailableEvent()
//        {
//            if (this.BatchQueue.Count == 1)
//            {
//                Logger.Current.Info("SyncBatchProducer: Signalling Queue availability event. Queue Count: {0}", this.BatchQueue.Count);

//                this._consumerEvent.Set();
//            }
//        }

//    }
//}
