using Dotmim.Sync.Enumerations;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync.Serialization
{
    /// <summary>
    /// Serialize json rows locally
    /// </summary>
    public class LocalJsonSerializer : IDisposable, IAsyncDisposable
    {
        private static readonly ISerializer serializer = SerializersCollection.JsonSerializerFactory.GetSerializer();

        private readonly SemaphoreSlim writerLock = new(1, 1);

        private StreamWriter sw;
        private Utf8JsonWriter writer;
        private Func<SyncTable, object[], Task<string>> writingRowAsync;
        private Func<SyncTable, string, Task<object[]>> readingRowAsync;
        private int isOpen;
        private bool disposedValue;

        public LocalJsonSerializer(BaseOrchestrator orchestrator = null, SyncContext context = null)
        {
            if (orchestrator == null)
                return;

            if (orchestrator.HasInterceptors<DeserializingRowArgs>())
                this.OnReadingRow(async (schemaTable, rowString) =>
                {
                    var args = new DeserializingRowArgs(context, schemaTable, rowString);
                    await orchestrator.InterceptAsync(args).ConfigureAwait(false);
                    return args.Result;
                });

            if (orchestrator.HasInterceptors<SerializingRowArgs>())
                this.OnWritingRow(async (schemaTable, rowArray) =>
                {
                    var args = new SerializingRowArgs(context, schemaTable, rowArray);
                    await orchestrator.InterceptAsync(args).ConfigureAwait(false);
                    return args.Result;
                });
        }

        ~LocalJsonSerializer()
        {
            CloseFile();
        }

        /// <summary>
        /// Returns if the file is opened
        /// </summary>
        public bool IsOpen
        {
            get => Interlocked.CompareExchange(ref isOpen, 0, 0) == 1;
            set => Interlocked.Exchange(ref isOpen, value ? 1 : 0);
        }

        /// <summary>
        /// Gets the file extension
        /// </summary>
        public string Extension => "json";

        /// <summary>
        /// Close the current file, close the writer
        /// </summary>
        public void CloseFile()
        {
            // Close file
            if (!this.IsOpen)
                return;

            writerLock.Wait();

            try
            {
                this.writer.WriteEndArray();
                this.writer.WriteEndObject();
                this.writer.Flush();
                this.writer.Dispose();
                this.sw.Dispose();
                this.IsOpen = false;
            }
            finally
            {
                writerLock.Release();
            }
        }

        /// <summary>
        /// Close the current file, close the writer
        /// </summary>
        public async Task CloseFileAsync()
        {
            // Close file
            if (!this.IsOpen)
                return;

            await writerLock.WaitAsync();

            try
            {
                this.writer.WriteEndArray();
                this.writer.WriteEndObject();
                await this.writer.FlushAsync();
                await this.writer.DisposeAsync();
#if NET6_0_OR_GREATER
                await this.sw.DisposeAsync();
#else
                this.sw.Dispose();
#endif
                this.IsOpen = false;
            }
            finally
            {
                writerLock.Release();
            }
        }

        /// <summary>
        /// Open the file and write header
        /// </summary>
        public void OpenFile(string path, SyncTable schemaTable, SyncRowState state, bool append = false)
        {
            if (this.writer != null)
            {
                await writerLock.WaitAsync();

                try
                {
                    if (this.writer != null)
                    {
                        this.writer.Dispose();
                        this.writer = null;
                    }
                }
                finally
                {
                    writerLock.Release();
                }
            }

            this.IsOpen = true;

            var fi = new FileInfo(path);

            if (!fi.Directory.Exists)
                fi.Directory.Create();

            await writerLock.WaitAsync();

            try
            {
                this.sw = new StreamWriter(path, append);
                this.writer = new Utf8JsonWriter(sw.BaseStream);

                this.writer.WriteStartObject();
                this.writer.WriteString("t", schemaTable.TableName);
                this.writer.WriteString("s", schemaTable.SchemaName);
                this.writer.WriteNumber("st", (int)state);

                this.writer.WriteStartArray("c");
                foreach (var c in schemaTable.Columns)
                {
                    this.writer.WriteStartObject();
                    this.writer.WriteString("n", c.ColumnName);
                    this.writer.WriteString("t", c.DataType);
                    if (schemaTable.IsPrimaryKey(c.ColumnName))
                    {
                        this.writer.WriteNumber("p", 1);
                    }
                    this.writer.WriteEndObject();
                }

                this.writer.WriteEndArray();
                this.writer.WriteStartArray("r");
                this.writer.Flush();
            }
            finally
            {
                writerLock.Release();
            }
        }

        /// <summary>
        /// Append a sync row to the writer
        /// </summary>
        public async Task WriteRowToFileAsync(SyncRow row, SyncTable schemaTable)
        {
            var innerRow = row.ToArray();

            string str;

            if (this.writingRowAsync != null)
            {
                str = await this.writingRowAsync(schemaTable, innerRow);
            }
            else
            {
                str = string.Empty; // This won't ever be used, but is need to compile.
            }

            await writerLock.WaitAsync();

            try
            {
                writer.WriteStartArray();

                if (this.writingRowAsync != null)
                {
                    writer.WriteStringValue(str);
                }
                else
                {
                    for (var i = 0; i < innerRow.Length; i++)
                    {
                        var jsonBytes = serializer.Serialize(innerRow[i]);
                        this.writer.WriteRawValue(jsonBytes);
                    }
                }

                writer.WriteEndArray();
                writer.Flush();
            }
            finally
            {
                writerLock.Release();
            }
        }

        /// <summary>
        /// Interceptor on writing row
        /// </summary>
        public void OnWritingRow(Func<SyncTable, object[], Task<string>> func) => this.writingRowAsync = func;

        /// <summary>
        /// Interceptor on reading row
        /// </summary>
        public void OnReadingRow(Func<SyncTable, string, Task<object[]>> func) => this.readingRowAsync = func;

        /// <summary>
        /// Gets the current file size
        /// </summary>
        /// <returns>Current file size as long</returns>
        public async Task<long> GetCurrentFileSizeAsync()
        {
            long position = 0L;

            await writerLock.WaitAsync();

            try
            {
                if (this.sw?.BaseStream != null)
                {
                    position = this.sw.BaseStream.Position / 1024L;
                }
            }
            finally
            {
                writerLock.Release();
            }

            return position;
        }

        /// <summary>
        /// Get the table contained in a serialized file
        /// </summary>
        public static (SyncTable schemaTable, int rowsCount, SyncRowState state) GetSchemaTableFromFile(string path)
        {
            if (!File.Exists(path))
                return default;

            string tableName = null, schemaName = null;
            int rowsCount = 0;

            SyncTable schemaTable = null;
            SyncRowState state = SyncRowState.None;

            using var stream = File.OpenRead(path);
            using var doc = JsonDocument.Parse(stream);
            var root = doc.RootElement;

            foreach (var property in root.EnumerateObject())
            {
                switch (property.Name)
                {
                    case "n":
                        tableName = property.Value.GetString();
                        break;
                    case "s":
                        schemaName = property.Value.GetString();
                        break;
                    case "st":
                        state = (SyncRowState)property.Value.GetInt32();
                        break;
                    case "c": // Dont want to read columns if any
                        schemaTable = GetSchemaTableFromReader(property.Value, tableName, schemaName);
                        break;
                    case "r":
                        bool schemaEmpty = schemaTable == null;

                        if (schemaEmpty)
                        {
                            schemaTable = new SyncTable(tableName, schemaName);
                        }

                        foreach (var array in property.Value.EnumerateArray())
                        {
                            if (schemaEmpty && array.GetArrayLength() >= 2)
                            {
                                for (int i = 1; i < array.GetArrayLength(); i++)
                                {
                                    var type = array[i].ValueKind == JsonValueKind.Null ? typeof(object) : array[i].GetType();
                                    schemaTable.Columns.Add($"C{i}", type);
                                }
                                schemaEmpty = false;
                            }
                            rowsCount++;
                        }

                        break;
                }
            }

            return (schemaTable, rowsCount, state);
        }

        /// <summary>
        /// Enumerate all rows from file
        /// </summary>
        public IEnumerable<SyncRow> GetRowsFromFile(string path, SyncTable schemaTable)
        {
            if (!File.Exists(path))
                yield break;

            using var stream = File.OpenRead(path);
            using var document = JsonDocument.Parse(stream);
            var root = document.RootElement;

            SyncRowState state = SyncRowState.None;

            string tableName = null, schemaName = null;

            foreach (var property in root.EnumerateObject())
            {
                switch (property.Name)
                {
                    case "n":
                        tableName = property.Value.GetString();
                        break;
                    case "s":
                        schemaName = property.Value.GetString();
                        break;
                    case "st":
                        state = (SyncRowState)property.Value.GetInt32();
                        break;
                    case "c":
                        var tmpTable = GetSchemaTableFromReader(property.Value, schemaTable?.TableName ?? tableName, schemaTable?.SchemaName ?? schemaName);

                        if (tmpTable != null)
                            schemaTable = tmpTable;

                        //if (schemaTable == null && tmpTable != null)
                        //    schemaTable = tmpTable;

                        continue;
                    case "r":
                        bool schemaEmpty = schemaTable == null;

                        if (schemaEmpty)
                        {
                            schemaTable = new SyncTable(tableName, schemaName);
                        }

                        // read all array
                        foreach (var arrayElement in property.Value.EnumerateArray())
                        {
                            if (arrayElement.ValueKind == JsonValueKind.Array)
                            {
                                List<object> array = new List<object>();

                                foreach (var item in arrayElement.EnumerateArray())
                                {
                                    array.Add(item.ValueKind switch
                                    {
                                        JsonValueKind.String => item.GetString(),
                                        JsonValueKind.Number => item.GetDecimal(),
                                        JsonValueKind.True => true,
                                        JsonValueKind.False => false,
                                        _ => null
                                    });
                                }

                                if (array == null || array.Count < 2)
                                {
                                    string rowStr = "[" + string.Join(",", array) + "]";

                                    throw new Exception($"Can't read row {rowStr} from file {path}");
                                }

                                if (schemaEmpty) // array[0] contains the state, not a column
                                {
                                    for (int i = 1; i < array.Count; i++)
                                    {
                                        schemaTable.Columns.Add($"C{i}", array[i].GetType());
                                    }

                                    schemaEmpty = false;
                                }

                                if (array.Count != (schemaTable.Columns.Count + 1))
                                {
                                    string rowStr = "[" + string.Join(",", array) + "]";

                                    throw new Exception($"Table {schemaTable.GetFullName()} with {schemaTable.Columns.Count} columns does not have the same columns count as the row read {rowStr} which have {array.Count - 1} values.");
                                }

                                // if we have some columns, we check the date time thing
                                if (schemaTable.Columns?.HasSyncColumnOfType(typeof(DateTime)) == true)
                                {
                                    for (var index = 1; index < array.Count; index++)
                                    {
                                        var column = schemaTable.Columns[index - 1];

                                        // Set the correct value in existing row for DateTime types.
                                        // They are being Deserialized as DateTimeOffsets
                                        if (column != null && column.GetDataType() == typeof(DateTime) && array[index] != null && array[index] is DateTimeOffset)
                                            array[index] = ((DateTimeOffset)array[index]).DateTime;
                                    }
                                }
                                yield return new SyncRow(schemaTable, array.ToArray());
                            }
                        }

                        yield break;
                }
            }
        }

        private static SyncTable GetSchemaTableFromReader(JsonElement value, string tableName, string schemaName)
        {
            SyncTable schemaTable;

            if (value.ValueKind != JsonValueKind.Array)
                return null;

            // get columns from array
            var includedColumns = value.Deserialize<List<JsonElement>>();

            // if we don't have columns specified, we are assuming it's the same columns
            if (includedColumns == null || includedColumns.Count == 0)
                return null;

            schemaTable = new SyncTable(tableName, schemaName);

            for (int i = 0; i < includedColumns.Count; i++)
            {
                var includedColumn = includedColumns[i];

                // column name & type from file
                var includedColumnName = includedColumn.GetProperty("n").GetString();
                var includedColumnType = SyncColumn.GetTypeFromAssemblyQualifiedName(includedColumn.GetProperty("t").GetString());
                var isPrimaryKey = includedColumn.TryGetProperty("p", out _);

                // Adding the column 
                schemaTable.Columns.Add(new SyncColumn(includedColumnName, includedColumnType));

                if (isPrimaryKey)
                    schemaTable.PrimaryKeys.Add(includedColumnName);
            }

            return schemaTable;
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    CloseFile();
                }

                disposedValue = true;
            }
        }

        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }

        public async ValueTask DisposeAsync() => await CloseFileAsync();
    }
}
