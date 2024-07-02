using Dotmim.Sync.Enumerations;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
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
            if (!this.IsOpen)
                return;

            writerLock.Wait();

            try
            {
                if (writer != null)
                {
                    this.writer.WriteEndArray();
                    this.writer.WriteEndObject();
                    this.writer.WriteEndArray();
                    this.writer.WriteEndObject();
                    this.writer.Flush();
                    this.writer.Dispose();
                }

                this.sw?.Dispose();
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
            if (!this.IsOpen)
                return;

            await writerLock.WaitAsync();

            try
            {
                if (writer != null)
                {
                    this.writer.WriteEndArray();
                    this.writer.WriteEndObject();
                    this.writer.WriteEndArray();
                    this.writer.WriteEndObject();
                    await this.writer.FlushAsync();
                    await this.writer.DisposeAsync();
                }

                if (this.sw != null)
                {
#if NET6_0_OR_GREATER
                    await this.sw.DisposeAsync();
#else
                    this.sw.Dispose();
#endif
                }

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
        public async Task OpenFileAsync(string path, SyncTable schemaTable, SyncRowState state, bool append = false)
        {
            await ResetWriterAsync();

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
                this.writer.WritePropertyName("t");

                this.writer.WriteStartArray();
                this.writer.WriteStartObject();


                this.writer.WriteString("n", schemaTable.TableName);
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

        private async Task ResetWriterAsync()
        {
            if (this.writer == null)
            {
                return;
            }

            await writerLock.WaitAsync();

            try
            {
                if (this.writer != null)
                {
                    await this.writer.DisposeAsync();
                    this.writer = null;
                }
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
                str = await this.writingRowAsync(schemaTable, innerRow);
            else
                str = string.Empty; // This won't ever be used, but is need to compile.

            await writerLock.WaitAsync();

            try
            {
                writer.WriteStartArray();

                if (this.writingRowAsync != null)
                    writer.WriteStringValue(str);
                else
                    for (var i = 0; i < innerRow.Length; i++)
                        this.writer.WriteRawValue(serializer.Serialize(innerRow[i]));

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

            using var fileStream = File.OpenRead(path);
            using var jsonReader = new JsonReader(fileStream);

            while (jsonReader.Read())
            {
                if (jsonReader.TokenType != JsonTokenType.PropertyName)
                    continue;

                // read current value
                var propertyValue = jsonReader.GetString();

                switch (propertyValue)
                {
                    case "n":
                        tableName = jsonReader.ReadAsString();
                        break;
                    case "s":
                        schemaName = jsonReader.ReadAsString();
                        break;
                    case "st":
                        state = (SyncRowState)jsonReader.ReadAsInt32();
                        break;
                    case "c": // Dont want to read columns if any
                        schemaTable = GetSchemaTableFromReader(jsonReader, tableName, schemaName);
                        break;
                    case "r":
                        // go into first array
                        var hasToken = jsonReader.Read();

                        if (!hasToken)
                            break;

                        var depth = jsonReader.Depth;
                        // iterate objects array
                        while (jsonReader.Read() && jsonReader.Depth > depth)
                        {
                            var innerDepth = jsonReader.Depth;

                            // iterate values
                            while (jsonReader.Read() && jsonReader.Depth > innerDepth)
                                continue;

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
            using var jsonReader = new JsonReader(stream);

            var state = SyncRowState.None;

            string tableName = null, schemaName = null;

            while (jsonReader.Read())
            {
                if (jsonReader.TokenType != JsonTokenType.PropertyName)
                    continue;

                var propertyValue = jsonReader.GetString();

                switch (propertyValue as string)
                {
                    case "n":
                        tableName = jsonReader.ReadAsString();
                        break;
                    case "s":
                        schemaName = jsonReader.ReadAsString();
                        break;
                    case "st":
                        state = (SyncRowState)jsonReader.ReadAsInt16();
                        break;
                    case "c":
                        var tmpTable = GetSchemaTableFromReader(jsonReader, schemaTable?.TableName ?? tableName, schemaTable?.SchemaName ?? schemaName);

                        if (tmpTable != null)
                            schemaTable = tmpTable;

                        continue;
                    case "r":

                        bool schemaEmpty = schemaTable == null;

                        if (schemaEmpty)
                            schemaTable = new SyncTable(tableName, schemaName);

                        // go into first array
                        var hasToken = jsonReader.Read();

                        if (!hasToken)
                            break;

                        var depth = jsonReader.Depth;
                        // iterate objects array
                        while (jsonReader.Read() && jsonReader.Depth > depth)
                        {
                            var innerDepth = jsonReader.Depth;

                            // iterate values
                            int index = 0;
                            object[] values = new object[schemaTable.Columns.Count + 1];
                            StringBuilder stringBuilder = new StringBuilder();
                            bool getStringOnly = this.readingRowAsync != null;

                            while (jsonReader.Read() && jsonReader.TokenType != JsonTokenType.EndArray)
                            {
                                object value = null;
                                var columnType = index >= 1 ? schemaTable.Columns[index - 1].GetDataType() : typeof(Int16);

                                if (this.readingRowAsync != null)
                                {
                                    if (index > 0)
                                        stringBuilder.Append(",");

                                    stringBuilder.Append(jsonReader.GetString());
                                }
                                else
                                {
                                    if (jsonReader.TokenType == JsonTokenType.Null || jsonReader.TokenType == JsonTokenType.None)
                                        value = null;
                                    else if (jsonReader.TokenType == JsonTokenType.String && jsonReader.TryGetDateTimeOffset(out DateTimeOffset datetimeOffset))
                                        value = datetimeOffset;
                                    else if (jsonReader.TokenType == JsonTokenType.String)
                                        value = jsonReader.GetString();
                                    else if (jsonReader.TokenType == JsonTokenType.False || jsonReader.TokenType == JsonTokenType.True)
                                        value = jsonReader.GetBoolean();
                                    else if (jsonReader.TokenType == JsonTokenType.Number && jsonReader.TryGetInt64(out long l))
                                        value = l;
                                    else if (jsonReader.TokenType == JsonTokenType.Number)
                                        value = jsonReader.GetDouble();

                                    try
                                    {
                                        if (value != null)
                                            values[index] = SyncTypeConverter.TryConvertTo(value, columnType);
                                    }
                                    catch (Exception)
                                    {
                                        // No exception as a custom converter could be used to override type 
                                        // like a datetime converted to ticks (long)
                                    }
                                }
                                index++;
                            }

                            if (this.readingRowAsync != null)
                                values = this.readingRowAsync(schemaTable, stringBuilder.ToString()).GetAwaiter().GetResult();


                            if (values == null || values.Length < 2)
                            {
                                string rowStr = "[" + string.Join(",", values) + "]";
                                throw new Exception($"Can't read row {rowStr} from file {path}");
                            }

                            if (schemaEmpty) // array[0] contains the state, not a column
                            {
                                for (int i = 1; i < values.Length; i++)
                                    schemaTable.Columns.Add($"C{i}", values[i].GetType());

                                schemaEmpty = false;
                            }

                            if (values.Length != (schemaTable.Columns.Count + 1))
                            {
                                string rowStr = "[" + string.Join(",", values) + "]";
                                throw new Exception($"Table {schemaTable.GetFullName()} with {schemaTable.Columns.Count} columns does not have the same columns count as the row read {rowStr} which have {values.Length - 1} values.");
                            }

                            // if we have some columns, we check the date time thing
                            if (schemaTable.Columns?.HasSyncColumnOfType(typeof(DateTime)) == true)
                            {
                                for (var index2 = 1; index2 < values.Length; index2++)
                                {
                                    var column = schemaTable.Columns[index2 - 1];

                                    // Set the correct value in existing row for DateTime types.
                                    // They are being Deserialized as DateTimeOffsets
                                    if (column != null && column.GetDataType() == typeof(DateTime) && values[index2] != null && values[index2] is DateTimeOffset)
                                        values[index2] = ((DateTimeOffset)values[index2]).DateTime;
                                }
                            }
                            yield return new SyncRow(schemaTable, values);
                        }

                        yield break;
                }
            }
        }

        private static SyncTable GetSchemaTableFromReader(JsonReader jsonReader, string tableName, string schemaName)
        {
            bool hadMoreTokens;

            while ((hadMoreTokens = jsonReader.Read()) && jsonReader.TokenType != JsonTokenType.StartArray)
                continue;

            if (!hadMoreTokens)
                return null;

            // get current depth
            var schemaTable = new SyncTable(tableName, schemaName);

            while (jsonReader.Read() && jsonReader.TokenType != JsonTokenType.EndArray)
            {
                // reading an object containing a column
                if (jsonReader.TokenType == JsonTokenType.StartObject)
                {
                    string includedColumnName = null;
                    string includedColumnTypeName = null;
                    bool isPrimaryKey = false;

                    while (jsonReader.Read() && jsonReader.TokenType != JsonTokenType.EndObject)
                    {
                        var propertyValue = jsonReader.GetString();

                        switch (propertyValue)
                        {
                            case "n":
                                includedColumnName = jsonReader.ReadAsString();
                                break;
                            case "t":
                                includedColumnTypeName = jsonReader.ReadAsString();
                                break;
                            case "p":
                                isPrimaryKey = jsonReader.ReadAsInt16() == 1;
                                break;
                            default:
                                break;
                        }
                    }
                    var includedColumnType = SyncColumn.GetTypeFromAssemblyQualifiedName(includedColumnTypeName);

                    // Adding the column 
                    if (!string.IsNullOrEmpty(includedColumnName) && !string.IsNullOrEmpty(includedColumnTypeName))
                    {
                        schemaTable.Columns.Add(new SyncColumn(includedColumnName, includedColumnType));

                        if (isPrimaryKey)
                            schemaTable.PrimaryKeys.Add(includedColumnName);
                    }

                }

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
                    this.writerLock?.Dispose();
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

        public async ValueTask DisposeAsync()
        {
            await CloseFileAsync();
            this.writerLock.Dispose();
            GC.SuppressFinalize(this);
        }
    }
}
