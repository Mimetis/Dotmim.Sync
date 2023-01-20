using Dotmim.Sync.Enumerations;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;


namespace Dotmim.Sync.Serialization
{

    /// <summary>
    /// Serialize json rows locally
    /// </summary>
    public class LocalJsonSerializer
    {

        private StreamWriter sw;
        private JsonTextWriter writer;
        private Func<SyncTable, object[], Task<string>> writingRowAsync;
        private Func<SyncTable, string, Task<object[]>> readingRowAsync;
        private readonly object writerLock = new object();


        public LocalJsonSerializer()
        {

        }

        public LocalJsonSerializer(BaseOrchestrator orchestrator, SyncContext context)
        {
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

        /// <summary>
        /// Returns if the file is opened
        /// </summary>
        public bool IsOpen { get; set; }

        /// <summary>
        /// Gets the file extension
        /// </summary>
        public string Extension => "json";

        /// <summary>
        /// Close the current file, close the writer
        /// </summary>
        /// <returns></returns>
        public void CloseFile()
        {
            // Close file
            if (!this.IsOpen)
                return;

            lock (writerLock)
            {
                this.writer.WriteEndArray();
                this.writer.WriteEndObject();
                this.writer.WriteEndArray();
                this.writer.WriteEndObject();
                this.writer.Flush();
                this.writer.Close();
                this.sw.Close();
                this.IsOpen = false;
            }

        }

        /// <summary>
        /// Open the file and write header
        /// </summary>
        public void OpenFile(string path, SyncTable schemaTable, SyncRowState state, bool append = false)
        {
            if (this.writer != null)
            {
                lock (writerLock)
                {
                    if (this.writer != null)
                    {
                        this.writer.Close();
                        this.writer = null;
                    }
                }
            }

            this.IsOpen = true;

            var fi = new FileInfo(path);

            if (!fi.Directory.Exists)
                fi.Directory.Create();

            this.sw = new StreamWriter(path, append);
            this.writer = new JsonTextWriter(sw) { CloseOutput = true, AutoCompleteOnClose = false };

            this.writer.WriteStartObject();
            this.writer.WritePropertyName("t");
            this.writer.WriteStartArray();
            this.writer.WriteStartObject();
            this.writer.WritePropertyName("n");
            this.writer.WriteValue(schemaTable.TableName);
            this.writer.WritePropertyName("s");
            this.writer.WriteValue(schemaTable.SchemaName);
            this.writer.WritePropertyName("st");
            this.writer.WriteValue((int)state);

            this.writer.WritePropertyName("c");
            this.writer.WriteStartArray();
            foreach (var c in schemaTable.Columns)
            {
                this.writer.WriteStartObject();
                this.writer.WritePropertyName("n");
                this.writer.WriteValue(c.ColumnName);
                this.writer.WritePropertyName("t");
                this.writer.WriteValue(c.DataType);
                if (schemaTable.IsPrimaryKey(c.ColumnName))
                {
                    this.writer.WritePropertyName("p");
                    this.writer.WriteValue(1);
                }
                this.writer.WriteEndObject();
            }

            this.writer.WriteEndArray();
            this.writer.WritePropertyName("r");
            this.writer.WriteStartArray();
            this.writer.WriteWhitespace(Environment.NewLine);
        }
        
        /// <summary>
        /// Append a sync row to the writer
        /// </summary>
        public async Task WriteRowToFileAsync(SyncRow row, SyncTable schemaTable)
        {
            writer.WriteStartArray();

            var innerRow = row.ToArray();

            if (this.writingRowAsync != null)
            {
                var str = await this.writingRowAsync(schemaTable, innerRow);
                writer.WriteValue(str);
            }
            else
            {
                for (var i = 0; i < innerRow.Length; i++)
                    writer.WriteValue(innerRow[i]);

            }

            writer.WriteEndArray();
            writer.WriteWhitespace(Environment.NewLine);
            writer.Flush();
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
        /// Gets the file size
        /// </summary>
        /// <returns></returns>
        public Task<long> GetCurrentFileSizeAsync()
            => this.sw != null && this.sw.BaseStream != null ?
                Task.FromResult(this.sw.BaseStream.Position / 1024L) :
                Task.FromResult(0L);



        /// <summary>
        /// Get the table contained in a serialized file
        /// </summary>
        public (SyncTable schemaTable, int rowsCount, SyncRowState state) GetSchemaTableFromFile(string path)
        {
            if (!File.Exists(path))
                return default;

            string tableName = null, schemaName = null;
            int rowsCount = 0;

            SyncTable schemaTable = null;
            SyncRowState state = SyncRowState.None;

            var serializer = new JsonSerializer { DateParseHandling = DateParseHandling.DateTimeOffset };
            using var reader = new JsonTextReader(new StreamReader(path));

            while (reader.Read())
            {
                if (reader.TokenType == JsonToken.PropertyName && reader.ValueType == typeof(string) && reader.Value != null && (string)reader.Value == "n")
                {
                    tableName = reader.ReadAsString();
                    continue;
                }

                if (reader.TokenType == JsonToken.PropertyName && reader.ValueType == typeof(string) && reader.Value != null && (string)reader.Value == "s")
                {
                    schemaName = reader.ReadAsString();
                    continue;
                }
                if (reader.TokenType == JsonToken.PropertyName && reader.ValueType == typeof(string) && reader.Value != null && (string)reader.Value == "st")
                {
                    state = (SyncRowState)reader.ReadAsInt32();
                    continue;
                }

                // Dont want to read columns if any
                if (reader.TokenType == JsonToken.PropertyName && reader.ValueType == typeof(string) && reader.Value != null && (string)reader.Value == "c")
                {
                    schemaTable = GetSchemaTableFromReader(reader, serializer, tableName, schemaName);
                    continue;
                }

                if (reader.TokenType == JsonToken.PropertyName && reader.ValueType == typeof(string) && reader.Value != null && (string)reader.Value == "r")
                {
                    // go to children
                    reader.Read();

                    bool schemaEmpty = false;

                    if (schemaTable == null)
                    {
                        schemaEmpty = true;
                        schemaTable = new SyncTable(tableName, schemaName);
                    }

                    // read all array
                    while (reader.Read() && reader.TokenType != JsonToken.EndArray)
                    {
                        if (reader.TokenType == JsonToken.StartArray)
                        {
                            var array = serializer.Deserialize<object[]>(reader);

                            if (schemaEmpty && array.Length >= 2) // array[0] contains the state, not a column
                            {
                                for (int i = 1; i <= array.Length - 1; i++)
                                {
                                    var t = array[i] == null ? typeof(object) : array[i].GetType();
                                    schemaTable.Columns.Add($"C{i}", t);
                                }

                                schemaEmpty = false;
                            }
                            rowsCount += 1;
                        }
                    }
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

            JsonSerializer serializer = new JsonSerializer { DateParseHandling = DateParseHandling.DateTimeOffset };

            using var reader = new JsonTextReader(new StreamReader(path));
            reader.DateParseHandling = DateParseHandling.DateTimeOffset;
            SyncRowState state = SyncRowState.None;

            string tableName = null, schemaName = null;

            while (reader.Read())
            {
                if (reader.TokenType == JsonToken.PropertyName && reader.ValueType == typeof(string) && reader.Value != null && (string)reader.Value == "n")
                {
                    tableName = reader.ReadAsString();
                    continue;
                }

                if (reader.TokenType == JsonToken.PropertyName && reader.ValueType == typeof(string) && reader.Value != null && (string)reader.Value == "s")
                {
                    schemaName = reader.ReadAsString();
                    continue;
                }
                if (reader.TokenType == JsonToken.PropertyName && reader.ValueType == typeof(string) && reader.Value != null && (string)reader.Value == "st")
                {
                    state = (SyncRowState)reader.ReadAsInt32();
                    continue;
                }

                if (reader.TokenType == JsonToken.PropertyName && reader.ValueType == typeof(string) && (string)reader.Value == "c")
                {
                    var tmpTable = GetSchemaTableFromReader(reader, serializer, schemaTable != null ? schemaTable.TableName : tableName, schemaTable != null ? schemaTable.SchemaName : schemaName);

                    if (tmpTable != null)
                        schemaTable = tmpTable;

                    //if (schemaTable == null && tmpTable != null)
                    //    schemaTable = tmpTable;

                    continue;
                }

                if (reader.TokenType == JsonToken.PropertyName && reader.ValueType == typeof(string) && (string)reader.Value == "r")
                {
                    reader.Read();

                    bool schemaEmpty = false;

                    if (schemaTable == null)
                    {
                        schemaEmpty = true;
                        schemaTable = new SyncTable(tableName, schemaName);
                    }


                    // read all array
                    while (reader.Read() && reader.TokenType != JsonToken.EndArray)
                    {
                        if (reader.TokenType == JsonToken.StartArray)
                        {
                            object[] array = null;

                            if (this.readingRowAsync != null)
                            {
                                var jArray = serializer.Deserialize<JArray>(reader);
                                var value = jArray[0].Value<string>();
                                array = this.readingRowAsync(schemaTable, value).GetAwaiter().GetResult();
                            }
                            else
                            {
                                array = serializer.Deserialize<object[]>(reader);
                            }

                            if (array == null || array.Length < 2)
                            {
                                string rowStr = "";

                                if (array != null)
                                {
                                    rowStr = "[";
                                    for (var o = 0; o < array.Length; o++)
                                        rowStr += array[o].ToString() + ",";
                                    rowStr += "]";
                                }

                                throw new Exception($"Can't read row {rowStr} from file {path}");
                            }

                            if (schemaEmpty) // array[0] contains the state, not a column
                            {
                                for (int i = 1; i <= array.Length - 1; i++)
                                    schemaTable.Columns.Add($"C{i}", array[i].GetType());

                                schemaEmpty = false;
                            }


                            if ((schemaTable.Columns.Count + 1) != array.Length)
                            {
                                string rowStr = "";
                                string rowsCount = "0";
                                if (array != null)
                                {
                                    rowStr = "[";
                                    for (var o = 0; o < array.Length; o++)
                                        rowStr += array[o].ToString() + ",";
                                    rowStr += "]";

                                    rowsCount = (array.Length - 1).ToString();
                                }

                                throw new Exception($"Table {schemaTable.GetFullName()} with {schemaTable.Columns.Count} columns does not have the same columns count as the row read {rowStr} which have {rowsCount} values.");
                            }

                            // if we have some columns, we check the date time thing
                            if (schemaTable.Columns != null && schemaTable.Columns.HasSyncColumnOfType(typeof(DateTime)))
                            {
                                for (var index = 1; index < array.Length; index++)
                                {
                                    var column = schemaTable.Columns[index - 1];

                                    // Set the correct value in existing row for DateTime types.
                                    // They are being Deserialized as DateTimeOffsets
                                    if (column != null && column.GetDataType() == typeof(DateTime) && array[index] != null && array[index] is DateTimeOffset)
                                        array[index] = ((DateTimeOffset)array[index]).DateTime;
                                }
                            }
                            yield return new SyncRow(schemaTable, array);
                        }
                    }

                    yield break;
                }
            }

        }

        private SyncTable GetSchemaTableFromReader(JsonTextReader reader, JsonSerializer serializer, string tableName, string schemaName)
        {
            SyncTable schemaTable;

            while (reader.Read() && reader.TokenType != JsonToken.StartArray)
                continue;

            // If we don't find any more rows, assuming we have same columns
            if (!reader.HasLineInfo())
                return null;

            // get columns from array
            var includedColumns = serializer.Deserialize<List<JObject>>(reader);

            // if we don't have columns specified, we are assuming it's the same columns
            if (includedColumns == null || includedColumns.Count == 0)
                return null;

            schemaTable = new SyncTable(tableName, schemaName);

            for (int i = 0; i < includedColumns.Count; i++)
            {
                // column name & type from file
                var includedColumnName = includedColumns[i]["n"].Value<string>();
                var includedColumnType = SyncColumn.GetTypeFromAssemblyQualifiedName(includedColumns[i]["t"].Value<string>());
                var isPrimaryKey = includedColumns[i].ContainsKey("p");

                // Adding the column 
                schemaTable.Columns.Add(new SyncColumn(includedColumnName, includedColumnType));

                if (isPrimaryKey)
                    schemaTable.PrimaryKeys.Add(includedColumnName);
            }

            return schemaTable;

        }

    }
}
