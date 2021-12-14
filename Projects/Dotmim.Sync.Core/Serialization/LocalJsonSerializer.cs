using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.Serialization
{

    public class LocalJsonSerializerFactory : ILocalSerializerFactory
    {
        public string Key => "json";
        public ILocalSerializer GetLocalSerializer() => new LocalJsonSerializer();
    }
    public class LocalJsonSerializer : ILocalSerializer
    {
        private StreamWriter sw;
        private JsonTextWriter writer;

        public string Extension => "json";

        public async Task CloseFileAsync(string path, SyncTable shemaTable)
        {
            // Close file
            this.writer.WriteEndArray();
            this.writer.WriteEndObject();
            this.writer.WriteEndArray();
            this.writer.WriteEndObject();
            this.writer.Flush();
            await this.writer.CloseAsync();
            this.sw.Close();
        }
        public async Task OpenFileAsync(string path, SyncTable shemaTable)
        {
            if (this.writer != null)
            {
                await this.writer.CloseAsync();
                this.writer = null;
            }

            this.sw = new StreamWriter(path);
            this.writer = new JsonTextWriter(sw) { CloseOutput = true };

            this.writer.WriteStartObject();
            this.writer.WritePropertyName("t");
            this.writer.WriteStartArray();
            this.writer.WriteStartObject();
            this.writer.WritePropertyName("n");
            this.writer.WriteValue(shemaTable.TableName);
            this.writer.WritePropertyName("s");
            this.writer.WriteValue(shemaTable.SchemaName);
            this.writer.WritePropertyName("r");
            this.writer.WriteStartArray();
            this.writer.WriteWhitespace(Environment.NewLine);

        }
        public Task WriteRowToFileAsync(SyncRow row, SyncTable shemaTable)
        {
            writer.WriteStartArray();
            var innerRow = row.ToArray();
            for (var i = 0; i < innerRow.Length; i++)
                writer.WriteValue(innerRow[i]);
            writer.WriteEndArray();
            writer.WriteWhitespace(Environment.NewLine);
            writer.Flush();

            return Task.CompletedTask;
        }
        public Task<long> GetCurrentFileSizeAsync()
            => this.sw != null && this.sw.BaseStream != null ?
                Task.FromResult(this.sw.BaseStream.Position / 1024L) :
                Task.FromResult(0L);

        public IEnumerable<SyncRow> ReadRowsFromFile(string path, SyncTable shemaTable)
        {
            if (!File.Exists(path))
                yield break;

            JsonSerializer serializer = new JsonSerializer();
            using var reader = new JsonTextReader(new StreamReader(path));
            while (reader.Read())
            {
                if (reader.TokenType == JsonToken.PropertyName && reader.ValueType == typeof(string) && reader.Value != null && (string)reader.Value == "t")
                {
                    while (reader.Read() && reader.TokenType != JsonToken.EndArray)
                    {
                        if (reader.TokenType == JsonToken.PropertyName && reader.ValueType == typeof(string) && reader.Value != null && (string)reader.Value == "n")
                        {
                            reader.Read();

                            while (reader.Read() && reader.TokenType != JsonToken.EndObject)
                            {
                                if (reader.TokenType == JsonToken.PropertyName && reader.ValueType == typeof(string) && reader.Value != null && (string)reader.Value == "r")
                                {
                                    // Go to children of the array
                                    reader.Read();
                                    // read all array
                                    while (reader.Read() && reader.TokenType != JsonToken.EndArray)
                                    {
                                        if (reader.TokenType == JsonToken.StartArray)
                                        {
                                            var array = serializer.Deserialize<object[]>(reader);
                                            yield return new SyncRow(shemaTable, array);
                                        }
                                    }
                                }

                            }
                        }
                    }
                }
            }

        }
    }
}
