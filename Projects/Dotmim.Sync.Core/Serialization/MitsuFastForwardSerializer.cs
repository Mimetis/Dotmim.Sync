//using Newtonsoft.Json;
//using System;
//using System.Collections.Generic;
//using System.IO;
//using System.Text;
//using System.Threading.Tasks;

//namespace Dotmim.Sync.Serialization
//{
//    public class MitsuFastForwardSerializer : IFastForwardSerializer
//    {
//        private FileStream sw;
//        private BinaryWriter writer;

//        public string Extension => "bin";

//        public Task CloseAsync(string path, SyncTable shemaTable)
//        {
//            // Close file
//            this.writer.Flush();
//            this.writer.Close();
//            this.sw.Close();
//            return Task.CompletedTask;
//        }
//        public Task OpenAsync(string path, SyncTable shemaTable)
//        {
//            if (this.writer != null)
//            {
//                this.writer.Dispose();
//                this.writer = null;
//            }

//            this.sw = new FileStream(path, FileMode.OpenOrCreate);
//            this.writer = new BinaryWriter(sw);
//            this.writer.Write(shemaTable.TableName);
//            this.writer.Write(shemaTable.SchemaName);
//            this.writer.Write(Environment.NewLine);
//            return Task.CompletedTask;

//        }
//        public Task WriteRowAsync(object[] row, SyncTable schemaTable)
//        {
//            // writing state
//            writer.Write((int)row[0]);

//            for (var i = 1; i < row.Length; i++)
//            {
//                var col = schemaTable.Columns[i - 1];
//                var colType = col.GetDataType();
//                var isNull = row[i] == null;

//                // first, write if it's a null column value
//                writer.Write(isNull);

//                if (isNull)
//                    continue;

//                if (colType == typeof(DateTime))
//                    writer.Write(((DateTime)row[i]).ToBinary());
//                else if (colType == typeof(DateTimeOffset))
//                    writer.Write(((DateTimeOffset)row[i]).Ticks);
//                else if (colType == typeof(TimeSpan))
//                    writer.Write(((TimeSpan)row[i]).Ticks);
//                else if (colType == typeof(Guid))
//                    writer.Write(((Guid)row[i]).ToByteArray());
//                else if (colType == typeof(bool))
//                    writer.Write(SyncTypeConverter.TryConvertTo<bool>(row[i]));
//                else if (colType == typeof(byte))
//                    writer.Write((byte)row[i]);
//                else if (colType == typeof(char))
//                    writer.Write((char)row[i]);
//                else if (colType == typeof(double))
//                    writer.Write((double)row[i]);
//                else if (colType == typeof(float))
//                    writer.Write((float)row[i]);
//                else if (colType == typeof(int))
//                    writer.Write((int)row[i]);
//                else if (colType == typeof(long))
//                    writer.Write((long)row[i]);
//                else if (colType == typeof(short))
//                    writer.Write((short)row[i]);
//                else if (colType == typeof(uint))
//                    writer.Write((uint)row[i]);
//                else if (colType == typeof(ulong))
//                    writer.Write((ulong)row[i]);
//                else if (colType == typeof(ushort))
//                    writer.Write((ushort)row[i]);
//                else if (colType == typeof(byte[]))
//                    writer.Write((byte[])row[i]);
//                else if (colType == typeof(decimal))
//                    writer.Write((decimal)row[i]);
//                else if (colType == typeof(string))
//                    writer.Write((string)row[i]);
//                else if (colType == typeof(sbyte))
//                    writer.Write(SyncTypeConverter.TryConvertTo<sbyte>(row[i]));
//                else if (colType == typeof(char[]))
//                    writer.Write((char[])row[i]);
//                else 
//                    writer.Write((dynamic)row[i]);

//            }
//            writer.Write(Environment.NewLine);

//            writer.Flush();

//            return Task.CompletedTask;
//        }

//        public Task<long> GetCurrentSizeAsync()
//            => this.sw != null ?
//                Task.FromResult(this.sw.Position / 1024L) :
//                Task.FromResult(0L);
//    }
//}
