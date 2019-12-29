using Dotmim.Sync.Enumerations;
using MessagePack;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using Xunit;

namespace Dotmim.Sync.Tests.StandAlone
{
    public class SerializerTests
    {

        [Fact]
        public void Test_Schema_MessagePackSerializer()
        {
            var inSchema = CreateSchema();
            byte[] bin = null;
            SyncSet outSchema;
            MessagePackSerializer.DefaultOptions.WithResolver(MessagePack.Resolvers.ContractlessStandardResolver.Instance);

            using (var ms = new MemoryStream())
            {
                MessagePackSerializer.Serialize(ms, inSchema);
                bin = ms.ToArray();
            }

            using (var fs = new FileStream("MsgPach_Schema.json", FileMode.Create))
            {
                fs.Write(bin, 0, bin.Length);
            }

            using (var ms = new MemoryStream(bin))
            {
                outSchema = MessagePackSerializer.Deserialize<SyncSet>(ms);
            }

            Assertions(outSchema);
        }

        [Fact]
        public void Test_Schema_BinarryFormatter()
        {
            var inSchema = CreateSchema();
            byte[] bin = null;
            SyncSet outSchema;

            var schemaSerializer = new BinaryFormatter
            {
                TypeFormat = FormatterTypeStyle.TypesAlways
            };
            using (var ms = new MemoryStream())
            {
                schemaSerializer.Serialize(ms, inSchema);
                bin = ms.ToArray();
            }

            using (var fs = new FileStream("Binary_Schema.bin", FileMode.Create))
            {
                fs.Write(bin, 0, bin.Length);
            }

            using (var ms = new MemoryStream(bin))
            {
                outSchema = schemaSerializer.Deserialize(ms) as SyncSet;
            }

            Assertions(outSchema);
        }


        [Fact]
        public void Test_Schema_DataContractSerializer()
        {
            var schemaSerializer = new DataContractSerializer(typeof(SyncSet));
            var inSchema = CreateSchema();
            byte[] bin = null;
            SyncSet outSchema;

            using (var ms = new MemoryStream())
            {
                schemaSerializer.WriteObject(ms, inSchema);
                bin = ms.ToArray();
            }

            using (var fs = new FileStream("Datacontract_Schema.xml", FileMode.Create))
            {
                fs.Write(bin, 0, bin.Length);
            }

            using (var ms = new MemoryStream(bin))
            {
                outSchema = schemaSerializer.ReadObject(ms) as SyncSet;
            }

            Assertions(outSchema);
        }


        [Fact]
        public void Test_Schema_JsonSerializer()
        {
            var inSchema = CreateSchema();

            var serializer = new JsonSerializer();
            byte[] bin = null;
            SyncSet outSchema;

            using (var ms = new MemoryStream())
            {
                using (var writer = new StreamWriter(ms))
                {
                    using (var jsonWriter = new JsonTextWriter(writer))
                    {
                        serializer.Serialize(jsonWriter, inSchema);
                    }
                }
                bin = ms.ToArray();
            }

            // for readiness
            using (var fs = new FileStream("Json_schema.json", FileMode.Create))
            {
                fs.Write(bin, 0, bin.Length);
            }

            Assert.NotNull(bin);

            using (var ms = new MemoryStream(bin))
            {
                using (var sr = new StreamReader(ms))
                {
                    using (var reader = new JsonTextReader(sr))
                    {
                        outSchema = serializer.Deserialize<SyncSet>(reader);
                    }
                }
            }

            Assertions(outSchema);
        }


        private void Assertions(SyncSet outSchema)
        {
            // Call the EnsureSchema to propagate schema to all entities
            outSchema.EnsureSchema();

            Assert.NotNull(outSchema);
            Assert.Equal("AdventureWorks", outSchema.DataSourceName);
            Assert.False(outSchema.CaseSensitive);
            Assert.Equal("spp", outSchema.StoredProceduresPrefix);
            Assert.Equal("sps", outSchema.StoredProceduresSuffix);
            Assert.Equal("ttp", outSchema.TrackingTablesPrefix);
            Assert.Equal("tts", outSchema.TrackingTablesSuffix);
            Assert.Equal("tp", outSchema.TriggersPrefix);
            Assert.Equal("ts", outSchema.TriggersSuffix);
            Assert.Equal(SyncOptions.DefaultScopeName, outSchema.ScopeName);
            Assert.NotEmpty(outSchema.Tables);
            Assert.NotEmpty(outSchema.Filters);
            Assert.NotEmpty(outSchema.Relations);
            Assert.Equal(2, outSchema.Tables.Count);
            Assert.Single(outSchema.Relations);
            Assert.Single(outSchema.Filters);

            var tbl1 = outSchema.Tables[0];
            Assert.Equal("ServiceTickets", tbl1.TableName);
            Assert.Null(tbl1.SchemaName);
            Assert.Equal(SyncDirection.Bidirectional, tbl1.SyncDirection);
            Assert.NotNull(tbl1.Schema);
            Assert.Equal(outSchema, tbl1.Schema);
            Assert.Equal(8, tbl1.Columns.Count);
            Assert.Equal(tbl1, tbl1.Columns.Table);
            Assert.NotEmpty(tbl1.Columns.InnerCollection);

            var col = tbl1.Columns[0];
            Assert.Equal("ServiceTicketID", col.ColumnName);
            Assert.True(col.AllowDBNull);
            Assert.Equal(10, col.AutoIncrementSeed);
            Assert.Equal(1, col.AutoIncrementStep);
            Assert.True(col.IsAutoIncrement);
            Assert.False(col.IsCompute);
            Assert.True(col.IsReadOnly);
            Assert.Equal(0, col.Ordinal);
            Assert.Equal(tbl1, col.Table);

            // check orders on others columns
            Assert.Equal(7, tbl1.Columns["CustomerID"].Ordinal);

            var tbl2 = outSchema.Tables[1];
            Assert.Equal("Product", tbl2.TableName);
            Assert.Equal("SalesLT", tbl2.SchemaName);
            Assert.Equal(SyncDirection.UploadOnly, tbl2.SyncDirection);
            Assert.NotNull(tbl2.Schema);
            Assert.Equal(outSchema, tbl2.Schema);
            Assert.Equal(2, tbl2.Columns.Count);
            Assert.Equal(tbl2, tbl2.Columns.Table);
            Assert.NotEmpty(tbl2.Columns.InnerCollection);
            Assert.Single(tbl2.PrimaryKeys);
            Assert.Equal("Id", tbl2.PrimaryKeys[0]);

            var col2 = tbl2.Columns[0];
            Assert.Equal(tbl2, col2.Table);

            // Check Filters
            Assert.NotEmpty(outSchema.Filters);
            var sf = outSchema.Filters[0];
            Assert.Equal("Product", sf.TableName);
            Assert.Equal("Title", sf.ColumnName);
            Assert.Equal("SalesLT", sf.SchemaName);
            Assert.Equal(outSchema, sf.Schema);
            Assert.Equal((int)DbType.String, sf.ColumnType);

            // Check Relations
            Assert.NotEmpty(outSchema.Relations);
            var rel = outSchema.Relations[0];
            Assert.Equal("AdventureWorks_Product_ServiceTickets", rel.RelationName);
            Assert.NotEmpty(rel.ParentKeys);
            Assert.NotEmpty(rel.ChildKeys);
            var c = rel.ChildKeys.ToList()[0];
            Assert.Equal("ProductId", c.ColumnName);
            Assert.Equal("ServiceTickets", c.TableName);
            Assert.Null(c.SchemaName);
            var p = rel.ParentKeys.ToList()[0];
            Assert.Equal("ProductId", p.ColumnName);
            Assert.Equal("Product", p.TableName);
            Assert.Equal("SalesLT", p.SchemaName);

        }

        private static SyncSet CreateSchema()
        {
            var set = new SyncSet() { DataSourceName = "AdventureWorks" };
            set.CaseSensitive = false;
            set.StoredProceduresPrefix = "spp";
            set.StoredProceduresSuffix = "sps";
            set.TrackingTablesPrefix = "ttp";
            set.TrackingTablesSuffix = "tts";
            set.TriggersPrefix = "tp";
            set.TriggersSuffix = "ts";

            var tbl = new SyncTable("ServiceTickets", null);
            tbl.OriginalProvider = "SqlServerProvider";
            tbl.SyncDirection = Enumerations.SyncDirection.Bidirectional;
            
            set.Tables.Add(tbl);

            var c = SyncColumn.Create<int>("ServiceTicketID");
            c.DbType = 8;
            c.AllowDBNull = true;
            c.IsAutoIncrement = true;
            c.AutoIncrementStep = 1;
            c.AutoIncrementSeed = 10;
            c.IsCompute = false;
            c.IsReadOnly = true;
            tbl.Columns.Add(c);

            tbl.Columns.Add(SyncColumn.Create<string>("Title"));
            tbl.Columns.Add(SyncColumn.Create<string>("Description"));
            tbl.Columns.Add(SyncColumn.Create<int>("StatusValue"));
            tbl.Columns.Add(SyncColumn.Create<int>("EscalationLevel"));
            tbl.Columns.Add(SyncColumn.Create<DateTime>("Opened"));
            tbl.Columns.Add(SyncColumn.Create<DateTime>("Closed"));
            tbl.Columns.Add(SyncColumn.Create<int>("CustomerID"));

            tbl.PrimaryKeys.Add("ServiceTicketID");

            // Add Second tables
            var tbl2 = new SyncTable("Product", "SalesLT");
            tbl2.SyncDirection = Enumerations.SyncDirection.UploadOnly;

            tbl2.Columns.Add(SyncColumn.Create<int>("Id"));
            tbl2.Columns.Add(SyncColumn.Create<string>("Title"));
            tbl2.PrimaryKeys.Add("Id");

            set.Tables.Add(tbl2);


            // Add Filters
            var sf = new SyncFilter("Title", "Product", "SalesLT", (int)DbType.String);
            set.Filters.Add(sf);

            // Add Relations
            var parentKeys = new[] { new SyncColumnIdentifier("ProductId", "Product", "SalesLT") };
            var childKeys = new[] { new SyncColumnIdentifier("ProductId", "ServiceTickets") };
            var rel = new SyncRelation("AdventureWorks_Product_ServiceTickets", parentKeys, childKeys);

            set.Relations.Add(rel);

            return set;
        }

     
    }
}
