using Dotmim.Sync.Serialization;
using Dotmim.Sync.Data;
using Dotmim.Sync.Data.Surrogate;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Xunit;
using Newtonsoft.Json;

namespace Dotmim.Sync.Tests.StandAlone
{
    public class BinaryConvertTests
    {
        public class Client
        {
            public int Id { get; set; }
            public Guid ClientId { get; set; }
            public String FirstName { get; set; }
            public String LastName { get; set; }
            public DateTime Birthday { get; set; }
            public bool IsAvailable { get; set; }
            public double Money { get; set; }
        }

        public class NullableClient
        {
            public int? Id { get; set; }
            public Guid? ClientId { get; set; }
            public String FirstName { get; set; }
            public String LastName { get; set; }
            public DateTime? Birthday { get; set; }
            public bool? IsAvailable { get; set; }
            public double? Money { get; set; }
        }

        [Fact]
        public void Serialize_Object_With_Bin()
        {

            BaseConverter<Client> serializer = new DmBinaryConverter<Client>();

            Guid guid = Guid.NewGuid();
            var client = new Client
            {
                Id = 1,
                ClientId = guid,
                Birthday = new DateTime(1976, 10, 23),
                FirstName = "sébastien",
                LastName = "Pertus"
            };

            var b = serializer.Serialize(client);

            using (var ms = new MemoryStream(b))
            {
                var client2 = serializer.Deserialize(ms);

                Assert.NotSame(client, client2);
                Assert.Equal(1, client2.Id);
                Assert.Equal(guid, client2.ClientId);
                Assert.Equal("sébastien", client2.FirstName);

            }

            var serializer2 = new DmBinaryConverter<List<Client>>();

            // Test List<T>
            var lst = new List<Client>();
            lst.Add(client);
            lst.Add(client);
            lst.Add(client);

            var b2 = serializer2.Serialize(lst);
            using (var ms = new MemoryStream(b2))
            {
                var lst2 = serializer2.Deserialize(ms);
                Assert.NotSame(lst, lst2);
                Assert.Equal(3, lst2.Count);
            }

            var serializer3 = new DmBinaryConverter<NullableClient>();

            // test nullable
            var client3 = new NullableClient
            {
                Id = null,
                ClientId = null,
                Birthday = null,
                FirstName = "~#'\"é{[",
                LastName = null,
                Money = null
            };

            var b3 = serializer3.Serialize(client3);
            using (var ms = new MemoryStream(b3))
            {
                var client4 = serializer3.Deserialize(ms);

                Assert.NotSame(client3, client4);
                Assert.Null(client4.Id);
                Assert.Null(client4.ClientId);
                Assert.Null(client4.Birthday);
                Assert.Null(client4.LastName);
                Assert.Null(client4.Money);
                Assert.Equal("~#'\"é{[", client4.FirstName);
            }

            var serializer4 = new DmBinaryConverter<DmSetSurrogate>();


            // Test on datatable
            var ds = new DmSet("Fabrikam");
            var tbl = new DmTable("ServiceTickets");
            var id = new DmColumn<Guid>("ServiceTicketID");
            tbl.Columns.Add(id);
            var key = new DmKey(new DmColumn[] { id });
            tbl.PrimaryKey = key;
            tbl.Columns.Add(new DmColumn<string>("Title"));
            tbl.Columns.Add(new DmColumn<string>("Description"));
            tbl.Columns.Add(new DmColumn<int>("StatusValue"));
            tbl.Columns.Add(new DmColumn<int>("EscalationLevel"));
            tbl.Columns.Add(new DmColumn<DateTime>("Opened"));
            tbl.Columns.Add(new DmColumn<DateTime>("Closed"));
            tbl.Columns.Add(new DmColumn<int>("CustomerID"));

            var st = tbl.NewRow();
            st["ServiceTicketID"] = Guid.NewGuid();
            st["Title"] = "Titre AER";
            st["Description"] = "Description 2";
            st["EscalationLevel"] = 1;
            st["StatusValue"] = 2;
            st["Opened"] = DateTime.Now;
            st["Closed"] = null;
            st["CustomerID"] = 1;
            tbl.Rows.Add(st);
            ds.Tables.Add(tbl);

            var dsSurrogate = new DmSetSurrogate(ds);
              var b4 =  serializer4.Serialize(dsSurrogate);

            using (var ms = new MemoryStream(b4))
            {
                var dsSurrogate2 = serializer4.Deserialize(ms);

                var ds2 = dsSurrogate2.ConvertToDmSet(ds);

                Assert.NotSame(ds, ds2);
                Assert.Equal(ds.CaseSensitive, ds2.CaseSensitive);
                Assert.Equal(ds.Culture, ds2.Culture);
                Assert.Equal(ds.DmSetName, ds2.DmSetName);
                Assert.Equal(ds.Tables.Count, ds2.Tables.Count);
                Assert.Equal(ds.Tables[0].Columns.Count, ds2.Tables[0].Columns.Count);
                Assert.Equal(ds.Tables[0].Rows.Count, ds2.Tables[0].Rows.Count);
                Assert.Equal(ds.Tables[0].PrimaryKey.Columns.Length, ds2.Tables[0].PrimaryKey.Columns.Length);
            }
        }


        [Fact]
        public void Serialize_Object_With_Json()
        {

            BaseConverter<Client> serializer = new Serialization.JsonConverter<Client>();

            var guid = Guid.NewGuid();
            var client = new Client
            {
                Id = 1,
                ClientId = guid,
                Birthday = new DateTime(1976, 10, 23),
                FirstName = "sébastien",
                LastName = "Pertus"
            };

            var b = serializer.Serialize(client);

            using (var ms = new MemoryStream(b))
            {
                var client2 = serializer.Deserialize(ms);

                Assert.NotSame(client, client2);
                Assert.Equal(1, client2.Id);
                Assert.Equal(guid, client2.ClientId);
                Assert.Equal("sébastien", client2.FirstName);

            }

            var serializer2 = new DmBinaryConverter<List<Client>>();

            // Test List<T>
            var lst = new List<Client>();
            lst.Add(client);
            lst.Add(client);
            lst.Add(client);

            var b2 = serializer2.Serialize(lst);
            using (var ms = new MemoryStream(b2))
            {
                var lst2 = serializer2.Deserialize(ms);
                Assert.NotSame(lst, lst2);
                Assert.Equal(3, lst2.Count);
            }

            var serializer3 = new DmBinaryConverter<NullableClient>();

            // test nullable
            var client3 = new NullableClient
            {
                Id = null,
                ClientId = null,
                Birthday = null,
                FirstName = "~#'\"é{[",
                LastName = null,
                Money = null
            };

            var b3 = serializer3.Serialize(client3);
            using (var ms = new MemoryStream(b3))
            {
                var client4 = serializer3.Deserialize(ms);

                Assert.NotSame(client3, client4);
                Assert.Null(client4.Id);
                Assert.Null(client4.ClientId);
                Assert.Null(client4.Birthday);
                Assert.Null(client4.LastName);
                Assert.Null(client4.Money);
                Assert.Equal("~#'\"é{[", client4.FirstName);
            }

            var serializer4 = new DmBinaryConverter<DmSetSurrogate>();


            // Test on datatable
            var ds = new DmSet("Fabrikam");
            var tbl = new DmTable("ServiceTickets");
            var id = new DmColumn<Guid>("ServiceTicketID");
            tbl.Columns.Add(id);
            var key = new DmKey(new DmColumn[] { id });
            tbl.PrimaryKey = key;
            tbl.Columns.Add(new DmColumn<string>("Title"));
            tbl.Columns.Add(new DmColumn<string>("Description"));
            tbl.Columns.Add(new DmColumn<int>("StatusValue"));
            tbl.Columns.Add(new DmColumn<int>("EscalationLevel"));
            tbl.Columns.Add(new DmColumn<DateTime>("Opened"));
            tbl.Columns.Add(new DmColumn<DateTime>("Closed"));
            tbl.Columns.Add(new DmColumn<int>("CustomerID"));

            var st = tbl.NewRow();
            st["ServiceTicketID"] = Guid.NewGuid();
            st["Title"] = "Titre AER";
            st["Description"] = "Description 2";
            st["EscalationLevel"] = 1;
            st["StatusValue"] = 2;
            st["Opened"] = DateTime.Now;
            st["Closed"] = null;
            st["CustomerID"] = 1;
            tbl.Rows.Add(st);
            ds.Tables.Add(tbl);

            var dsSurrogate = new DmSetSurrogate(ds);
            var b4 = serializer4.Serialize(dsSurrogate);

            using (var ms = new MemoryStream(b4))
            {
                var dsSurrogate2 = serializer4.Deserialize(ms);

                var ds2 = dsSurrogate2.ConvertToDmSet(ds);

                Assert.NotSame(ds, ds2);
                Assert.Equal(ds.CaseSensitive, ds2.CaseSensitive);
                Assert.Equal(ds.Culture, ds2.Culture);
                Assert.Equal(ds.DmSetName, ds2.DmSetName);
                Assert.Equal(ds.Tables.Count, ds2.Tables.Count);
                Assert.Equal(ds.Tables[0].Columns.Count, ds2.Tables[0].Columns.Count);
                Assert.Equal(ds.Tables[0].Rows.Count, ds2.Tables[0].Rows.Count);
                Assert.Equal(ds.Tables[0].PrimaryKey.Columns.Length, ds2.Tables[0].PrimaryKey.Columns.Length);
            }
        }

    }
}
