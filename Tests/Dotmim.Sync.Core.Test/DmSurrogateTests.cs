using Dotmim.Sync.Data;
using Dotmim.Sync.Data.Surrogate;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace Dotmim.Sync.Core.Test
{
    public class DmSurrogateTests
    {
        DmSet set;

        public DmSurrogateTests()
        {
            set = new DmSet("ClientDmSet");

            var tbl = new DmTable("ServiceTickets");
            set.Tables.Add(tbl);
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

            #region adding rows
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

            st = tbl.NewRow();
            st["ServiceTicketID"] = Guid.NewGuid();
            st["Title"] = "Titre DE";
            st["Description"] = "Description 2";
            st["EscalationLevel"] = 3;
            st["StatusValue"] = 2;
            st["Opened"] = DateTime.Now;
            st["Closed"] = null;
            st["CustomerID"] = 1;
            tbl.Rows.Add(st);

            st = tbl.NewRow();
            st["ServiceTicketID"] = Guid.NewGuid();
            st["Title"] = "Titre FF";
            st["Description"] = "Description 2";
            st["EscalationLevel"] = 3;
            st["StatusValue"] = 4;
            st["Opened"] = DateTime.Now;
            st["Closed"] = null;
            st["CustomerID"] = 2;
            tbl.Rows.Add(st);

            st = tbl.NewRow();
            st["ServiceTicketID"] = Guid.NewGuid();
            st["Title"] = "Titre AC";
            st["Description"] = "Description 2";
            st["EscalationLevel"] = 1;
            st["StatusValue"] = 2;
            st["Opened"] = DateTime.Now;
            st["Closed"] = null;
            st["CustomerID"] = 2;
            tbl.Rows.Add(st);

            st = tbl.NewRow();
            st["ServiceTicketID"] = Guid.NewGuid();
            st["Title"] = "Titre ZDZDZ";
            st["Description"] = "Description 2";
            st["EscalationLevel"] = 0;
            st["StatusValue"] = 1;
            st["Opened"] = DateTime.Now;
            st["Closed"] = null;
            st["CustomerID"] = 2;
            tbl.Rows.Add(st);

            st = tbl.NewRow();
            st["ServiceTicketID"] = Guid.NewGuid();
            st["Title"] = "Titre VGH";
            st["Description"] = "Description 2";
            st["EscalationLevel"] = 0;
            st["StatusValue"] = 1;
            st["Opened"] = DateTime.Now;
            st["Closed"] = null;
            st["CustomerID"] = 3;
            tbl.Rows.Add(st);

            st = tbl.NewRow();
            st["ServiceTicketID"] = Guid.NewGuid();
            st["Title"] = "Titre ETTG";
            st["Description"] = "Description 2";
            st["EscalationLevel"] = 2;
            st["StatusValue"] = 1;
            st["Opened"] = DateTime.Now;
            st["Closed"] = null;
            st["CustomerID"] = 3;
            tbl.Rows.Add(st);

            st = tbl.NewRow();
            st["ServiceTicketID"] = Guid.NewGuid();
            st["Title"] = "Titre SADZD";
            st["Description"] = "Description 2";
            st["EscalationLevel"] = 1;
            st["StatusValue"] = 1;
            st["Opened"] = DateTime.Now;
            st["Closed"] = null;
            st["CustomerID"] = 3;
            tbl.Rows.Add(st);

            st = tbl.NewRow();
            st["ServiceTicketID"] = Guid.NewGuid();
            st["Title"] = "Titre AEEE";
            st["Description"] = "Description 2";
            st["EscalationLevel"] = 0;
            st["StatusValue"] = 0;
            st["Opened"] = DateTime.Now;
            st["Closed"] = null;
            st["CustomerID"] = 1;
            tbl.Rows.Add(st);

            st = tbl.NewRow();
            st["ServiceTicketID"] = Guid.NewGuid();
            st["Title"] = "Titre CZDADA";
            st["Description"] = "Description 2";
            st["EscalationLevel"] = 0;
            st["StatusValue"] = 0;
            st["Opened"] = DateTime.Now;
            st["Closed"] = null;
            st["CustomerID"] = 3;
            tbl.Rows.Add(st);

            st = tbl.NewRow();
            st["ServiceTicketID"] = Guid.NewGuid();
            st["Title"] = "Titre AFBBB";
            st["Description"] = "Description 2";
            st["EscalationLevel"] = 0;
            st["StatusValue"] = 3;
            st["Opened"] = DateTime.Now;
            st["Closed"] = null;
            st["CustomerID"] = 3;
            tbl.Rows.Add(st);

            st = tbl.NewRow();
            st["ServiceTicketID"] = Guid.NewGuid();
            st["Title"] = "Titre AZDCV";
            st["Description"] = "Description 2";
            st["EscalationLevel"] = 2;
            st["StatusValue"] = 2;
            st["Opened"] = DateTime.Now;
            st["Closed"] = null;
            st["CustomerID"] = 2;
            tbl.Rows.Add(st);

            st = tbl.NewRow();
            st["ServiceTicketID"] = Guid.NewGuid();
            st["Title"] = "Titre UYTR";
            st["Description"] = "Description 2";
            st["EscalationLevel"] = 0;
            st["StatusValue"] = 1;
            st["Opened"] = DateTime.Now;
            st["Closed"] = null;
            st["CustomerID"] = 3;
            tbl.Rows.Add(st);

            st = tbl.NewRow();
            st["ServiceTicketID"] = Guid.NewGuid();
            st["Title"] = "Titre NHJK";
            st["Description"] = "Description 2";
            st["EscalationLevel"] = 0;
            st["StatusValue"] = 1;
            st["Opened"] = DateTime.Now;
            st["Closed"] = null;
            st["CustomerID"] = 1;
            tbl.Rows.Add(st);

            st = tbl.NewRow();
            st["ServiceTicketID"] = Guid.NewGuid();
            st["Title"] = "Titre XCVBN";
            st["Description"] = "Description 2";
            st["EscalationLevel"] = 0;
            st["StatusValue"] = 1;
            st["Opened"] = DateTime.Now;
            st["Closed"] = null;
            st["CustomerID"] = 2;
            tbl.Rows.Add(st);

            st = tbl.NewRow();
            st["ServiceTicketID"] = Guid.NewGuid();
            st["Title"] = "Titre LKNB";
            st["Description"] = "Description 2";
            st["EscalationLevel"] = 3;
            st["StatusValue"] = 2;
            st["Opened"] = DateTime.Now;
            st["Closed"] = null;
            st["CustomerID"] = 3;
            tbl.Rows.Add(st);

            st = tbl.NewRow();
            st["ServiceTicketID"] = Guid.NewGuid();
            st["Title"] = "Titre ADFVB";
            st["Description"] = "Description 2";
            st["EscalationLevel"] = 0;
            st["StatusValue"] = 2;
            st["Opened"] = DateTime.Now;
            st["Closed"] = null;
            st["CustomerID"] = 1;
            tbl.Rows.Add(st);
            #endregion

            tbl.AcceptChanges();
        }

        [Fact]
        public void Create_DmSetSurrogate()
        {
            DmSetSurrogate dmSetSurrogate = new DmSetSurrogate(set);

            Assert.Equal(set.Tables.Count, dmSetSurrogate.DmTableSurrogates.Length);

            var records = dmSetSurrogate.DmTableSurrogates[0].Records;

            Assert.Equal(set.Tables[0].Rows.Count, records[0].Count);
        }


        [Fact]
        public void Create_DmTableSurrogate()
        {
            DmTable tbl = set.Tables[0];

            DmTableSurrogate dmTableSurrogate = new DmTableSurrogate(tbl);

            var records = dmTableSurrogate.Records;

            Assert.Equal(tbl.Rows.Count, records[0].Count);
        }

        [Fact]
        public void Convert_DmSetSurrogate_To_DmSet()
        {
            // Convert to DmSetSurrogate
            var surrogateDs = new DmSetSurrogate(set);
            // serialize as json string
            var stringSurrogate = JsonConvert.SerializeObject(surrogateDs);
            // deserialize as surrogate
            var surrogateDsFromJson = JsonConvert.DeserializeObject<DmSetSurrogate>(stringSurrogate);
            // Convert to DmSet
            var set2 = surrogateDs.ConvertToDmSet(set);

            Assert.NotSame(set, set2);
            Assert.Equal(set.CaseSensitive, set2.CaseSensitive);
            Assert.Equal(set.Culture, set2.Culture);
            Assert.Equal(set.DmSetName, set2.DmSetName);
            Assert.Equal(set.Tables.Count, set2.Tables.Count);
            Assert.Equal(set.Tables[0].Columns.Count, set2.Tables[0].Columns.Count);
            Assert.Equal(set.Tables[0].Rows.Count, set2.Tables[0].Rows.Count);

        }

        [Fact]
        public void Convert_DmSetSurrogate_To_ClonableDmSet()
        {
            // Convert to DmSetSurrogate
            var surrogateDs = new DmSetSurrogate(set);
            // serialize as json string
            var stringSurrogate = JsonConvert.SerializeObject(surrogateDs);
            // deserialize as surrogate
            var surrogateDsFromJson = JsonConvert.DeserializeObject<DmSetSurrogate>(stringSurrogate);
            // Convert to DmSet
            var set2 = surrogateDs.ConvertToDmSet(set);

            Assert.NotSame(set, set2);
            Assert.Equal(set.CaseSensitive, set2.CaseSensitive);
            Assert.Equal(set.Culture, set2.Culture);
            Assert.Equal(set.DmSetName, set2.DmSetName);
            Assert.Equal(set.Tables.Count, set2.Tables.Count);
            Assert.Equal(set.Tables[0].Columns.Count, set2.Tables[0].Columns.Count);
            Assert.Equal(set.Tables[0].Rows.Count, set2.Tables[0].Rows.Count);

            // Check the PKeys
            Assert.Equal(set.Tables[0].PrimaryKey.Columns.Length, set2.Tables[0].PrimaryKey.Columns.Length);
        }


        [Fact]
        public void Check_RowsSates()
        {
            var dmTable = set.Tables[0];
            dmTable.Rows[0].Delete();
            dmTable.Rows[1].SetAdded();
            dmTable.Rows[2].SetModified();

            DmSetSurrogate dss = new DmSetSurrogate(set);

            // Convert to DmSetSurrogate
            var surrogateDs = new DmSetSurrogate(set);
            // serialize as json string
            var stringSurrogate = JsonConvert.SerializeObject(surrogateDs);
            // deserialize as surrogate
            var surrogateDsFromJson = JsonConvert.DeserializeObject<DmSetSurrogate>(stringSurrogate);
            // Convert to DmSet
            var set2 = surrogateDs.ConvertToDmSet(set);

            var rows2 = set2.Tables[0].Rows;


            Assert.Equal(DmRowState.Deleted, rows2[0].RowState);
            Assert.Equal(DmRowState.Added, rows2[1].RowState);
            Assert.Equal(DmRowState.Modified, rows2[2].RowState);
            Assert.Equal(DmRowState.Unchanged, rows2[3].RowState);
            Assert.Equal(DmRowState.Unchanged, rows2[4].RowState);
            Assert.Equal(DmRowState.Unchanged, rows2[5].RowState);


        }

        [Fact]
        public void Properties()
        {
            DmSet set = new DmSet("DMSET");
            DmTable clientsTable = new DmTable("Clients");
            DmTable productsTable = new DmTable("Products");

            set.Tables.Add(clientsTable);
            set.Tables.Add(productsTable);

            DmColumn productId = new DmColumn<Int32>("Id");
            productId.AllowDBNull = false;
            productId.AutoIncrement = true;
            productsTable.Columns.Add(productId);

            DmColumn fkClientId = new DmColumn<Guid>("clientId");
            fkClientId.AllowDBNull = true;
            productsTable.Columns.Add(fkClientId);

            DmColumn productName = new DmColumn<string>("name");
            productName.AllowDBNull = true;
            productName.DbType = System.Data.DbType.StringFixedLength;
            productName.MaxLength = 150;
            productsTable.Columns.Add(productName);

            DmColumn productPrice = new DmColumn<Decimal>("price");
            productPrice.AllowDBNull = false;
            productPrice.DbType = System.Data.DbType.VarNumeric;
            productPrice.Precision = 6;
            productPrice.Scale = 2;
            productsTable.Columns.Add(productPrice);

            productsTable.PrimaryKey = new DmKey(new DmColumn[] { productId, productName, productPrice });

            DmColumn clientId = new DmColumn<Guid>("Id");
            clientId.AllowDBNull = false;
            clientsTable.Columns.Add(clientId);

            DmColumn clientName = new DmColumn<string>("Name");
            clientsTable.Columns.Add(clientName);

            clientsTable.PrimaryKey = new DmKey(clientId);

            // ForeignKey
            DmRelation fkClientRelation = new DmRelation("FK_Products_Clients", clientId, fkClientId);
            productsTable.AddForeignKey(fkClientRelation);

            var clientGuid = Guid.NewGuid();
            var drClient = clientsTable.NewRow();
            drClient["Name"] = "Pertus";
            drClient["Id"] = clientGuid;
            clientsTable.Rows.Add(drClient);

            var drProduct = productsTable.NewRow();
            drProduct["clientId"] = clientGuid;
            drProduct["name"] = "Ensemble bleu blanc rouge";
            drProduct["price"] = 12.23d ;
            productsTable.Rows.Add(drProduct);


            // Convert to DmSetSurrogate
            var surrogateDs = new DmSetSurrogate(set);
            // serialize as json string
            var stringSurrogate = JsonConvert.SerializeObject(surrogateDs);
            // deserialize as surrogate
            var surrogateDsFromJson = JsonConvert.DeserializeObject<DmSetSurrogate>(stringSurrogate);
            // Convert to DmSet
            var set2 = surrogateDs.ConvertToDmSet(set);

            // Assertions on DmSet properties
            Assert.Equal(set.DmSetName, set2.DmSetName);
            Assert.Equal(set.Culture, set2.Culture);
            Assert.Equal(set.CaseSensitive, set2.CaseSensitive);
            Assert.Equal(set.Relations.Count, set2.Relations.Count);
            Assert.Equal(set.Tables.Count, set2.Tables.Count);

            //Assertions on Table properties
            var productsTable2 = set2.Tables["Products"];
            var clientsTable2 = set2.Tables["Clients"];
            AssertIsEqual(productsTable, productsTable2);
            AssertIsEqual(clientsTable, clientsTable2);

            // Assertions on columns
            var productId2 = set2.Tables["Products"].Columns["Id"];
            AssertIsEqual(productId, productId2);
            var fkClientId2 = set2.Tables["Products"].Columns["clientId"];
            AssertIsEqual(fkClientId, fkClientId2);
            var productName2 = set2.Tables["Products"].Columns["name"];
            AssertIsEqual(productName, productName2);
            var productPrice2 = set2.Tables["Products"].Columns["price"];
            AssertIsEqual(productPrice, productPrice2);
            var clientId2 = set2.Tables["Clients"].Columns["Id"];
            AssertIsEqual(clientId, clientId2);
            var clientName2 = set2.Tables["Clients"].Columns["Name"];
            AssertIsEqual(clientName, clientName2);



        }

        private void AssertIsEqual(DmTable t1, DmTable t2)
        {
            Assert.Equal(t1.CaseSensitive, t2.CaseSensitive);
            Assert.Equal(t1.Columns.Count, t2.Columns.Count);
            Assert.Equal(t1.Culture, t2.Culture);
            Assert.Equal(t1.DmSet.DmSetName, t2.DmSet.DmSetName);
            Assert.Equal(t1.Prefix, t2.Prefix);
            Assert.Equal(t1.PrimaryKey.Columns.Length, t2.PrimaryKey.Columns.Length);
            Assert.Equal(t1.Rows.Count, t2.Rows.Count);
            Assert.Equal(t1.TableName, t2.TableName);
        }
        private void AssertIsEqual(DmColumn c, DmColumn d)
        {
            Assert.Equal(c.AllowDBNull, d.AllowDBNull);
            Assert.Equal(c.AutoIncrement, d.AutoIncrement);
            Assert.Equal(c.ColumnName, d.ColumnName);
            Assert.Equal(c.DataType, d.DataType);
            Assert.Equal(c.DbType, d.DbType);
            Assert.Equal(c.DefaultValue, d.DefaultValue);
            Assert.Equal(c.IsValueType, d.IsValueType);
            Assert.Equal(c.MaxLength, d.MaxLength);
            Assert.Equal(c.Ordinal, d.Ordinal);
            Assert.Equal(c.Precision, d.Precision);
            Assert.Equal(c.PrecisionSpecified, d.PrecisionSpecified);
            Assert.Equal(c.ReadOnly, d.ReadOnly);
            Assert.Equal(c.Scale, d.Scale);
            Assert.Equal(c.ScaleSpecified, d.ScaleSpecified);
            Assert.Equal(c.Unique, d.Unique);
            Assert.Equal(c.Table.TableName, d.Table.TableName);

        }


    }
}
