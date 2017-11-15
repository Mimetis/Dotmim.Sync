using Dotmim.Sync.Builders;
using Dotmim.Sync.Data;
using Dotmim.Sync.SqlServer;
using System;
using System.Data.SqlClient;
using System.Linq;
using Xunit;

namespace Dotmim.Sync.Test
{
    public class SqlBuilderTableTest : IDisposable
    {

        string connectionString = "Data Source=(localdb)\\MSSQLLocalDB; Initial Catalog={0}; Integrated Security=true;";
        string databaseName;
        string masterConnectionString;
        string clientConnectionString;
        DmSet set;

        public SqlBuilderTableTest()
        {
            databaseName = "TESTDB" + DateTime.Now.ToString("yyyyMMddHHmm");
            masterConnectionString = String.Format(connectionString, "master");
            clientConnectionString = String.Format(connectionString, databaseName);

            // Create database
            using (SqlConnection connection = new SqlConnection(masterConnectionString))
            {
                using (var cmd = new SqlCommand())
                {
                    cmd.Connection = connection;
                    cmd.CommandText = $"if exists(select * from sys.databases where name = '{databaseName}') " +
                                      $"Begin " +
                                      $"ALTER DATABASE {databaseName} SET SINGLE_USER WITH ROLLBACK IMMEDIATE " +
                                      $"Drop Database {databaseName} " +
                                      $"End " +
                                      $"Create Database {databaseName} ";

                    connection.Open();
                    cmd.ExecuteNonQuery();
                    connection.Close();
                }
            }

            // Generate the DmSet schema
            set = new DmSet();
            DmTable clientsTable = new DmTable("Clients");
            DmTable productsTable = new DmTable("Products");

            // orders matters !!
            set.Tables.Add(clientsTable);
            set.Tables.Add(productsTable);

            DmColumn id = new DmColumn<Int32>("Id");
            id.AllowDBNull = false;
            id.AutoIncrement = true;
            productsTable.Columns.Add(id);

            DmColumn fkClientId = new DmColumn<Guid>("clientId");
            fkClientId.AllowDBNull = true;
            productsTable.Columns.Add(fkClientId);

            DmColumn name = new DmColumn<string>("name");
            name.AllowDBNull = false;
            name.DbType = System.Data.DbType.StringFixedLength;
            name.MaxLength = 150;
            productsTable.Columns.Add(name);

            DmColumn salary = new DmColumn<Decimal>("salary");
            salary.AllowDBNull = false;
            salary.DbType = System.Data.DbType.VarNumeric;
            salary.Precision = 6;
            salary.Scale = 2;
            productsTable.Columns.Add(salary);

            productsTable.PrimaryKey = new DmKey(new DmColumn[] { id, name, salary });

            DmColumn clientId = new DmColumn<Guid>("Id");
            clientId.AllowDBNull = false;
            clientsTable.Columns.Add(clientId);

            DmColumn clientName = new DmColumn<string>("Name");
            clientsTable.Columns.Add(clientName);

            clientsTable.PrimaryKey = new DmKey(clientId);

            // ForeignKey
            DmRelation fkClientRelation = new DmRelation("FK_Products_Clients", clientId, fkClientId);
            productsTable.AddForeignKey(fkClientRelation);
        }

        [Fact]
        public void BuilderTable_CreateTable()
        {
            var provider = new SqlSyncProvider(clientConnectionString);

            using (var connection = provider.CreateConnection())
            {
                var options = DbBuilderOption.CreateOrUseExistingSchema;
                var builder = provider.GetDatabaseBuilder(set.Tables["Products"], options);
                var tableBuilder = builder.CreateTableBuilder(connection);

                connection.Open();

                // Check if we need to create the tables
                if (tableBuilder.NeedToCreateTable(options))
                {
                    tableBuilder.CreateTable();
                    tableBuilder.CreatePrimaryKey();
                }

                connection.Close();
            }

            // Check result
            using (var connection = new SqlConnection(clientConnectionString))
            {

                var table = set.Tables["Products"];

                // Check Columns
                var commandColumn = $"Select col.name as name, col.column_id, typ.name as [type], col.max_length, col.precision, col.scale, col.is_nullable, col.is_identity from sys.columns as col " +
                                $"Inner join sys.tables as tbl on tbl.object_id = col.object_id " +
                                $"Inner Join sys.systypes typ on typ.xusertype = col.system_type_id " +
                                $"Where tbl.name = @tableName " +
                                $"Order by col.column_id";

                ObjectNameParser tableNameParser = new ObjectNameParser(table.TableName);
                DmTable dmTable = new DmTable(tableNameParser.UnquotedStringWithUnderScore);
                using (SqlCommand sqlCommand = new SqlCommand(commandColumn, connection))
                {
                    sqlCommand.Parameters.AddWithValue("@tableName", tableNameParser.ObjectName);

                    connection.Open();
                    using (var reader = sqlCommand.ExecuteReader())
                    {
                        dmTable.Fill(reader);
                    }
                    connection.Close();
                }

                // Check columns number
                Assert.Equal(4, dmTable.Rows.Count);
                var rows = dmTable.Rows.OrderBy(r => (int)r["column_id"]).ToList();

                var c = rows[0];
                var name = c["name"].ToString();
                var ordinal = (int)c["column_id"];
                var typeString = c["type"].ToString();
                var maxLength = (Int16)c["max_length"];
                var precision = (byte)c["precision"];
                var scale = (byte)c["scale"];
                var isNullable = (bool)c["is_nullable"];
                var isIdentity = (bool)c["is_identity"];

                Assert.Equal("Id", name);
                Assert.False(isNullable);
                Assert.True(isIdentity);

                c = rows[1];
                name = c["name"].ToString();
                ordinal = (int)c["column_id"];
                typeString = c["type"].ToString();
                maxLength = (Int16)c["max_length"];
                precision = (byte)c["precision"];
                scale = (byte)c["scale"];
                isNullable = (bool)c["is_nullable"];
                isIdentity = (bool)c["is_identity"];

                Assert.Equal("clientId", name);
                Assert.True(isNullable);
                Assert.False(isIdentity);

                c = rows[2];
                name = c["name"].ToString();
                ordinal = (int)c["column_id"];
                typeString = c["type"].ToString();
                maxLength = (Int16)c["max_length"];
                precision = (byte)c["precision"];
                scale = (byte)c["scale"];
                isNullable = (bool)c["is_nullable"];
                isIdentity = (bool)c["is_identity"];

                Assert.Equal("name", name);
                Assert.False(isNullable);
                Assert.Equal(300, maxLength);
                Assert.Equal("nvarchar", typeString);

                c = rows[3];
                name = c["name"].ToString();
                ordinal = (int)c["column_id"];
                typeString = c["type"].ToString();
                maxLength = (Int16)c["max_length"];
                precision = (byte)c["precision"];
                scale = (byte)c["scale"];
                isNullable = (bool)c["is_nullable"];
                isIdentity = (bool)c["is_identity"];

                Assert.Equal("salary", name);
                Assert.False(isNullable);
                Assert.Equal(6, precision);
                Assert.Equal(2, scale);


            }

        }
        [Fact]
        public void BuilderTable_CreateTrackingTable()
        {
            var provider = new SqlSyncProvider(clientConnectionString);

            using (var connection = provider.CreateConnection())
            {
                var options = DbBuilderOption.CreateOrUseExistingSchema;
                var builder = provider.GetDatabaseBuilder(set.Tables["Products"], options);

                var tableBuilder = builder.CreateTrackingTableBuilder(connection);

                connection.Open();

                // Check if we need to create the tables
                tableBuilder.CreateTable();
                tableBuilder.CreatePk();
                tableBuilder.CreateIndex();

                connection.Close();
            }

            // Check result
            using (var connection = new SqlConnection(clientConnectionString))
            {

                var table = set.Tables["Products"];

                // Check Columns
                var commandColumn = $"Select col.name as name, col.column_id, typ.name as [type], col.max_length, col.precision, col.scale, col.is_nullable, col.is_identity from sys.columns as col " +
                                $"Inner join sys.tables as tbl on tbl.object_id = col.object_id " +
                                $"Inner Join sys.systypes typ on typ.xusertype = col.system_type_id " +
                                $"Where tbl.name = @tableName " +
                                $"Order by col.column_id";

                ObjectNameParser tableNameParser = new ObjectNameParser(table.TableName + "_tracking");
                DmTable dmTable = new DmTable(tableNameParser.UnquotedStringWithUnderScore);
                using (SqlCommand sqlCommand = new SqlCommand(commandColumn, connection))
                {
                    sqlCommand.Parameters.AddWithValue("@tableName", tableNameParser.ObjectName);

                    connection.Open();
                    using (var reader = sqlCommand.ExecuteReader())
                    {
                        dmTable.Fill(reader);
                    }
                    connection.Close();
                }

                // Check columns number
                Assert.Equal(10, dmTable.Rows.Count);
                var rows = dmTable.Rows.OrderBy(r => (int)r["column_id"]).ToList();

                var c = rows[0];
                var name = c["name"].ToString();
                Assert.Equal("Id", name);
                c = rows[1];
                name = c["name"].ToString();
                Assert.Equal("name", name);
                c = rows[2];
                name = c["name"].ToString();
                Assert.Equal("salary", name);
                c = rows[3];
                name = c["name"].ToString();
                Assert.Equal("create_scope_id", name);
                c = rows[4];
                name = c["name"].ToString();
                Assert.Equal("update_scope_id", name);
                c = rows[5];
                name = c["name"].ToString();
                Assert.Equal("create_timestamp", name);
                c = rows[6];
                name = c["name"].ToString();
                Assert.Equal("update_timestamp", name);
                c = rows[7];
                name = c["name"].ToString();
                Assert.Equal("timestamp", name);
                c = rows[8];
                name = c["name"].ToString();
                Assert.Equal("sync_row_is_tombstone", name);
                c = rows[9];
                name = c["name"].ToString();
                Assert.Equal("last_change_datetime", name);
                

            }

        }

        public void Dispose()
        {
            // Delete database
            using (SqlConnection connection = new SqlConnection(masterConnectionString))
            {
                using (var cmd = new SqlCommand())
                {
                    cmd.Connection = connection;
                    cmd.CommandText = $"if exists(select * from sys.databases where name = '{databaseName}') " +
                                      $"Begin " +
                                      $"ALTER DATABASE {databaseName} SET SINGLE_USER WITH ROLLBACK IMMEDIATE " +
                                      $"Drop Database {databaseName} " +
                                      $"End ";

                    connection.Open();
                    cmd.ExecuteNonQuery();
                    connection.Close();
                }
            }
        }
    }
}
