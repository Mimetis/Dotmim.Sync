using Dotmim.Sync.Enumerations;
using Microsoft.EntityFrameworkCore;
using System;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.Threading.Tasks;

namespace Dotmim.Sync.Tests.Models
{


    public partial class SyncLogsContext : DbContext
    {
        public string ConnectionString { get; }

        public SyncLogsContext(DbContextOptions<SyncLogsContext> options)
            : base(options)
        {
        }

        public SyncLogsContext(string connectionString)
        {
            this.ConnectionString = connectionString;
        }
        public SyncLogsContext(DbConnection connection)
        {
            this.Connection = connection;
        }



        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            if (!optionsBuilder.IsConfigured)
            {
                if (this.Connection != null)
                {
                    optionsBuilder.UseSqlServer(this.Connection);
                }
                else
                {
                    optionsBuilder.UseSqlServer(this.ConnectionString);
                }
            }

        }


        public bool TableExists(string tableName)
        {
            var exists = true;

            using (var command = this.Database.GetDbConnection().CreateCommand())
            {
                command.CommandText = @"SELECT count(*) FROM sys.tables AS T INNER JOIN sys.schemas AS S ON T.schema_id = S.schema_id
                         WHERE S.Name = 'dbo' AND T.Name = @tableName";

                var p = command.CreateParameter();
                p.ParameterName = "@tableName";
                p.Value = tableName;
                command.Parameters.Add(p);

                var connection = this.Database.GetDbConnection();

                bool wasOpened = true;

                if (connection.State != System.Data.ConnectionState.Open)
                {
                    connection.Open();
                    wasOpened = false;
                }

                exists = (int)command.ExecuteScalar() == 1;

                if (!wasOpened)
                    connection.Close();
            }
            return exists;
        }
        public void EnsureTablesCreated()
        {

            if (!TableExists("scope_info_sync_logs"))
            {
                this.Database.ExecuteSqlRaw($@"
                        CREATE TABLE [dbo].[scope_info_sync_logs](
	                        [SessionId] [uniqueidentifier] NOT NULL,
	                        [ClientScopeId] [uniqueidentifier] NOT NULL,
	                        [ScopeName] [nvarchar](100) NULL,
	                        [ScopeParameters] [nvarchar](MAX) NULL,
	                        [Network] [nvarchar](10) NULL,
	                        [State] [nvarchar](50) NULL,
	                        [SyncType] [nvarchar](100) NULL,
	                        [IsNew] bit NULL,
	                        [StartTime] [datetime] NULL,
	                        [EndTime] [datetime] NULL,
	                        [FromTimestamp] [bigint] NULL,
	                        [ToTimestamp] [bigint] NULL,
	                        [ChangesAppliedOnServer] [nvarchar](MAX) NULL,
	                        [ChangesAppliedOnClient] [nvarchar](MAX) NULL,
	                        [SnapshotChangesAppliedOnClient] [nvarchar](MAX) NULL,
	                        [ClientChangesSelected] [nvarchar](MAX) NULL,
	                        [ServerChangesSelected] [nvarchar](MAX) NULL,
	                        [Error] [nvarchar](MAX) NULL,
	                        [Properties] [nvarchar](MAX) NULL,
                         CONSTRAINT [PK_scope_info_sync_logs] PRIMARY KEY CLUSTERED 
                        (
	                        [SessionId] ASC
                        ))");
            }

            if (!TableExists("scope_info_sync_tables_logs"))
            {
                this.Database.ExecuteSqlRaw($@"
                        CREATE TABLE [dbo].[scope_info_sync_tables_logs](
	                        [SessionId] [uniqueidentifier] NOT NULL,
	                        [ClientScopeId] [uniqueidentifier] NOT NULL,
	                        [TableName] [nvarchar](250) NOT NULL,
	                        [ScopeName] [nvarchar](100) NULL,
	                        [ScopeParameters] [nvarchar](MAX) NULL,
	                        [State] [nvarchar](50) NULL,
	                        [Command] [nvarchar](MAX) NULL,
	                        [TableChangesSelected] [nvarchar](MAX) NULL,
	                        [TableChangesUpsertsApplied] [nvarchar](MAX) NULL,
	                        [TableChangesDeletesApplied] [nvarchar](MAX) NULL,
	                        [Properties] [nvarchar](MAX) NULL,
                         CONSTRAINT [PK_scope_info_sync_tables_logs] PRIMARY KEY CLUSTERED 
                        (
	                        [SessionId] ASC,
                            [TableName] ASC
                        ))");
            }
        }


        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<SyncLog>().HasKey(ba => new { ba.SessionId });
            modelBuilder.Entity<SyncLogTable>().HasKey(ba => new { ba.SessionId, ba.TableName });

        }

        public virtual DbSet<SyncLog> SyncLog { get; set; }
        public virtual DbSet<SyncLogTable> SyncLogTable { get; set; }
        public DbConnection Connection { get; }

        public async Task EnsureDatabasesAsync()
        {
            // Create server database with items
            await this.Database.EnsureDeletedAsync();
            await this.Database.EnsureCreatedAsync();
        }
    }


    [Table("scope_info_sync_logs")]
    public class SyncLog
    {
        public Guid SessionId { get; set; }
        public Guid ClientScopeId { get; set; }
        public string ScopeName { get; set; }
        public string ScopeParameters { get; set; }
        public string State { get; set; }
        public string Network { get; set; }
        public string Error { get; set; }

        [Column(TypeName = "nvarchar(100)")]
        public SyncType SyncType { get; set; }
        public DateTime? StartTime { get; set; }
        public DateTime? EndTime { get; set; }
        public bool IsNew { get; set; }
        public long? FromTimestamp { get; set; }
        public long? ToTimestamp { get; set; }
        public string ChangesAppliedOnServer { get; set; }
        public string ChangesAppliedOnClient { get; set; }
        public string SnapshotChangesAppliedOnClient { get; set; }
        public string ClientChangesSelected { get; set; }
        public string ServerChangesSelected { get; set; }
        public string Properties { get; set; }
    }



    [Table("scope_info_sync_tables_logs")]
    public class SyncLogTable
    {
        public Guid SessionId { get; set; }
        public Guid ClientScopeId { get; set; }
        public string TableName { get; set; }
        public string ScopeName { get; set; }
        public string ScopeParameters { get; set; }
        public string State { get; set; }
        public string Command { get; set; }
        public string TableChangesSelected { get; set; }
        public string TableChangesUpsertsApplied { get; set; }
        public string TableChangesDeletesApplied { get; set; }
        public string Properties { get; set; }
    }

}
