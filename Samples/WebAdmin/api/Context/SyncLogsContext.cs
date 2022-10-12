
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using Dotmim.Sync.Enumerations;
using Microsoft.EntityFrameworkCore;
using Newtonsoft.Json;

namespace Api.Context;


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
                optionsBuilder.UseSqlServer(this.Connection);
            else
                optionsBuilder.UseSqlServer(this.ConnectionString);
        }

    }


    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<SyncLog>().HasKey(ba => new { ba.SessionId });
        modelBuilder.Entity<SyncLogTable>().HasKey(ba => new { ba.SessionId, ba.TableName });
        modelBuilder.Entity<SyncLogTable>().HasOne(ba => ba.Parent).WithMany(b => b.Details).HasForeignKey(ba => new { ba.SessionId });

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
    public ICollection<SyncLogTable> Details { get; set; }
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

    [JsonIgnore]
    public SyncLog Parent { get; set; }

    public string State { get; set; }

    public string Command { get; set; }
    public string TableChangesSelected { get; set; }
    public string TableChangesUpsertsApplied { get; set; }
    public string TableChangesDeletesApplied { get; set; }
    public string Properties { get; set; }
}
