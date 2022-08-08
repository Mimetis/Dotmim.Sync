
using System.ComponentModel.DataAnnotations.Schema;
using Dotmim.Sync.Enumerations;
using Microsoft.EntityFrameworkCore;
using Newtonsoft.Json;

namespace Api.Context;

public partial class SyncLogsContext : DbContext
{
  public SyncLogsContext(DbContextOptions<SyncLogsContext> options)
     : base(options)
  {
  }

  protected override void OnModelCreating(ModelBuilder modelBuilder)
  {
    modelBuilder.Entity<SyncLog>().HasKey(ba => new { ba.SessionId, ba.ClientScopeId });
    modelBuilder.Entity<SyncLogTable>().HasKey(ba => new { ba.SessionId, ba.ClientScopeId, ba.TableName });
    modelBuilder.Entity<SyncLogTable>().HasOne(ba => ba.Parent).WithMany(b => b.Details).HasForeignKey(ba => new { ba.SessionId, ba.ClientScopeId });
  }

  public virtual DbSet<SyncLog> SyncLog { get; set; }
  public virtual DbSet<SyncLogTable> SyncLogTable { get; set; }

}


[Table("scope_info_sync_logs")]
public class SyncLog
{
  public ICollection<SyncLogTable> Details { get; set; }
  public Guid SessionId { get; set; }
  public Guid ClientScopeId { get; set; }
  public string ScopeName { get; set; }

  [Column(TypeName = "nvarchar(100)")]
  public SyncType SyncType { get; set; }
  public DateTime StartTime { get; set; }
  public bool IsNew { get; set; }
  public long? FromTimestamp { get; set; }
  public long? ToTimestamp { get; set; }
  public long? TotalChangesSelected { get; set; }
  public long? TotalChangesSelectedUpdates { get; set; }
  public long? TotalChangesSelectedDeletes { get; set; }
  public long? TotalChangesApplied { get; set; }
  public long? TotalChangesAppliedUpdates { get; set; }
  public long? TotalChangesAppliedDeletes { get; set; }
  public long? TotalResolvedConflicts { get; set; }
}



[Table("scope_info_sync_tables_logs")]
public class SyncLogTable
{
  [JsonIgnore]
  public SyncLog Parent { get; set; }
  public Guid SessionId { get; set; }
  public Guid ClientScopeId { get; set; }
  public string TableName { get; set; }
  public string ScopeName { get; set; }
  public string Command { get; set; }
  public long? TotalChangesSelected { get; set; }
  public long? TotalChangesSelectedUpdates { get; set; }
  public long? TotalChangesSelectedDeletes { get; set; }
  public long? TotalChangesApplied { get; set; }
  public long? TotalChangesAppliedUpdates { get; set; }
  public long? TotalChangesAppliedDeletes { get; set; }
  public long? TotalResolvedConflicts { get; set; }
}
