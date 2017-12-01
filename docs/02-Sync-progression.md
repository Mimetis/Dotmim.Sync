# Progression

Each sync step is defined by a `SyncStage` enumeration :  

```
/// <summary>
/// Sync progress step. Used for the user feedback
/// </summary>
public enum SyncStage
{
    /// <summary>Begin a new sync session</summary>
    BeginSession,

    /// <summary>Ensure scopes</summary>
    EnsureScopes,

    /// <summary>Ensure configuration and tables</summary>
    EnsureConfiguration,

    /// <summary>Ensure database and tables</summary>
    EnsureDatabase,

    /// <summary>Occurs before changes are selected from datastore</summary>
    SelectingChanges,

    /// <summary>Occurs after changes are selected from datastore</summary>
    SelectedChanges,

    /// <summary>Occurs before applying changes</summary>
    ApplyingChanges,
    /// <summary>Occurs afeter changes are applied</summary>
    AppliedChanges,

    /// <summary>Occurs before applying inserts </summary>
    ApplyingInserts,
    /// <summary>Occurs before applying updates </summary>
    ApplyingUpdates,
    /// <summary>Occurs before applying deletes </summary>
    ApplyingDeletes,

    /// <summary>Writes scopes</summary>
    WriteMetadata,

    /// <summary>End the current sync session</summary>
    EndSession,

    /// <summary>Cleanup metadata from tracking tables.</summary>
    CleanupMetadata

}
```

You can follow the sync progression through the `SyncPogress` event :  

```
agent.SyncProgress += SyncProgress;

private static void SyncProgress(object sender, SyncProgressEventArgs e)
{
    switch (e.Context.SyncStage)
    {
        case SyncStage.BeginSession:
            Console.WriteLine($"Begin Session.");
            break;
        case SyncStage.EndSession:
            Console.WriteLine($"End Session.");
            break;
        case SyncStage.EnsureMetadata:
            if (e.Configuration != null)
                Console.WriteLine($"Configuration readed. {e.Configuration.ScopeSet.Tables.Count} table(s) involved.");
            if (e.DatabaseScript != null)
                Console.WriteLine($"Database is created");
            break;
        case SyncStage.SelectedChanges:
            Console.WriteLine($"Selected changes : {e.ChangesStatistics.TotalSelectedChanges}");
            break;
        case SyncStage.AppliedChanges:
            Console.WriteLine($"Applied changes : {e.ChangesStatistics.TotalAppliedChanges}");
            break;
        case SyncStage.WriteMetadata:
            if (e.Scopes != null)
                e.Scopes.ForEach(sc => Console.WriteLine($"\t{sc.Id} synced at {sc.LastSync}. "));
            break;
        case SyncStage.CleanupMetadata:
            Console.WriteLine($"CleanupMetadata");
            break;
    }
}

```