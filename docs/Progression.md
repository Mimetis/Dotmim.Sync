# Progression

Each sync step is defined by a `SyncStage` enumeration :  

``` cs
/// <summary>
/// Sync progress step.
/// </summary>
public enum SyncStage
{
}
```

Possibles values are:
* **BeginSession**: Begin a new sync session.
* **EnsureScopes**: Check if the scope table is created and check last sync.
* **EnsureConfiguration**: Load configuration.
* **EnsureDatabase**: Ensure databases are created, following the configuration object.
* **SelectingChanges**: Occurs when local rows are requested.
* **SelectedChanges**: Occurs after changes are selected from datastore.
* **ApplyingChanges**: Occurs before applying changes.
* **AppliedChanges**: Occurs afeter changes are applied.
* **WriteMetadata**: Write metadatas after a sync is completed.
* **EndSession**: End the current sync session.
* **CleanupMetadata**: Cleanup metadata from tracking tables.
    
You can follow the sync progression through the `SyncPogress` event :  

``` cs
agent.SyncProgress += SyncProgress;
```

Here is a quick example, often used to provide some feedback to the users

``` cs
private static void SyncProgress(object sender, SyncProgressEventArgs e)
{
    switch (e.Context.SyncStage)
    {
        case SyncStage.SelectedChanges:
            Console.WriteLine($"Selected changes : {e.ChangesStatistics.TotalSelectedChanges}");
            break;
        case SyncStage.AppliedChanges:
            Console.WriteLine($"Applied changes : {e.ChangesStatistics.TotalAppliedChanges}");
            break;
    }
}

```