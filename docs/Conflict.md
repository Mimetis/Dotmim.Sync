# Conflicts

## Default behavior

By default, conflicts are resolved automaticaly using the configuration policy property `ConflictResolutionPolicy` set in the `SyncConfiguration` object :  
You can choose: 
* `ConflictResolutionPolicy.ServerWins` : The server is allways the winner of any conflict. this behavior is the default behavior.
* `ConflictResolutionPolicy.ClientWins` : The client is allways the winner of any conflict.

``` cs
configuration.ConflictResolutionPolicy = ConflictResolutionPolicy.ServerWins;
``` 

First of all, **a conflict is allways resolved from the server side.**

Depending on your policy resolution, the workflow is:
* A conflict is generated on the client and the server side (for example a row is updated on both the server and the client database)
* The client is launching a sync processus.
* The server tries to apply the row and a conflict is generated.
* The server resolve the conflict on the server side.
* If the server wins, the server row is sent to the client and will overwrite the client version.
* If the client wins, the server will force apply the client row on the server.


## Handling conflicts manually

If you decide to manually resolve a conflict, the `ConflictResolutionPolicy` option will be ignored.  
To be able to resolve a conflict, you just have to subscribe on the `ApplyChangedFailed` event and choose the correct version.  

``` cs
// Subscribing to the ApplyChangedFailed event
agent.ApplyChangedFailed += ApplyChangedFailed;

// Custom conflict resolution
static void ApplyChangedFailed(object sender, ApplyChangeFailedEventArgs e)
{
}
```

The `ApplyChangeFailedEventArgs` argument contains all the required propreties to be able to resolve your conflict:

You will determinate the correct version through the `Action` property of type `ConflictAction`:
* `ConflictAction.ClientWins` : The client row will be applied on server, even if there is a conflict, so the client row wins.
* `ConflictAction.ServerWins` : The client row won't be applied on the server, so the server row wins.
* `ConflictAction.MergeRow`   : It's up to you to choose the correct row to send on both server and client. the `FinalRow` object will be used instead of Server or Client row.

You are able to compare the row in conflict through the `Conflict` property of type `SyncConflict`:
* `Conflict.LocalRow`   : Contains the conflict row from the client side. This row is readonly.
* `Conflict.RemoteRow`  : Contains the conflict row from the server side. This row is readonly.
* `Conflict.Type`       : Gets the `ConflictType` enumeration. For example `ConflictType.RemoteUpdateLocalUpdate` represents a conflict row beetween an updated row on the server and the same row updated on the client as well.

You can use the current connection during this event to be able to perform actions on the server side through the `DbConnection` and `DbTransaction` properties.  
If you decide to rollback the transaction, all the sync process will be rollback. 

Eventually, the `FinalRow` property is used when you specify an Action to `ConflictAction.MergeRow`. In this way, you decide what will contains the row applied on both server and client side. Be careful, the `FinalRow` property is null until you specify the `Action` property to `ConflictAction.MergeRow` !

## Examples

Manually resolving a conflict based on a column value:

``` cs
private void ApplyChangedFailed(object sender, ApplyChangeFailedEventArgs e)
{
    e.Action = (int)e.Conflict.RemoteRow["Id"] == 1 ? ConflictAction.ClientWins : ConflictAction.ServerWins;
}
```

Manually resolving a conflict based on the conflict type :

``` cs
private void ApplyChangedFailed(object sender, ApplyChangeFailedEventArgs e)
{
    switch (e.Conflict.Type)
    {
        case ConflictType.RemoteUpdateLocalInsert:
        case ConflictType.RemoteUpdateLocalUpdate:
        case ConflictType.RemoteUpdateLocalDelete:
        case ConflictType.RemoteInsertLocalInsert:
        case ConflictType.RemoteInsertLocalUpdate:
        case ConflictType.RemoteInsertLocalDelete:
            e.Action = ConflictAction.ServerWins;
            break;
        case ConflictType.RemoteUpdateLocalNoRow:
        case ConflictType.RemoteInsertLocalNoRow:
        case ConflictType.RemoteDeleteLocalNoRow:
            e.Action = ConflictAction.ClientWins;
            break;
    }
}
```

Resolving a conflict by specifying a merged row :

``` cs
static void ApplyChangedFailed(object sender, ApplyChangeFailedEventArgs e)
{
    e.Action = ConflictAction.MergeRow;
    e.FinalRow["RegionDescription"] = "Eastern alone !";
}
```
Be careful, the `e.FinalRow` is null until you specify the `Action` property to `ConflictAction.MergeRow` !