# Tables already existing

Imagine you have already a **master** database and already some **clients** database.  
You may want to initiate a **new** synchronization beetween **existing** databases, that contains **existing** rows.   
The quick answer to this is : It's working by default, with no additionnal steps to do.  
Here is a basic solution showing you how you can deal with it.

## Solution

Basically, the starter solution is this one:
You have both an **already existing SQLite** client database and an **already existing SQL Server** master database.  
I assume you're aware that the **schema** should be **exactly the same** beetween the server and the client, since it's NOT dedicated to the `Dotmim.Sync` fx (to create the client schema)   

So to start, imagine we have those 2 databases:

Here is the **SQL Server** database, acting as the `master`:
```sql
if (not exists (select * from sys.tables where name = 'ServiceTickets'))
begin
   CREATE TABLE [ServiceTickets](
     [ServiceTicketID] [uniqueidentifier] NOT NULL,
     [Title] [nvarchar](max) NOT NULL,
     [StatusValue] [int] NOT NULL,
     [Opened] [datetime] NULL,
   CONSTRAINT [PK_ServiceTickets] PRIMARY KEY CLUSTERED ( [ServiceTicketID] ASC ));
end
```
And then the **sqlite** table as the `client`:
```sql
CREATE TABLE IF NOT EXISTS [ServiceTickets](
  [ServiceTicketID] blob NOT NULL,
  [Title] text NOT NULL,
  [StatusValue] integer NOT NULL,
  [Opened] datetime NULL,
PRIMARY KEY ( [ServiceTicketID] ASC ))
```
Then we can add 1 row in each database, **before** engaging any sync (even any sync configuration) with this kind of method:

``` csharp
public void AddingDatas(DbConnection connection)
{

    var command = connection.CreateCommand();
    command.CommandText = $@"INSERT INTO [ServiceTickets] ([ServiceTicketID], [Title], [StatusValue], [Opened]) 
                                VALUES (@ServiceTicketID, @Title, @StatusValue, @Opened)";

    DbParameter parameter = null;
    parameter = command.CreateParameter();
    parameter.DbType = DbType.Guid;
    parameter.ParameterName = "@ServiceTicketID";
    parameter.Value = Guid.NewGuid();
    command.Parameters.Add(parameter);

    parameter = command.CreateParameter();
    parameter.DbType = DbType.String;
    parameter.ParameterName = "@Title";
    parameter.Value = $"Title - {Guid.NewGuid().ToString()}";
    command.Parameters.Add(parameter);

    parameter = command.CreateParameter();
    parameter.DbType = DbType.Int32;
    parameter.ParameterName = "@StatusValue";
    parameter.Value = new Random().Next(0, 10);
    command.Parameters.Add(parameter);

    parameter = command.CreateParameter();
    parameter.DbType = DbType.DateTime;
    parameter.ParameterName = "@Opened";
    parameter.Value = DateTime.Now;
    command.Parameters.Add(parameter);

    try
    {
        connection.Open();
        command.ExecuteNonQuery();
        connection.Close();
    }
    catch (Exception ex)
    {
        Debug.WriteLine(ex.Message);
        throw;
    }
    finally
    {
        if (connection.State != ConnectionState.Closed)
            connection.Close();
    }
}
```

Ok, right now, I have **One line on the server side** and **One line on the client side**.  
Be aware, we are at a starting point where **NO** Sync architecture is involved (no metadatas, no tracking tables and so on...)

Then, we can try a new sync processus:

```csharp
SqlSyncProvider serverProvider;
SqliteSyncProvider clientProvider;

serverProvider = new SqlSyncProvider(serverConnectionString);
clientProvider = new SqliteSyncProvider(clientSqliteFilePath);

var simpleConfiguration = new SyncConfiguration();

agent = new SyncAgent(clientProvider, serverProvider, (conf =>{
    conf.Add(new string[] { "ServiceTickets" })
}));

var progressClient = new Progress<ProgressArgs>(s => Console.WriteLine($"[Client]: {s.Context.SyncStage}:\t{s.Message}"));
var progressServer = new Progress<ProgressArgs>(s => Console.WriteLine($"[Server]: {s.Context.SyncStage}:\t{s.Message}"));

agent.RemoteProvider.SetProgress(progress);
var session = await agent.SynchronizeAsync(progress);

Console.WriteLine(session);
```

Here is an extract output debug I have:
```
[Client]:Changes selected TableName: ServiceTickets Deletes: 0 Inserts: 1 Updates: 0 TotalChanges: 1
[Server]:Applying changes TableName: ServiceTickets State: Added
[Server]:Changes applied TableName: ServiceTickets State: Added Applied: 1 Failed: 0
[Server]:Selecting changes TableName: ServiceTickets
[Server]:Changes selected TableName: ServiceTickets Deletes: 0 Inserts: 1 Updates: 0 TotalChanges: 1
[Client]:Applying changes TableName: ServiceTickets State: Added
[Client]:Changes applied TableName: ServiceTickets State: Added Applied: 1 Failed: 0

Synchronization done.
    Total changes downloaded: 1 
    Total changes uploaded: 1
    Total conflicts: 0
    Total duration :0:0:3.281 
```
