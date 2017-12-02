# Filtering tables

You can filter datas from any tables.  
In a nutshell, enabling filter is two steps :
1. Configuring tables and columns on the server side in the `SyncConfiguration` object.
2. Adding the correct paramater on each client in the `SyncAgent` orchestrator.

## Server side configuration

First of all, you have to add all the filtered **tables** and the corresponding **columns** in the `SyncConfiguration` object.
This step is required on the **server** side, to be able to generate all required **stored procédures**.  

In the `Filters` property, add a new filter, composed with **table** name and **column** name :  
```
SyncConfiguration configuration = new SyncConfiguration(new[] { "ServiceTickets" });

// Add a filter
configuration.Filters.Add("ServiceTickets", "CustomerID");

```
## Client side configuration

On each client, you will have to specify the **value** to provider fo filtering.  
On the `SyncAgent` just add the corresponding parameter, with the correct value :  

```
SyncAgent agent = new SyncAgent(clientProvider, serverProvider, configuration);
agent.Parameters.Add("ServiceTickets", "CustomerID", 10);

var session = await agent.SynchronizeAsync();

```

On this particular client, only tickets when CustomerId=10 are synchronized. 
