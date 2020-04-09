# How does it work

Basically, **DMS** architecture is composed of several business objects:
- **Providers** : A provider is responsible to communicate with the local database. You can choose various providers, like `SQL`, `MySQL` or `Sqlite`. Each provider can work on both side of the sync architecture : Server side or Client side.
- **Orchestrators** : An orchestrator is agnostic to the underline database. An orchestrator communicate with the database through a provider. A provider is always required when you create a new orchestrator. We have two kind of orchestrator : local and remote (or client and server)
- **SyncAgent**: There is only one sync agent. This object is responsible of the correct *flow* between two orchestrators. The sync agent will:
  - Create a local orchestrator with a typed provider.
  - Create a remote orchestrator with a typed provider.
  - Synchronize client and server, using all the methods from the orchestrators.

## Overview

Here is the big picture of the components used in a simple synchronization, over **TCP**:

![Simple TCP architecture](/assets/Architecture01.png)

If we take a close look to the `HelloSync` sample:

``` cs
var serverProvider = new MySqlSyncProvider(serverConnectionString);
var clientProvider = new SqliteSyncProvider(clientConnectionString);

var tables = new string[] {"ProductCategory", "ProductModel", "Product" };

var agent = new SyncAgent(clientProvider, serverProvider, tables);

var result = await agent.SynchronizeAsync();

Console.WriteLine(result);
```

There is no mention of any `Orchestrators` here. It's basically because the `SyncAgent` instance will create them under the hood, for simplicity.  
We can rewrite this code, this way:

``` cs

// Create 2 providers, one for MySql, one for Sqlite.
var serverProvider = new MySqlSyncProvider(serverConnectionString);
var clientProvider = new SqliteSyncProvider(clientConnectionString);

// Setup and options define the tables and some useful options.
var setup = new SyncSetup(new string[] {"ProductCategory", "ProductModel", "Product" });
var options = new SyncOptions();

// Define a local orchestrator, using the Sqlite provider
// and a remote orchestrator, using the MySql provider.
var localOrchestrator = new LocalOrchestrator(clientProvider, options, setup);
var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options, setup);

// Create the agent with existing orchestrators
var agent = new SyncAgent(localOrchestrator, remoteOrchestrator);

// Launch the sync
var result = await agent.SynchronizeAsync();

Console.WriteLine(result);
```

As you can see here, all the components are declared:
- Each provider : One Sqlite and One MySql
- Each orchestrator : a local orchestrator coupled with the Sqlite provider and a remote orchestrator coupled with the MySql provider
- One sync agent : The sync agent instance needs of course both orchestrators to be able to launch the sync process.

## Multiple clients overview

Of course, a real scenario will involve more clients databases.   
Each client will have its own provider, depending on the local database type. 
And each client will have a sync agent, responsible of the sync process:

![Simple TCP architecture 2](/assets/Architecture02.png)


## Sync over HTTP

In a real world scenario, you may want to protect your hub database (the *server side* database), if your clients are not part of your local network, like mobile devices which will communicate only through an http connection.   
In this particular scenario, the sync agent will not be able to use a simple RemoteOrchestrator, since this one works only on a tcp network.   
Here is coming a new orchestrator in the game. Or shoud I say *two* new orchestrators:
- The `WebClientOrchestrator`: This orchestrator will run locally, and will act "*as*" a orchestrator from the sync agent, but under the hood will generate an http request with a payload containing all the required information
- The `WebServerOrchestrator`: On the opposite side, this orchestrator is hosted through an exposed web api, and will get the incoming request from the `WebClientOrchestrator` and will then call the server provider correctly.

Here is the big picture of this more advanced scenario:

![Simple Http architecture 2](/assets/Architecture03.png)

You can read more on the web architecture and how to implement it, here : [Asp.net Core Web Api sync proxy](/web)
