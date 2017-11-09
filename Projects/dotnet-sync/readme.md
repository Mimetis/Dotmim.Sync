
# Introduction

the idea is to create a CLI for enable a direct sync through a command line.

# CLI

The cli will be added as a package to the `dotnet` command.  
The command line **exe** will be called (by convention) :  `dotnet-sync.exe` to respect the naming convention from the `dotnet` runtime.  
Once installed, the command line will be then called like this : `dotnet sync [command] [--arguments]`  

## How it works
The CLI will create what we can call a **CLI Project**.
The **CLI Project** represents a sync processus beetween a server and a client.  
No sync process can be launched if no **CLI Project** is loaded (Actually, a default project is allways created when launching the CLI).  
Here is the most straightforward sample to launch a Sync process:
```
$ dotnet sync provider -p SqlSyncProvider -c "Data Source=(localdb)...." -s Server
Server provider of type SqlSyncProvider added.

$ dotnet sync provider -p SqliteSyncProvider -cs "adWorks.db" -s Client
Client provider of type SqliteSyncProvider added.

$ dotnet sync table --add ProductCategory, Product
Table ProductCategory added.

$ dotnet sync -s
Sync Start
Begin Session.
Ensure Scopes
Ensure Configuration
Configuration readed. 2 table(s) involved.
Selecting changes...
Changes selected : 0
Applying changes...
Changes applied : 0
Writing Scopes.
        436250e7-316e-45e5-ad9e-bae089e528ff synced at 06/11/2017 10:46:37.
        59a439cf-73e5-4cfb-8e19-092560554495 synced at 06/11/2017 10:46:37.
End Session.

```

Using a YAML file can be easier (see YAML section bellow)

```
$ dotnet sync yaml -f "projectsync.yml"
Loading yaml file
Project "projectsync01" loaded

$ dotnet sync -s
Sync Start
Begin Session.
Ensure Scopes
Ensure Configuration
Configuration readed. 2 table(s) involved.
Selecting changes...
Changes selected : 0
Applying changes...
Changes applied : 0
Writing Scopes.
        436250e7-316e-45e5-ad9e-bae089e528ff synced at 06/11/2017 10:46:37.
        59a439cf-73e5-4cfb-8e19-092560554495 synced at 06/11/2017 10:46:37.
End Session.
```

After a first sync, your last project is allways loaded when you launch the CLI.  
You can so directly launch the next sync process :  
```
$ dotnet sync -s
Sync Start
Begin Session.
Ensure Scopes
Ensure Configuration
Configuration readed. 2 table(s) involved.
Selecting changes...
Changes selected : 0
Applying changes...
Changes applied : 0
Writing Scopes.
        436250e7-316e-45e5-ad9e-bae089e528ff synced at 06/11/2017 10:46:37.
        59a439cf-73e5-4cfb-8e19-092560554495 synced at 06/11/2017 10:46:37.
End Session.
```


## CLI Project

The CLI will create a **CLI Project** stored in a datastore. Actually, a SQLite database is used to store CLI projects.
A CLI Project is mandatory to be able to launch a synchronization beetween two databases. When launching the CLI, a default project is loaded (called `__sync_default_project`).
A **CLI Project** can be represented by a yaml file to describe itself (see section on YAML bellow).  

A **CLI Project** is defined by :
* A project name.
* Two providers : One server and One client.
* At least one table defined by its name, schema (optional and only used on SQL Server) and direction (optional, default is `bidirectional`)
* A configuration defined with several key-value (as shown in the yaml sample below).

## Creating the CLI

**TODO** : How to install the CLI ? 

References :
* Adding the cli to `dotnet` command : [abusing dotnet core cli](https://surfingthecode.com/2017/02/abusing-dotnet-core-cli/)  
* `dotnet` extensibility model [https://docs.microsoft.com/fr-fr/dotnet/core/tools/extensibility]()
* Installing the `dotnet ef` CLI : [https://docs.microsoft.com/en-us/ef/core/miscellaneous/cli/dotnet]() : Great, but require a `.csproj` file.

## CLI Commands

### Integration within dotnet command line

Since we are called from the `dotnet` command, and to be compliant with the `dotnet` command extensibility, each command will begin like this : 
```
$ dotnet sync [command] [--arguments]
```
Some useful requests don't need [command] and are called directly with their [arguments]

* `-v` or `--verion` : Get the current CLI & Dotmim.Sync version. 
* `-h` or `--help` : Get the help informations.
* `-s` or `--sync` : Launch the sync process on the actual loaded project. 
* `--verbose` : Enable verbose output.

### Create, Get, Delete CLI Project

All project commands are called prefixed with [command] `project`.  
The `project` command is the default command, you can ommit it .  
Every sync process is associated with a project. When you launch for the first time the CLI, a **default** project is created (called `__sync_default_project`).  


Arguments available within the `project` command :
* `-n` or `--new` : Creating a new project with a unique name.
* `-l` or `--load` : Load an existing project by its unique name.
* `-r` or `--remove` : Delete an existing project by its unique name.
* `-ls` or `--list` : List all projects created and saved within CLI.


Creating a **CLI project** called "**syncproject01**" : 
```
$ dotnet sync project -n "syncproject01"

Creating new project "syncproject01"
Project "syncproject01" loaded
```

Getting an existing **CLI Project** :
```
$ dotnet sync project -l "syncproject01"

Project "syncproject01" loaded
```

Deleting an existing **CLI Project** :
```
$ dotnet sync project -d "syncproject01"

Project "syncproject01" deleted
```


### Adding Sync providers

Once you have loaded your **CLI project**, you can add providers.
You must add one server and one client provider.
  
All providers commands are available through the [command] `provider` : 
```
$ dotnet sync provider [arguments]
```

Arguments available :
* `-p` or `--providerType` : Adding a provider type, like `SqlSyncProvider` or `SqliteSyncProvider` or `MySqlSyncProvider`.
* `-c` or `--connectionString` : Adding the provider connection string.
* `-s` or `--syncType` : Adding the provider sync type : could be `Server` or `Client`

Adding providers of type `SqlSyncProvider` as server side and `SqliteSyncProvider` as client side :  
```
$ dotnet sync provider -p SqlSyncProvider -c "Data Source=(localdb)...." -s Server;

Server provider of type SqlSyncProvider added to project syncproject01

$ dotnet sync provider -p SqliteSyncProvider -cs "adWorks.db" -s Client;

Client provider of type SqliteSyncProvider added to project syncproject01
 
```

Calling the the `dotnet sync provider` again will replace the current provider : 
```
$ dotnet sync provider -p MySqlSyncProvider -c "...." -s Server;

Server provider of type MySqlSyncProvider replaced in the project syncproject01
```

### Adding Sync tables

One you have loaded your **CLI Project**, you can add tables.
At least one table is mandatory to be able to launch the sync processus
All tables commands are available through the [command] `table` :
```
$ dotnet sync table [arguments]
```

Arguments availables:
* `-a` or `--add` : Adding a table identified with its name.
* `-s` or `--schema` : Set the schema name for the current table. Only used with the `SqlSyncProvider`.
* `-r` or `--remove` : Remove the specfied table from the sync process.
* `-d` or `--direction` : Set the sync direction for the current table. Could be `Bidirectional`, `UploadOnly` or `DownloadOnly`

Adding tables to the current project :  
```
$ dotnet sync table --add ProductCategory
Table ProductCategory added to the project syncproject01

$ dotnet sync table -a Product -d downloadonly
Table Product [DownloadOnly] added to the project syncproject01

$ dotnet sync table -a Employee -d downloadonly -s Sales
Table Sales.Employee [DownloadOnly] added to the project syncproject01

```

### Adding configuration options

You can specify several options for your current **CLI Project** through the configuation command.  
All configuration options are available through the [command] `conf` :
```
$ dotnet sync conf [arguments]
```

Arguments availables:

* `-c` or `--conflict` : can be  `ServerWins` or `ClientWins`. Default is `ServerWins`.
* `-s` or `--batchSize` : set the batch size. Default is 1000.
* `-d` or `--batchDirectory` : Set the batch directory. Default is your environment temp folder.
* `-f` or `--format` : Set the serialization format. Can be `json` or `dm`. Default is `json`
* `-o` or `--bulkOperations` : Set if you want to use bulk operations when using the `SqlSyncProvider` is used. Default is `true`.

Adding configuration to the current loaded **CLI Project**:
```
$ dotnet sync conf -c ClientWins -f dm -o true
```

# YAML Format

Working with a yml file could be easier.
All YAML options are available through the [command] `yaml` :
```
$ dotnet sync yaml [arguments]
```

Arguments availables:

* `-f` or `--file`: Set the file name to load. if not set, the default directory is used.
* `-d` or `--directory` : Set the directory where the YAML file is stored.

Loaded a **CLI Project** stored in a YAML file :
```
$ dotnet sync yaml -f "projectsync.yml"
```

## YAML File sample

Example of what we can have in such way :  
```
project: projectsync01

providers:
	- providerType:	SqlSyncProvider
	  connectionString : "Data Source=(localdb)\MSSQLLocalDB; Initial Catalog=AdventureWorks;Integrated Security=true;"
	  syncType: Server	
	
	- providerType:	SqliteSyncProvider
	  connectionString : "adWorks.db"
	  syncType: Client	
	
tables: 
	- name : ProductCategory
	  schema : dbo
	  syncDirection : bidirectional

	- name : Product
	  schema : dbo
	  syncDirection : bidirectional

configuration:
	- conflictResolution : ServerWins
	- downloadBatchSizeInKB : 1000
	- batchDirectory : "C:\\tmp"
	- serializationFormat : json
	- useBulkOperations : true

```

## YAML for .NET Core parser

Available on github :  [https://github.com/aaubry/YamlDotNet](https://github.com/aaubry/YamlDotNet)















