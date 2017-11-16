
# Introduction

the idea is to create a CLI for enable a direct sync through a command line.

# CLI

The cli will be added as a package to the `dotnet` command.  
The command line **exe** will be called (by convention) :  `dotnet-sync.exe` to respect the naming convention from the `dotnet` runtime.  
Once installed, the command line will be then called like this : `dotnet sync [command] [--arguments]`  

## How it works
Firs of all, the `donet sync` CLI works with a project, called **CLI Project**, containing everything related to the sync processus (a name, a server provider, a client provider, some tables, and some optional configurations options)  
No sync process can be launched if no **CLI Project** where previously created.  

Here is the most straightforward steps to launch a sync process:

* Create a project (called projectsync01 in our sample)
* Add a server provider (SqlSyncProvider in our sample)
* Add a client provider (SqliteSyncProvider in our sample)
* Add two tables to synchronize (Product & ProductCategory in our sample)
* Launch the Sync process.

```
$ dotnet sync -n syncproject01
Project syncproject01 created.

$ dotnet sync syncproject01 provider -p sqlserver -c "Data Source=(localdb)...." -s server
Server provider of type SqlSyncProvider saved into project syncproject01.

$ dotnet sync syncproject01 provider -p sqlite -c "adWorks.db" -s client
Client provider of type SqliteSyncProvider saved into project syncproject01.

$ dotnet sync syncproject01 table --add ProductCategory
Table ProductCategory added to project syncproject01.

$ dotnet sync table --add Product
Table Product added to project syncproject01.

$ dotnet sync -s
Sync Start
Begin Session.
Ensure Scopes
Ensure Configuration
	Configuration readed. 2 table(s) involved.
Selecting changes...
	Changes selected : 0
Applying changes...
	Changes applied : 1234
Writing Scopes.
        436250e7-316e-45e5-ad9e-bae089e528ff synced at 06/11/2017 10:46:37.
        59a439cf-73e5-4cfb-8e19-092560554495 synced at 06/11/2017 10:46:37.
End Session.

```

## CLI Project

The CLI will create a **CLI Project** stored in a datastore. Actually, a **SQLite** database is used to store CLI projects.
A **CLI Project** is mandatory to be able to launch a synchronization beetween two databases. 

A **CLI Project** is defined by :
* A project name.
* Two providers : One server and One client.
* At least one table defined by its name, schema (optional and only used on SQL Server) and direction (optional, default is `bidirectional`)
* A configuration defined with several key-value (as shown in the yaml sample below).

A **CLI Project** can be represented by a yaml file also to describe itself (see section on YAML bellow).  

## Creating the CLI

**TODO** : How to install the CLI ? 

References :
* Adding the cli to `dotnet` command : [abusing dotnet core cli](https://surfingthecode.com/2017/02/abusing-dotnet-core-cli/)  
* `dotnet` extensibility model [https://docs.microsoft.com/fr-fr/dotnet/core/tools/extensibility](https://docs.microsoft.com/fr-fr/dotnet/core/tools/extensibility)
* Installing the `dotnet ef` CLI : [https://docs.microsoft.com/en-us/ef/core/miscellaneous/cli/dotnet](https://docs.microsoft.com/fr-fr/dotnet/core/tools/extensibility) : Great, but require a `.csproj` file.

## CLI Commands

### Integration within dotnet command line

Since we are called from the `dotnet` command, and to be compliant with the `dotnet` command extensibility, each command will begin like this : 
```
$ dotnet sync [command] [--arguments]
```

### Create, Get, Delete CLI Project

All projects commands are available directly with arguments : 
```
$ dotnet sync [arguments]
```

Arguments available :
* `-v`  or `--verion` 	: Get the current CLI & Dotmim.Sync version. 
* `-h`  or `--help` 	: Get the help informations.
* `-n`  or `--new` 		: Creating a new project with a unique name.
* `-i`  or `--info` 	: Load an existing project by its unique name and write all its informations.
* `-r`  or `--remove` 	: Delete an existing project by its unique name.
* `-ls` or `--list` 	: List all projects created and saved within CLI.
* `-s`  or `--sync` 	: Launch the sync process on the actual loaded project. 


Getting the **CLI project** list:
```
$ dotnet sync -ls
PROJECT                         SERVER PROVIDER         CLIENT PROVIDER         TABLES
p0                              SqlServer               Sqlite                  7
advworkspj                      SqlServer               Sqlite                  2
contoso                         SqlServer               Sqlite                  2
proxy                           Web                     Sqlite                  -
```

Creating a **CLI project** called "**syncproject01**": 
```
$ dotnet sync -n "syncproject01"
Project "syncproject01" created
```

Getting an existing **CLI Project** informations:
```
$ dotnet sync -i "syncproject01"
PROJECT                          syncproject01
SERVER PROVIDER                  SqlServer
SERVER PROVIDER CS               data source=(localdb)\mssqllocaldb; initial catalog=adventureworks; integrated security=true;
CLIENT PROVIDER                  Sqlite
CLIENT PROVIDER CS               c:\users\johndoe\.dmsync\advworks.db
CONF CONFLICT                    ServerWins
CONF BATCH DIR                   C:\Users\johndoe\AppData\Local\Temp\DotmimSync
CONF BATCH SIZE                  0
CONF SERIALIZATION               Json
CONF BULK OPERATIONS             True

TABLE                           SCHEMA                  DIRECTION       ORDER
ProductCategory                                         Bidirectional   0
ProductDescription                                      Bidirectional   1
ProductModel                                            Bidirectional   2
Product                                                 Bidirectional   3
Address                                                 Bidirectional   4
Customer                                                Bidirectional   5
CustomerAddress                                         Bidirectional   6
```

Deleting an existing **CLI Project** :
```
$ dotnet sync project -r "syncproject01"
Project "syncproject01" deleted.
```

### Adding Sync providers

Once you have loaded your **CLI project**, you can add providers.
You must add one server and one client provider.
  
All providers commands are available through the [command] `provider` : 
```
$ dotnet sync provider [arguments]
```

Arguments available :
* `-p` or `--providerType` : Adding a provider type, like `sqlserver` or `sqlite` or `web`.
* `-c` or `--connectionString` : Adding the provider connection string (or uri if provider type is set to `web`)
* `-s` or `--syncType` : Adding the provider sync type : could be `server` or `client`

Adding providers of type `SqlSyncProvider` as server side and `SqliteSyncProvider` as client side :  
```
$ dotnet sync provider -p sqlserver -c "Data Source=(localdb)...." -s server;
Server provider of type SqlSyncProvider saved to project syncproject01

$ dotnet sync provider -p sqlite -cs "adWorks.db" -s client;
Client provider of type SqliteSyncProvider saved to project syncproject01
 
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
* `-o` or `--order` : Specify table order. 
* `-s` or `--schema` : Set the schema name for the current table. Only used with the `SqlSyncProvider`.
* `-r` or `--remove` : Remove the specfied table from the sync process.
* `-d` or `--direction` : Set the sync direction for the current table. Could be `bidirectional` (or `b`), `uploadOnly` (or `u`), `downloadOnly` (or `d`)

Adding tables to the current project :  
```
$ dotnet sync table --add ProductCategory
Table ProductCategory added to the project syncproject01.

$ dotnet sync table -a Product -d downloadonly
Table Product [DownloadOnly] added to the project syncproject01.

$ dotnet sync table -a Employee -d downloadonly -s Sales
Table Sales.Employee [DownloadOnly] added to the project syncproject01.
```

### Adding configuration options

You can specify several options for your current **CLI Project** through the configuation command.  
All configuration options are available through the [command] `conf` :
```
$ dotnet sync conf [arguments]
```

Arguments availables:

* `-c` or `--conflict` : can be  `serverwins` or `clientwins`. Default is `ServerWins`.
* `-s` or `--batchSize` : set the batch size. Default is 1000.
* `-d` or `--batchDirectory` : Set the batch directory. Default is your environment temp folder.
* `-f` or `--format` : Set the serialization format. Can be `json` (JSON Format) or `bin` (BINARY Format). Default is `json`
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

* `-f` or `--file`: Set the file name to load.

Loaded a **CLI Project** stored in a YAML file :
```
$ dotnet sync yaml -f "projectsync.yml"
YAML file "projectsync.yml" correctly loaded. Project "syncproject01" with 2 table(s) loaded.
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












