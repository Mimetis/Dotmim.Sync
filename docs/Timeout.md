# Increasing timeout

If you're not working on **TCP** but more likely on **HTTP**, using a web api to expose your sync process, you will probably have to face some issues with timeout.  

> Before increasing timeout, be sure you have already setup a [snapshot](./Snapshot) for all your new clients.

By default, `Timeout` is fixed to 2 minutes.

To increase the overall timeout, you will have to work on both side: 
- Your web server api project.
- Your client application.

## Server side
There is no way to increase the `Timeout` period on your web api, with **.Net Core**   
The only solution is to provide a `web.config`, that you add manually to your project.  
More information here : [increase-the-timeout-of-asp-net-core-application](https://medium.com/aspnetcore/increase-the-timeout-of-asp-net-core-application-9a7b4f6deebf)

Here is a `web.config` example where `requestTimeout` is fixed to 20 minutes:
``` xml
<?xml version="1.0" encoding="utf-8"?>
<configuration>
  <system.webServer>
    <handlers>
      <add name="aspNetCore" path="*" verb="*" modules="AspNetCoreModule" resourceType="Unspecified"/>
    </handlers>
    <aspNetCore requestTimeout="00:20:00"  processPath="%LAUNCHER_PATH%" arguments="%LAUNCHER_ARGS%" stdoutLogEnabled="false" stdoutLogFile=".\logs\stdout" forwardWindowsAuthToken="false"/>
  </system.webServer>
</configuration>
```

## Client side

On the client side, the web orchestrator `WebClientOrchestrator` instance uses its own `HttpClient` instance unless you specify your own `HttpClient` instance.

So far, to increase the timeout, you can either:

* Provide your own `HttpClient` instance with the `Timeout` property correctly set:
``` cs
var handler = new HttpClientHandler { AutomaticDecompression = DecompressionMethods.GZip };
var client = new HttpClient(handler) { Timeout = TimeSpan.FromMinutes(20) };
var proxyClientProvider = new WebClientOrchestrator("http://localhost:52288/api/Sync", null, null, client);
```
* Increase the existing `HttpClient` instance, created by `WebClientOrchestrator`:
``` cs
var proxyClientProvider = new WebClientOrchestrator("http://localhost:52288/api/Sync");
proxyClientProvider.HttpClient.Timeout = TimeSpan.FromMinutes(20);

```
