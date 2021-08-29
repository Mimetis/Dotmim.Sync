using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Caching.Memory;
using Newtonsoft.Json;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

#if NET5_0 || NETCOREAPP3_1
using Microsoft.Extensions.Hosting;
#endif

namespace Dotmim.Sync.Web.Server
{
    /// <summary>
    /// Contains all providers registered on the server side
    /// </summary>
    public class WebServerManager 
    {
        public static Task WriteHelloAsync(HttpContext context, WebServerOrchestrator orchestrator, CancellationToken cancellationToken = default)
            => WriteHelloAsync(context, new[] { orchestrator }, cancellationToken);

        public static async Task WriteHelloAsync(HttpContext context, IEnumerable<WebServerOrchestrator> orchestrators, CancellationToken cancellationToken = default)
        {
            var httpResponse = context.Response;
            var stringBuilder = new StringBuilder();


            stringBuilder.AppendLine("<!doctype html>");
            stringBuilder.AppendLine("<html>");
            stringBuilder.AppendLine("<head>");
            stringBuilder.AppendLine("<meta charset='utf-8'>");
            stringBuilder.AppendLine("<meta name='viewport' content='width=device-width, initial-scale=1, shrink-to-fit=no'>");
            stringBuilder.AppendLine("<script src='https://cdn.jsdelivr.net/gh/google/code-prettify@master/loader/run_prettify.js'></script>");
            stringBuilder.AppendLine("<link rel='stylesheet' href='https://stackpath.bootstrapcdn.com/bootstrap/4.4.1/css/bootstrap.min.css' integrity='sha384-Vkoo8x4CGsO3+Hhxv8T/Q5PaXtkKtu6ug5TOeNV6gBiFeWPGFN9MuhOf23Q9Ifjh' crossorigin='anonymous'>");
            stringBuilder.AppendLine("</head>");
            stringBuilder.AppendLine("<title>Web Server properties</title>");
            stringBuilder.AppendLine("<body>");


            stringBuilder.AppendLine("<div class='container'>");
            stringBuilder.AppendLine("<h2>Web Server properties</h2>");

            foreach (var webOrchestrator in orchestrators)
            {

                string dbName = null;
                string version = null;
                string exceptionMessage = null;
                bool hasException = false;
                try
                {
                    (dbName, version) = await webOrchestrator.GetHelloAsync();
                }
                catch (Exception ex)
                {
                    exceptionMessage = ex.Message;
                    hasException = true;

                }

                stringBuilder.AppendLine("<ul class='list-group mb-2'>");
                stringBuilder.AppendLine($"<li class='list-group-item active'>Trying to reach database</li>");
                stringBuilder.AppendLine("</ul>");
                if (hasException)
                {
                    stringBuilder.AppendLine("<ul class='list-group mb-2'>");
                    stringBuilder.AppendLine($"<li class='list-group-item list-group-item-primary'>Exception occured</li>");
                    stringBuilder.AppendLine($"<li class='list-group-item list-group-item-danger'>");
                    stringBuilder.AppendLine($"{exceptionMessage}");
                    stringBuilder.AppendLine("</li>");
                    stringBuilder.AppendLine("</ul>");
                }
                else
                {
                    stringBuilder.AppendLine("<ul class='list-group mb-2'>");
                    stringBuilder.AppendLine($"<li class='list-group-item list-group-item-primary'>Database</li>");
                    stringBuilder.AppendLine($"<li class='list-group-item list-group-item-light'>");
                    stringBuilder.AppendLine($"Check database {dbName}: Done.");
                    stringBuilder.AppendLine("</li>");
                    stringBuilder.AppendLine("</ul>");

                    stringBuilder.AppendLine("<ul class='list-group mb-2'>");
                    stringBuilder.AppendLine($"<li class='list-group-item list-group-item-primary'>Engine version</li>");
                    stringBuilder.AppendLine($"<li class='list-group-item list-group-item-light'>");
                    stringBuilder.AppendLine($"{version}");
                    stringBuilder.AppendLine("</li>");
                    stringBuilder.AppendLine("</ul>");
                }

                stringBuilder.AppendLine("<ul class='list-group mb-2'>");
                stringBuilder.AppendLine($"<li class='list-group-item active'>ScopeName: {webOrchestrator.ScopeName}</li>");
                stringBuilder.AppendLine("</ul>");

                var s = JsonConvert.SerializeObject(webOrchestrator.Setup, Formatting.Indented);
                stringBuilder.AppendLine("<ul class='list-group mb-2'>");
                stringBuilder.AppendLine($"<li class='list-group-item list-group-item-primary'>Setup</li>");
                stringBuilder.AppendLine($"<li class='list-group-item list-group-item-light'>");
                stringBuilder.AppendLine("<pre class='prettyprint' style='border:0px;font-size:75%'>");
                stringBuilder.AppendLine(s);
                stringBuilder.AppendLine("</pre>");
                stringBuilder.AppendLine("</li>");
                stringBuilder.AppendLine("</ul>");

                s = JsonConvert.SerializeObject(webOrchestrator.Provider, Formatting.Indented);
                stringBuilder.AppendLine("<ul class='list-group mb-2'>");
                stringBuilder.AppendLine($"<li class='list-group-item list-group-item-primary'>Provider</li>");
                stringBuilder.AppendLine($"<li class='list-group-item list-group-item-light'>");
                stringBuilder.AppendLine("<pre class='prettyprint' style='border:0px;font-size:75%'>");
                stringBuilder.AppendLine(s);
                stringBuilder.AppendLine("</pre>");
                stringBuilder.AppendLine("</li>");
                stringBuilder.AppendLine("</ul>");

                s = JsonConvert.SerializeObject(webOrchestrator.Options, Formatting.Indented);
                stringBuilder.AppendLine("<ul class='list-group mb-2'>");
                stringBuilder.AppendLine($"<li class='list-group-item list-group-item-primary'>Options</li>");
                stringBuilder.AppendLine($"<li class='list-group-item list-group-item-light'>");
                stringBuilder.AppendLine("<pre class='prettyprint' style='border:0px;font-size:75%'>");
                stringBuilder.AppendLine(s);
                stringBuilder.AppendLine("</pre>");
                stringBuilder.AppendLine("</li>");
                stringBuilder.AppendLine("</ul>");

                s = JsonConvert.SerializeObject(webOrchestrator.WebServerOptions, Formatting.Indented);
                stringBuilder.AppendLine("<ul class='list-group mb-2'>");
                stringBuilder.AppendLine($"<li class='list-group-item list-group-item-primary'>Web Server Options</li>");
                stringBuilder.AppendLine($"<li class='list-group-item list-group-item-light'>");
                stringBuilder.AppendLine("<pre class='prettyprint' style='border:0px;font-size:75%'>");
                stringBuilder.AppendLine(s);
                stringBuilder.AppendLine("</pre>");
                stringBuilder.AppendLine("</li>");
                stringBuilder.AppendLine("</ul>");



            }
            stringBuilder.AppendLine("</div>");
            stringBuilder.AppendLine("</body>");
            stringBuilder.AppendLine("</html>");



            await httpResponse.WriteAsync(stringBuilder.ToString(), cancellationToken);


        }


    }
}
