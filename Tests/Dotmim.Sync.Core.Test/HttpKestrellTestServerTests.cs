using Microsoft.AspNetCore.Http;
using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Dotmim.Sync.Core.Test
{
    public class HttpKestrellTestServerTests
    {

        [Fact]
        public async Task Kestrell_Server_Request_EnsureSuccess()
        {
            using (var server = new KestrellTestServer())
            {
                var clientHandler = new ResponseDelegate(async baseAdress => {
                    var httpClient = new HttpClient();

                    var response = await httpClient.GetAsync(baseAdress + "first");
                    response.EnsureSuccessStatusCode();

                    var resString = await response.Content.ReadAsStringAsync();
                    Assert.Equal("first_first", resString);
                });

                var serverHandler = new RequestDelegate(async context =>
                {
                    var pathFirst = new PathString("/first");
                    Assert.Equal(context.Request.Path, pathFirst);

                    await context.Response.WriteAsync("first_first");
                });

                await server.Run(serverHandler, clientHandler);
            };
        }
    }
}
