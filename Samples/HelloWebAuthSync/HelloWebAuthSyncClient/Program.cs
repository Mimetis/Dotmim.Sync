using Dotmim.Sync;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Web.Client;
using Microsoft.IdentityModel.Tokens;
using System;
using System.Collections.Generic;
using System.IdentityModel.Tokens.Jwt;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Security.Claims;
using System.Text;
using System.Threading.Tasks;

namespace HelloWebSyncClient
{
    class Program
    {
        private static string clientConnectionString = $"Data Source=(localdb)\\mssqllocaldb; Initial Catalog=Client;Integrated Security=true;";

        static async Task Main()
        {
            try
            {
                Console.WriteLine("Be sure the web api has started. Then click enter..");
                Console.ReadLine();
                await SynchronizeAsync();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        private static async Task SynchronizeAsync()
        {
            // Database script used for this sample : https://github.com/Mimetis/Dotmim.Sync/blob/master/CreateAdventureWorks.sql 

            // Getting a JWT token
            // This sample is NOT SECURE at all
            // You should get a Jwt Token from an identity provider like Azure, Google, AWS or other.
            var token = GenerateJwtToken("spertus@microsoft.com", "SPERTUS01");
            HttpClient httpClient = new HttpClient();
            httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token);

            // Adding the HttpClient instance to the web client orchestrator
            var serverOrchestrator = new WebRemoteOrchestrator("https://localhost:44342/api/sync", client:httpClient);

            // Second provider is using plain old Sql Server provider, relying on triggers and tracking tables to create the sync environment
            var clientProvider = new SqlSyncProvider(clientConnectionString);

            // Creating an agent that will handle all the process
            var agent = new SyncAgent(clientProvider, serverOrchestrator);

            do
            {
                // Launch the sync process
                var s1 = await agent.SynchronizeAsync();
                // Write results
                Console.WriteLine(s1);

            } while (Console.ReadKey().Key != ConsoleKey.Escape);

            Console.WriteLine("End");
        }

        private static string GenerateJwtToken(string email, string userid)
        {
            var claims = new List<Claim>
            {
                new Claim(JwtRegisteredClaimNames.Iss, "Dotmim.Sync.Bearer"),
                new Claim(JwtRegisteredClaimNames.Aud, "Dotmim.Sync.Bearer"),
                new Claim(JwtRegisteredClaimNames.Sub, "Dotmim.Sync"),
                new Claim(JwtRegisteredClaimNames.Email, email),
                new Claim(JwtRegisteredClaimNames.Jti, Guid.NewGuid().ToString()),
                new Claim(ClaimTypes.NameIdentifier, userid)
            };

            var key = new SymmetricSecurityKey(Encoding.UTF8.GetBytes("SOME_RANDOM_KEY_DO_NOT_SHARE"));
            var creds = new SigningCredentials(key, SecurityAlgorithms.HmacSha256);

            var expires = DateTime.Now.AddDays(Convert.ToDouble(10));

            var token = new JwtSecurityToken(
                "Dotmim.Sync.Bearer",
                "Dotmim.Sync.Bearer",
                claims,
                expires: expires,
                signingCredentials: creds
            );

            return new JwtSecurityTokenHandler().WriteToken(token);
        }
    }
}
