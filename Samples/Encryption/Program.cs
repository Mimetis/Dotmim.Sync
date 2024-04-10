using Dotmim.Sync;
using Dotmim.Sync.SqlServer;
using System;
using System.IO;
using System.Security.Cryptography;
using System.Text.Json;
using System.Threading.Tasks;

namespace EncryptionClient
{
    class Program
    {
        public static string GetDatabaseConnectionString(string dbName) =>
                 $"Data Source=(localdb)\\mssqllocaldb; Initial Catalog={dbName}; Integrated Security=true;";

        static async Task Main(string[] args)
        {
            await EncryptionAsync();
            Console.ReadLine();
        }

        private static async Task EncryptionAsync()
        {
            // Database script used for this sample : https://github.com/Mimetis/Dotmim.Sync/blob/master/CreateAdventureWorks.sql 

            using var myAes = Aes.Create();

            myAes.GenerateKey();
            myAes.GenerateIV();

            // Create action for serializing and deserialzing for both remote and local orchestrators
            var deserializing = new Func<DeserializingRowArgs, Task>(async (args) =>
            {
                Console.ForegroundColor = ConsoleColor.DarkYellow;
                Console.WriteLine($"Deserializing row {args.RowString}");

                var byteArray = Convert.FromBase64String(args.RowString);

                using var decryptor = myAes.CreateDecryptor(myAes.Key, myAes.IV);
                await using var msDecrypt = new MemoryStream(byteArray);
                await using var csDecrypt = new CryptoStream(msDecrypt, decryptor, CryptoStreamMode.Read);

                args.Result = await JsonSerializer.DeserializeAsync<object[]>(csDecrypt);

                // output result to console
                Console.WriteLine($"row deserialized {new SyncRow(args.SchemaTable, args.Result)}");
                Console.ResetColor();
            });


            var serializing = new Func<SerializingRowArgs, Task>(async (sra) =>
            {
                Console.ForegroundColor = ConsoleColor.DarkGreen;
                // output arg to console
                Console.WriteLine($"Serializing row {new SyncRow(sra.SchemaTable, sra.RowArray).ToString()}");


                var strSet = JsonSerializer.Serialize(sra.RowArray);
                using var encryptor = myAes.CreateEncryptor(myAes.Key, myAes.IV);
                await using var msEncrypt = new MemoryStream();
                await using var csEncrypt = new CryptoStream(msEncrypt, encryptor, CryptoStreamMode.Write);
                await using (var swEncrypt = new StreamWriter(csEncrypt))
                    swEncrypt.Write(strSet);

                sra.Result = Convert.ToBase64String(msEncrypt.ToArray());
                Console.WriteLine($"row serialized: {0}", sra.Result);
                Console.ResetColor();
            });

            SqlSyncProvider serverProvider = new SqlSyncProvider(GetDatabaseConnectionString("AdventureWorks"));
            SqlSyncProvider clientProvider = new SqlSyncProvider(GetDatabaseConnectionString("Client"));

            // Defining options with Batchsize to enable serialization on disk
            var options = new SyncOptions { BatchSize = 1000 };

            var tables = new string[] { "ProductCategory", "ProductModel", "Product", "Address", "Customer", "CustomerAddress", "SalesOrderHeader", "SalesOrderDetail" };

            SyncAgent agent = new SyncAgent(clientProvider, serverProvider, options);

            // Get the orchestrators
            var localOrchestrator = agent.LocalOrchestrator;
            var remoteOrchestrator = agent.RemoteOrchestrator;

            // Encrypting / decrypting data on disk
            localOrchestrator.OnSerializingSyncRow(serializing);
            localOrchestrator.OnDeserializingSyncRow(deserializing);
            remoteOrchestrator.OnSerializingSyncRow(serializing);
            remoteOrchestrator.OnDeserializingSyncRow(deserializing);

            Console.WriteLine(await agent.SynchronizeAsync(tables));

            Console.WriteLine("End");
        }
    }
}
