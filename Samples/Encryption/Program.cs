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

            var myRijndael = new RijndaelManaged();
            myRijndael.GenerateKey();
            myRijndael.GenerateIV();

            // Create action for serializing and deserialzing for both remote and local orchestrators
            var deserializing = new Func<DeserializingRowArgs, Task>((args) =>
            {
                Console.ForegroundColor = ConsoleColor.DarkYellow;
                Console.WriteLine($"Deserializing row {args.RowString}");
                string value;
                var byteArray = Convert.FromBase64String(args.RowString);
                using var decryptor = myRijndael.CreateDecryptor(myRijndael.Key, myRijndael.IV);
                using var msDecrypt = new MemoryStream(byteArray);
                using var csDecrypt = new CryptoStream(msDecrypt, decryptor, CryptoStreamMode.Read);
                using (var swDecrypt = new StreamReader(csDecrypt))
                    value = swDecrypt.ReadToEnd();

                var array = JsonSerializer.Deserialize<object[]>(value);
                args.Result = array;

                // output result to console
                Console.WriteLine($"row deserialized {new SyncRow(args.SchemaTable, array)}");
                Console.ResetColor();
                return Task.CompletedTask;
            });


            var serializing = new Func<SerializingRowArgs, Task>(async (sra) =>
            {
                Console.ForegroundColor = ConsoleColor.DarkGreen;
                // output arg to console
                Console.WriteLine($"Serializing row {new SyncRow(sra.SchemaTable, sra.RowArray).ToString()}");


                var strSet = JsonSerializer.Serialize(sra.RowArray);
                using var encryptor = myRijndael.CreateEncryptor(myRijndael.Key, myRijndael.IV);
                using var msEncrypt = new MemoryStream();
                using var csEncrypt = new CryptoStream(msEncrypt, encryptor, CryptoStreamMode.Write);
                using (var swEncrypt = new StreamWriter(csEncrypt))
                    swEncrypt.Write(strSet);

                sra.Result = Convert.ToBase64String(msEncrypt.ToArray());
                Console.WriteLine($"row serialized: {0}", sra.Result);
                Console.ResetColor();
                return Task.CompletedTask;
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
