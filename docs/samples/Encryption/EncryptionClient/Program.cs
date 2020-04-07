using Dotmim.Sync;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Web.Client;
using Newtonsoft.Json;
using System;
using System.IO;
using System.Security.Cryptography;
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
            var deserializing = new Action<DeserializingSetArgs>(dsa =>
            {
                // Create an encryptor to perform the stream transform.
                var decryptor = myRijndael.CreateDecryptor(myRijndael.Key, myRijndael.IV);

                using (var csDecrypt = new CryptoStream(dsa.FileStream, decryptor, CryptoStreamMode.Read))
                {
                    using (var swDecrypt = new StreamReader(csDecrypt))
                    {
                        //Read all data to the ContainerSet
                        var str = swDecrypt.ReadToEnd();
                        dsa.Result = JsonConvert.DeserializeObject<ContainerSet>(str);

                        Console.WriteLine($"Deserialized container from file {dsa.FileName}. Container tables count:{dsa.Result.Tables.Count}");
                    }
                }
            });


            var serializing = new Action<SerializingSetArgs>(ssa =>
            {

                Console.WriteLine($"Serialized container to file {ssa.FileName}. container tables count:{ssa.Set.Tables.Count}");

                // Create an encryptor to perform the stream transform.
                var encryptor = myRijndael.CreateEncryptor(myRijndael.Key, myRijndael.IV);

                using (var msEncrypt = new MemoryStream())
                {
                    using (var csEncrypt = new CryptoStream(msEncrypt, encryptor, CryptoStreamMode.Write))
                    {
                        using (var swEncrypt = new StreamWriter(csEncrypt))
                        {
                            //Write all data to the stream.
                            var strSet = JsonConvert.SerializeObject(ssa.Set);
                            swEncrypt.Write(strSet);
                        }
                        ssa.Result = msEncrypt.ToArray();
                    }
                }

            });

            SqlSyncProvider serverProvider = new SqlSyncProvider(GetDatabaseConnectionString("AdventureWorks"));
            SqlSyncProvider clientProvider = new SqlSyncProvider(GetDatabaseConnectionString("Client"));

            // Defining options with Batchsize to enable serialization on disk
            var options = new SyncOptions { BatchSize = 1000 };

            SyncAgent agent = new SyncAgent(clientProvider, serverProvider, options, new string[] {
                "ProductCategory", "ProductModel", "Product", "Address", "Customer", "CustomerAddress", "SalesOrderHeader", "SalesOrderDetail"});

            // Get the orchestrators
            var localOrchestrator = agent.LocalOrchestrator;
            var remoteOrchestrator = agent.RemoteOrchestrator;

            // Encrypting / decrypting data on disk
            localOrchestrator.OnSerializingSet(serializing);
            localOrchestrator.OnDeserializingSet(deserializing);
            remoteOrchestrator.OnSerializingSet(serializing);
            remoteOrchestrator.OnDeserializingSet(deserializing);

            Console.WriteLine(await agent.SynchronizeAsync());

            Console.WriteLine("End");

        }

    }
}
