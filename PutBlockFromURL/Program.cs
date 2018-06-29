using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace PutBlockFromURL
{
    class Program
    {
        static String account1_connectionstring = "<add your connection string for the source account>";
        static String account2_connectionstring = "<add your connection string for the dest account>";
        static String container = "mycontainer";
        static String blob = "myfile";

        static void Main(string[] args) {
            // Time the copy operation
            Stopwatch stopWatch = new Stopwatch();
            stopWatch.Start();

            // MainAsync is the static method that calls async Storage APIs
            MainAsync(args).GetAwaiter().GetResult();

            // Stop the watch and print
            stopWatch.Stop();
            Console.WriteLine("Finished the copy in " + stopWatch.Elapsed);

            Console.WriteLine("Hit any key to exit ");
            Console.ReadKey();

        }
        static async Task MainAsync(string[] args)
        {
            // Copy a blob from account1 to account2 using Put Block From URL
            CloudStorageAccount account1 = CloudStorageAccount.Parse(account1_connectionstring);
            CloudStorageAccount account2 = CloudStorageAccount.Parse(account2_connectionstring);
            CloudBlobClient client1 = account1.CreateCloudBlobClient();
            CloudBlobClient client2 = account2.CreateCloudBlobClient();

            CloudBlobContainer container1 = client1.GetContainerReference(container);
            CloudBlobContainer container2 = client2.GetContainerReference(container);

            // Generate a read only SAS token valid for 1 hour 
            // This will be used as a source for Put Block From URL
            // Append the SAS token to the source
            CloudBlockBlob blob1 = container1.GetBlockBlobReference(blob);
            SharedAccessBlobPolicy policy = new SharedAccessBlobPolicy();
            policy.SharedAccessExpiryTime = DateTime.Now.AddHours(1);
            policy.SharedAccessStartTime = DateTime.Now.AddSeconds(-10);
            policy.Permissions = SharedAccessBlobPermissions.Read;
            String SAS = blob1.GetSharedAccessSignature(policy);
            String full_uri = blob1.Uri.ToString() + SAS;

            // Get a reference to the destination blob
            CloudBlockBlob blob2 = container2.GetBlockBlobReference(blob+"_destination");

            // Get Blob Properties to find out the length of the source blob
            await blob1.FetchAttributesAsync();

            // Based on 100MB blocks, let's figure out the number of blocks required to copy the data
            // We will call (blocks) times the PutBlockFromURL API 
            int block_size = 100 * 1024 * 1024;
            long blocks = (blob1.Properties.Length + block_size - 1 )/ block_size;

            try
            {
                // Get a block id List
                List<string> destBlockList = GetBlockIdList((int)blocks);

                // Call PutBlock multiple times
                // Alternatively do not await, and add all calls into a List<Task>() and then call WhenAll to concurrently copy all
                for (int i = 0; i < blocks; i++)
                {
                    Console.WriteLine("Putting block " + i + " with offset " + (long)i * (long)block_size);
                    await blob2.PutBlockAsync(destBlockList[i], new Uri(full_uri), (long)i * (long)block_size, block_size, null);
                }

                // Let's now PutBlockList to commit all Blocks in the destBlockList
                await blob2.PutBlockListAsync(destBlockList);
            }
            catch (StorageException ex) {
                Console.WriteLine(ex.RequestInformation.HttpStatusCode);
                Console.WriteLine(ex.Message);
            }

        }

        public static List<string> GetBlockIdList(int count)
        {
            List<string> blocks = new List<string>();
            for (int i = 0; i < count; i++)
            {
                blocks.Add(Convert.ToBase64String(Guid.NewGuid().ToByteArray()));
            }
            return blocks;
        }
    }
}
