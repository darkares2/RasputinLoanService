using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Queue;
using Microsoft.Extensions.Logging;

public static class MessageHelper {
        public static async Task QueueMessageAsync(string queueName, Message message, ILogger log)
        {
            // Get a reference to the queue
            var str = Environment.GetEnvironmentVariable("rasputinstorageaccount_STORAGE");
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(str);
            CloudQueueClient queueClient = storageAccount.CreateCloudQueueClient();
            CloudQueue queue = queueClient.GetQueueReference(queueName);

            // Create a new message and add it to the queue
            CloudQueueMessage queueMessage = new CloudQueueMessage(JsonSerializer.Serialize(message, new JsonSerializerOptions
                {
                    PropertyNamingPolicy = JsonNamingPolicy.CamelCase
                })
                );
            await queue.AddMessageAsync(queueMessage);
        }

        public static void AddContentHeader(Message message, string content)
        {
            // Insert content header before route header
            var headers = new List<MessageHeader>(message.Headers);
            // Remove existing content header
            headers.RemoveAll(h => h.Name == "content-header");
            var index = headers.FindIndex(h => h.Name == "route-header");
            headers.Insert(index, new MessageHeader() { Name = "content-header", Fields = new Dictionary<string, string>() { { "Content", content } } });
            message.Headers = headers.ToArray();
        }


}