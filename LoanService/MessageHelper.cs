using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Queue;
using Microsoft.Extensions.Logging;

public static class MessageHelper {
    public static async Task QueueMessageAsync(string queueName, Message message, ILogger log)
    {
        await using var client = new ServiceBusClient(Environment.GetEnvironmentVariable("rasputinServicebus"));
        ServiceBusSender sender = client.CreateSender(queueName);
        UpdateCurrentQueueHeader(message, queueName);
        string queueMessage = JsonSerializer.Serialize(message, new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase
            });            
        var messageBytes = Encoding.UTF8.GetBytes(queueMessage);
        ServiceBusMessage messageObject = new ServiceBusMessage(messageBytes);

        CancellationTokenSource cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        CancellationToken cancellationToken = cancellationTokenSource.Token;

        await sender.SendMessageAsync(messageObject, cancellationToken);
    }

    private static void UpdateCurrentQueueHeader(Message message, string queueName)
    {
        var current = message.Headers.FirstOrDefault(x => x.Name.Equals("current-queue-header"));
        current.Fields["Name"] = queueName;
        current.Fields["Timestamp"] = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffZ");
    }

    public static async Task SendLog(Message message)
        {
            var idHeader = message.Headers.FirstOrDefault(x => x.Name.Equals("id-header"));
            var current = message.Headers.FirstOrDefault(x => x.Name.Equals("current-queue-header"));
            LogTimer logTimer = new LogTimer() {
                Id = Guid.Parse(idHeader.Fields["GUID"]),
                Queue = current.Fields["Name"],
                SentTimestamp = DateTime.ParseExact(current.Fields["Timestamp"], "yyyy-MM-ddTHH:mm:ss.fffZ", CultureInfo.InvariantCulture),
                ReceiveTimestamp = DateTime.UtcNow
            };
            await using var client = new ServiceBusClient(Environment.GetEnvironmentVariable("rasputinServicebus"));
            ServiceBusSender sender = client.CreateSender("ms-logtimer");
            string queueMessage = JsonSerializer.Serialize(logTimer, new JsonSerializerOptions
                {
                    PropertyNamingPolicy = JsonNamingPolicy.CamelCase
                });            
            var messageBytes = Encoding.UTF8.GetBytes(queueMessage);
            ServiceBusMessage messageObject = new ServiceBusMessage(messageBytes);

            CancellationTokenSource cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(20));
            CancellationToken cancellationToken = cancellationTokenSource.Token;

            await sender.SendMessageAsync(messageObject, cancellationToken);
            await sender.CloseAsync();
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