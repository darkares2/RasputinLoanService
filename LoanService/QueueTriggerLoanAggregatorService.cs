using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;

namespace LoanService
{
    public class QueueTriggerLoanAggregatorService
    {
        private class AggregatedEntry {
            public DateTime entryTime { get; set; }
            public string body { get; set; }
            public Message message { get; set; }
        }
        private static readonly Dictionary<string, AggregatedEntry> aggregatedMessages = new Dictionary<string, AggregatedEntry>();

        [FunctionName("QueueTriggerLoanAggregatorService")]
        public async Task RunAsync([ServiceBusTrigger("ms-loans-aggregator", Connection = "rasputinServicebus")]string myQueueItem, ILogger log)
        {
            log.LogInformation($"ms-loans-aggregator triggered: {myQueueItem}");
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            DateTime receivedMessageTime = DateTime.UtcNow;

            var message = JsonSerializer.Deserialize<Message>(myQueueItem, new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase
            });
            var logMessage = new Message();
            try {
                List<MessageHeader> headers = new List<MessageHeader>();
                headers.Add(new MessageHeader() { Name = "id-header", Fields = new Dictionary<string, string>() { { "GUID", message.Headers.FirstOrDefault(x => x.Name.Equals("id-header")).Fields["GUID"] } } });
                headers.Add(new MessageHeader() { Name = "current-queue-header", Fields = new Dictionary<string, string>() { { "Name", message.Headers.FirstOrDefault(x => x.Name.Equals("current-queue-header")).Fields["Name"] }, { "Timestamp", message.Headers.FirstOrDefault(x => x.Name.Equals("current-queue-header")).Fields["Timestamp"] } } });
                logMessage.Headers = headers.ToArray();

                await BuildAggregatedReplyAsync(message, log);
                stopwatch.Stop();
                await MessageHelper.SendLog(logMessage, receivedMessageTime, stopwatch.ElapsedMilliseconds);
            } catch(Exception ex) {
                log.LogError("Processing failed", ex);
                var current = logMessage.Headers.FirstOrDefault(x => x.Name.Equals("current-queue-header"));
                current.Fields["Name"] = current.Fields["Name"] + $"-Error (LoanAggregator): {ex.Message}";
                stopwatch.Stop();
                await MessageHelper.SendLog(logMessage, receivedMessageTime, stopwatch.ElapsedMilliseconds);
            }
        }

        private async Task BuildAggregatedReplyAsync(Message message, ILogger log)
        {
            var content = GetMessageContent(message);
            var id = GetMessageId(message);

            var key = $"{id}-{content}";
            log.LogInformation($"Got aggregate key: {key}");

            if (aggregatedMessages.ContainsKey(key))
            {
                aggregatedMessages[key].entryTime = DateTime.Now;
                aggregatedMessages[key].body = message.Body;
                aggregatedMessages[key].message = message;
            }
            else
            {
                aggregatedMessages.Add(key, new AggregatedEntry
                {
                    entryTime = DateTime.Now,
                    body = message.Body,
                    message = message
                });
            }
            await HandleLoansHistoryAggregationAsync(id, log);
        }

        private async Task HandleLoansHistoryAggregationAsync(string id, ILogger log)
        {
            if (aggregatedMessages.ContainsKey($"{id}-Loans-history") && 
                aggregatedMessages.ContainsKey($"{id}-Books") &&
                aggregatedMessages.ContainsKey($"{id}-Users")) {
                log.LogInformation($"Got all messages for id: {id}");

                var books = JsonSerializer.Deserialize<Books[]>(aggregatedMessages[$"{id}-Books"].body, new JsonSerializerOptions
                {
                    PropertyNamingPolicy = JsonNamingPolicy.CamelCase
                });
                var history = new List<LoanHistory.Entry>();
                var loans = JsonSerializer.Deserialize<Loans[]>(aggregatedMessages[$"{id}-Loans-history"].body, new JsonSerializerOptions
                {
                    PropertyNamingPolicy = JsonNamingPolicy.CamelCase
                });
                var users = JsonSerializer.Deserialize<Users[]>(aggregatedMessages[$"{id}-Users"].body, new JsonSerializerOptions
                {
                    PropertyNamingPolicy = JsonNamingPolicy.CamelCase
                });
                foreach (var loan in loans)
                {
                    var user = users.Where(x => x.Id == loan.UserId).FirstOrDefault();
                    history.Add(new LoanHistory.Entry
                    {
                        LoanTimestamp = loan.LoanTimestamp,
                        Active = loan.Active,
                        UserId = loan.UserId,
                        Username = user.Username
                    });
                }
                var loanHistory = new LoanHistory() {
                    ISBN = books[0].ISBN,
                    Title = books[0].Title,
                    Author = books[0].Author,
                    PublicationDate = books[0].PublicationDate,
                    Price = books[0].Price,
                    History = history.ToArray()
                };
                var message = new Message
                {
                    Headers = aggregatedMessages[$"{id}-Loans-history"].message.Headers,
                    Body = JsonSerializer.Serialize(loanHistory, new JsonSerializerOptions
                    {
                        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
                    })
                };
                MessageHelper.AddContentHeader(message, "Loans-history-aggregated");

                await MessageHelper.QueueMessageAsync("api-router", message, log);

            }
        }

        private string GetMessageId(Message message)
        {
            return message.Headers.Where(x => x.Name == "id-header").Select(x => x.Fields["GUID"]).FirstOrDefault();
        }

        private string GetMessageContent(Message message)
        {
            return message.Headers.Where(x => x.Name == "content-header").Select(x => x.Fields["Content"]).FirstOrDefault();
        }
    }
}
