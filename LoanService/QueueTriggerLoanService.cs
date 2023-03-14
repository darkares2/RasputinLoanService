using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Queue;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace LoanService
{
    public class QueueTriggerLoanService
    {
        [FunctionName("QueueTriggerLoanService")]
        public async Task RunAsync([QueueTrigger("ms-loans", Connection = "rasputinstorageaccount_STORAGE")]string myQueueItem, ILogger log)
        {
            log.LogInformation($"C# Queue trigger function processed: {myQueueItem}");
            var message = JsonSerializer.Deserialize<Message>(myQueueItem, new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase
            });
            var cmd = JsonSerializer.Deserialize<CmdLoan>(message.Body, new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase
            });
            var loan = cmd.Loan;
            if (cmd.Command == "loan")
            {
                await InsertLoanAsync(message, loan, log);
            } else if (cmd.Command == "return")
            {
                await UpdateLoanAsync(message, loan, log);
            } else if (cmd.Command == "list_active_books_user")
            {
                await ListActiveUserLoansAsync(message, cmd.Parameter, log);
            } else {
                log.LogError($"Command {cmd.Command} not supported");
            }

        }

        private async Task UpdateLoanAsync(Message receivedMessage, Loans loan, ILogger log)
        {
            var connectionString = Environment.GetEnvironmentVariable("sqldb_connection");
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                string query = "UPDATE BookUserLink SET Active = @active WHERE ISBN = @isbn AND UserId = @userId";

                // Create a new SqlCommand object with the query and the SqlConnection object
                SqlCommand command = new SqlCommand(query, connection);

                // Add parameters to the SqlCommand object
                command.Parameters.AddWithValue("@active", false);
                command.Parameters.AddWithValue("@isbn", loan.ISBN);
                command.Parameters.AddWithValue("@userId", loan.UserId);

                connection.Open();

                // Execute the SQL query
                int rowsAffected = await command.ExecuteNonQueryAsync();

                // Output the number of rows affected by the query
                log.LogInformation($"Rows affected: {rowsAffected}");
            }
            var message = new Message
            {
                Headers = receivedMessage.Headers,
                Body = JsonSerializer.Serialize(loan, new JsonSerializerOptions
                {
                    PropertyNamingPolicy = JsonNamingPolicy.CamelCase
                })
            };
            await QueueMessageAsync("api-router", message, log);
        }

        private async Task ListActiveUserLoansAsync(Message receivedMessage, string parameter, ILogger log)
        {
            List<Loans> loans = new List<Loans>();
            var str = Environment.GetEnvironmentVariable("sqldb_connection");
            string query = "SELECT * FROM BookUserLink WHERE Active = 1";
            if (parameter != null)
            {
                query += " AND UserId IN (";
                var ids = parameter.Split(',');
                bool first = true;
                for (int i = 0; i < ids.Length; i++)
                {
                    query += (first ? "":",") + "@Id" + i;
                    first = false;
                }
                query += ")";
            }
            using (SqlConnection connection = new SqlConnection(str))
            {
                connection.Open();
                using (SqlCommand command = new SqlCommand(query, connection)) {
                    if (parameter != null)
                    {
                        var ids = parameter.Split(',');
                        for (int i = 0; i < ids.Length; i++)
                        {
                            command.Parameters.AddWithValue("@Id" + i, ids[i]);
                        }
                    }
                    using (SqlDataReader reader = await command.ExecuteReaderAsync()) {
                        while (reader.Read())
                        {
                            var loan = new Loans
                            {
                                ISBN = reader.GetString(0),
                                UserId = reader.GetInt32(1),
                                LoanTimestamp = reader.GetDateTime(2),
                                Active = reader.GetBoolean(3)
                            };
                            loans.Add(loan);   
                        }
                    }
                }
            }
            var message = new Message
            {
                Headers = receivedMessage.Headers,
                Body = JsonSerializer.Serialize(loans, new JsonSerializerOptions
                {
                    PropertyNamingPolicy = JsonNamingPolicy.CamelCase
                })
            };
            await QueueMessageAsync("api-router", message, log);
        }

        private async Task InsertLoanAsync(Message receivedMessage, Loans loan, ILogger log)
        {
            var connectionString = Environment.GetEnvironmentVariable("sqldb_connection");
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                string query = "INSERT INTO BookUserLink (ISBN, UserId, LoanTimestamp, Active) VALUES (@isbn, @userId, @loanTimestamp, @active)";

                SqlCommand command = new SqlCommand(query, connection);

                // Add parameters to the SqlCommand object
                command.Parameters.AddWithValue("@isbn", loan.ISBN);
                command.Parameters.AddWithValue("@userId", loan.UserId);
                command.Parameters.AddWithValue("@loanTimestamp", loan.LoanTimestamp);
                command.Parameters.AddWithValue("@active", loan.Active);

                connection.Open();

                // Execute the SQL query
                int rowsAffected = await command.ExecuteNonQueryAsync();

                // Output the number of rows affected by the query
                log.LogInformation($"Rows affected: {rowsAffected}");
            }
            var message = new Message
            {
                Headers = receivedMessage.Headers,
                Body = JsonSerializer.Serialize(loan, new JsonSerializerOptions
                {
                    PropertyNamingPolicy = JsonNamingPolicy.CamelCase
                })
            };
            await QueueMessageAsync("api-router", message, log);
        }

        private async Task QueueMessageAsync(string queueName, Message message, ILogger log)
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
    }
}
