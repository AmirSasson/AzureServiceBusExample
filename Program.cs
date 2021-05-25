using Azure.Messaging.ServiceBus;
using Microsoft.Azure.ServiceBus;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace AzureBus.DedupDemo
{
    class Program
    {
        static long receivedMsgCount = 0;
        static long successfulHandleMsgCount = 0;
        static string runId = DateTime.Now.ToString("hhmmss");

        static async Task Main(string[] args)
        {
            var numOfPublishers = 3;
            var numOfMsgPerPublisher = 500;
            var publisherBatchSize = 50;
            int handlersConcurrencyLevel = 5;
            int simulateFailurePercents = 20;
            int numOfHandlers = 3;


            IConfiguration config = new ConfigurationBuilder()
              .AddJsonFile("appsettings.json", false, false)
              .AddJsonFile("appsettings.overrides.json", false, false)
              .Build();
            string connectionString = config.GetValue<string>("ServiceBus:ConnectionString");
            string topicName = config.GetValue<string>("ServiceBus:TopicName");
            string subscriptionName = config.GetValue<string>("ServiceBus:SubscriptionName");

            await using var client = new ServiceBusClient(connectionString, new ServiceBusClientOptions
            {
                RetryOptions = new ServiceBusRetryOptions { Mode = ServiceBusRetryMode.Exponential, Delay = TimeSpan.FromSeconds(5) }
            });
            CancellationTokenSource source = new CancellationTokenSource();
            CancellationToken token = source.Token;

            var publishTasks = new List<Task>();
            for (int i = 0; i < numOfPublishers; i++)
            {
                var t = publishToTopics(connectionString, topicName, numOfMsgPerPublisher, publisherBatchSize, source);
                publishTasks.Add(t);
            }

            for (int i = 0; i < numOfHandlers; i++)
            {
                handleMessages(i.ToString(), connectionString, topicName, subscriptionName, handlersConcurrencyLevel, simulateFailurePercents, source);
            }

            await Task.WhenAll(publishTasks);

            Console.ReadKey();
            log($"[Summary] -receivedMsgCount: {Interlocked.Read(ref receivedMsgCount)}  Handled: {Interlocked.Read(ref successfulHandleMsgCount)}");
            Console.ReadKey();

        }

        private static void log(string msg, params object[] args)
        {
            string argsStr = "";
            if (args != null && args.Count() > 0)
            {
                argsStr = string.Join(" ", args.Select(arg => $"[{arg}]"));
            }
            Console.WriteLine($"{DateTime.Now.ToShortTimeString()} {argsStr} {msg}");
        }
        private static void log(Exception ex)
        {
            log(ex.ToString());
        }
        private static bool handleMessages(string workerId, string connectionString, string topicName, string subscriptionName, int concurrencyLevel, int simulateFailurePercents, CancellationTokenSource source)
        {
            SubscriptionClient subscriptionClient = new SubscriptionClient(
                connectionString: connectionString,
                topicPath: topicName,
                subscriptionName,
                ReceiveMode.PeekLock,
                retryPolicy: new RetryExponential(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(10), 20)
               );

            int successfullyHandled = 0;
            var rand = new Random((int)DateTime.Now.Ticks);

            bool shouldSimulateError()
            {
                return rand.Next(0, 100) <= simulateFailurePercents;
            }

            log($"running listener {workerId}");
            subscriptionClient.RegisterMessageHandler(
                (msg, cancel) =>
                {
                    var str = Encoding.UTF8.GetString(msg.Body);
                    var obj = JsonSerializer.Deserialize<MessageData>(str);
                    if (obj.RunId != runId)
                    {
                        return Task.CompletedTask;
                    }
                    Interlocked.Increment(ref receivedMsgCount);

                    if (shouldSimulateError())
                    {
                        throw new Exception("test");
                    }

                    Interlocked.Increment(ref successfulHandleMsgCount);

                    successfullyHandled++;
                    log($"[{workerId}] - {obj.Name} - retry {msg.SystemProperties.DeliveryCount} -receivedMsgCount: {Interlocked.Read(ref receivedMsgCount)}  Handled: {Interlocked.Read(ref successfulHandleMsgCount)}");
                    return Task.CompletedTask;
                },
                new MessageHandlerOptions(args => { log(args.Exception); return Task.CompletedTask; }) { AutoComplete = true, MaxConcurrentCalls = concurrencyLevel }
            );

            return true;
        }

        private static async Task publishToTopics(string connectionString, string topicName, int numOfMsgPerPublisher, int publisherBatchSize, CancellationTokenSource source)
        {
            // create the sender
            TopicClient topicClient = new TopicClient(connectionString: connectionString, entityPath: topicName);

            List<Message> buffer = new List<Message>();
            for (int i = 0; i < numOfMsgPerPublisher; i++)
            {
                // create a message that we can send. UTF-8 encoding is used when providing a string.
                var id = i + 1;
                Message message = new Message()
                {
                    MessageId = $"{runId}.{id}",
                    ContentType = "application/json",
                    Body = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(new MessageData($"{runId}.{id}", runId)))
                };
                buffer.Add(message);
                if (buffer.Count >= publisherBatchSize)
                {
                    await sendBatch(buffer);

                    await Task.Delay(0);
                    buffer = new List<Message>();
                }
            }

            if (buffer.Any())
            {
                await sendBatch(buffer);
            }

            log("Finished publishing");

            async Task sendBatch(IList<Message> buffer)
            {
                try
                {
                    log($"sending Batch of {buffer.Count}");
                    await topicClient.SendAsync(buffer);
                }
                catch (Exception e)
                {
                    log(e);
                    throw;
                }
            }
        }
    }
    public record MessageData(string Name, string RunId);
}
