using Azure.Messaging.ServiceBus;
using System;
using System.Threading.Tasks;

namespace QueueProcess
{
    /// <summary>
    /// Class of message receiver/consumer adapter.
    /// </summary>
    public class MessageReceiver
    {
        private readonly string _stringConnectionSas;
        private readonly string _queueName;

        /// <summary>
        /// Ctor.
        /// </summary>
        /// <param name="connectionString"></param>
        /// <param name="queueName"></param>
        public MessageReceiver(string connectionString, string queueName)
        {
            _stringConnectionSas = connectionString;
            _queueName = queueName;
        }

        #region public methods

        /// <summary>
        /// Receive the first message in the queue.
        /// </summary>
        /// <returns></returns>
        public async Task<string> ReceiveMessageAsync()
        {
            var clientOptions = new ServiceBusClientOptions()
            {
                TransportType = ServiceBusTransportType.AmqpWebSockets
            };
            //client = new ServiceBusClient(stringConnectionSas, clientOptions);
            // since ServiceBusClient implements IAsyncDisposable we create it with "await using"
            await using var client = new ServiceBusClient(_stringConnectionSas, clientOptions);

            // create a receiver that we can use to receive the message
            ServiceBusReceiver receiver = client.CreateReceiver(_queueName);

            // the received message is a different type as it contains some service set properties
            ServiceBusReceivedMessage receivedMessage = await receiver.ReceiveMessageAsync();

            // get the message body as a string
            string body = receivedMessage.Body.ToString();

            return body;
        }

        /// <summary>
        /// Receive all message by order from the queue.
        /// </summary>
        /// <returns></returns>
        public async Task ReceiveAllMessageAsync()
        {

            // the client that owns the connection and can be used to create senders and receivers
            ServiceBusClient client;

            // the processor that reads and processes messages from the queue
            ServiceBusProcessor processor;
            // The Service Bus client types are safe to cache and use as a singleton for the lifetime
            // of the application, which is best practice when messages are being published or read
            // regularly.
            //
            // Set the transport type to AmqpWebSockets so that the ServiceBusClient uses port 443. 
            // If you use the default AmqpTcp, make sure that ports 5671 and 5672 are open.



            var clientOptions = new ServiceBusClientOptions()
            {
                TransportType = ServiceBusTransportType.AmqpWebSockets
            };
            client = new ServiceBusClient(_stringConnectionSas, clientOptions);

            // create a processor that we can use to process the messages
            processor = client.CreateProcessor(_queueName, new ServiceBusProcessorOptions());

            try
            {
                // add handler to process messages
                processor.ProcessMessageAsync += MessageHandler;

                // add handler to process any errors
                processor.ProcessErrorAsync += ErrorHandler;

                // start processing 
                await processor.StartProcessingAsync();

                Console.WriteLine("Wait for a minute and then press any key to end the processing");
                Console.ReadKey();

                // stop processing 
                Console.WriteLine("\nStopping the receiver...");
                await processor.StopProcessingAsync();
                Console.WriteLine("Stopped receiving messages");
            }
            finally
            {
                // Calling DisposeAsync on client types is required to ensure that network
                // resources and other unmanaged objects are properly cleaned up.
                await processor.DisposeAsync();
                await client.DisposeAsync();
            }
        }
        #endregion

        #region private methods

        // handle received messages
        private static async Task MessageHandler(ProcessMessageEventArgs args)
        {
            string body = args.Message.Body.ToString();
            Console.WriteLine($"Received: {body}");

            // complete the message. message is deleted from the queue. 
            await args.CompleteMessageAsync(args.Message);
        }

        // handle any errors when receiving messages
        private static Task ErrorHandler(ProcessErrorEventArgs args)
        {
            Console.WriteLine(args.Exception.ToString());
            return Task.CompletedTask;
        }

        #endregion


    }
}
