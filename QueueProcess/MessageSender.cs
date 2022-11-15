using Azure.Messaging.ServiceBus;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace QueueProcess
{
    /// <summary>
    /// Class of message sender/producer adapter.
    /// </summary>
    public class MessageSender
    {
        private readonly string _stringConnectionSas;
        private readonly string _queueName;

        /// <summary>
        /// Ctor.
        /// </summary>
        /// <param name="connectionString"></param>
        /// <param name="queueName"></param>
        public MessageSender(string connectionString, string queueName)
        {
            _stringConnectionSas = connectionString;
            _queueName = queueName;
        }

        #region public methods

        /// <summary>
        /// Send message to queue sipmly.
        /// </summary>
        /// <param name="messageToSend"></param>
        /// <returns></returns>
        public async Task SendMessage(string messageToSend)
        {
            var clientOptions = new ServiceBusClientOptions()
            {
                TransportType = ServiceBusTransportType.AmqpWebSockets
            };
            //client = new ServiceBusClient(stringConnectionSas, clientOptions);
            // since ServiceBusClient implements IAsyncDisposable we create it with "await using"
            await using var client = new ServiceBusClient(_stringConnectionSas, clientOptions);
            // create the sender
            ServiceBusSender sender = client.CreateSender(_queueName);

            // create a message that we can send. UTF-8 encoding is used when providing a string.
            ServiceBusMessage message = new ServiceBusMessage(messageToSend);

            // send the message
            await sender.SendMessageAsync(message);
        }

        /// <summary>
        /// Send message object to queue sipmly.
        /// </summary>
        /// <param name="messageObjToSend"></param>
        /// <returns></returns>
        public async Task SendMessage<T>(T messageObjToSend)
        {
            var clientOptions = new ServiceBusClientOptions()
            {
                TransportType = ServiceBusTransportType.AmqpWebSockets
            };
            //client = new ServiceBusClient(stringConnectionSas, clientOptions);
            // since ServiceBusClient implements IAsyncDisposable we create it with "await using"
            await using var client = new ServiceBusClient(_stringConnectionSas, clientOptions);
            // create the sender
            ServiceBusSender sender = client.CreateSender(_queueName);

            // create a message that we can send. UTF-8 encoding is used when providing a string.
            var messageToSend = JsonConvert.SerializeObject(messageObjToSend);
            ServiceBusMessage message = new ServiceBusMessage(messageToSend);

            // send the message
            await sender.SendMessageAsync(message);
        }

        /// <summary>
        /// Send list of messages to the queue using MessageBatch.
        /// </summary>
        /// <param name="messagesToSend"></param>
        /// <returns></returns>
        public async Task SendMessageWithinBatch(List<string> messagesToSend)
        {
            // the client that owns the connection and can be used to create senders and receivers
            ServiceBusClient client;

            // the sender used to publish messages to the queue
            ServiceBusSender sender;

            // The Service Bus client types are safe to cache and use as a singleton for the lifetime
            // of the application, which is best practice when messages are being published or read
            // regularly.
            //
            // set the transport type to AmqpWebSockets so that the ServiceBusClient uses the port 443. 
            // If you use the default AmqpTcp, you will need to make sure that the ports 5671 and 5672 are open

            var clientOptions = new ServiceBusClientOptions()
            {
                TransportType = ServiceBusTransportType.AmqpWebSockets
            };
            client = new ServiceBusClient(_stringConnectionSas, clientOptions);
            sender = client.CreateSender(_queueName);

            // create a batch 
            using ServiceBusMessageBatch messageBatch = await sender.CreateMessageBatchAsync();

            // try adding messages to the batch
            foreach(var messageObjToSend in messagesToSend)
            {
                var messageToSend = JsonConvert.SerializeObject(messageObjToSend);

                if (!messageBatch.TryAddMessage(new ServiceBusMessage(messageToSend)))
                {
                    // if it is too large for the batch
                    throw new Exception($"The message {messagesToSend.IndexOf(messageObjToSend)} is too large to fit in the batch.");
                }
            }

            try
            {
                // Use the producer client to send the batch of messages to the Service Bus queue
                await sender.SendMessagesAsync(messageBatch);
            }
            finally
            {
                // Calling DisposeAsync on client types is required to ensure that network
                // resources and other unmanaged objects are properly cleaned up.
                await sender.DisposeAsync();
                await client.DisposeAsync();
            }
        }

        /// <summary>
        /// Send list of messages to the queue using MessageBatch.
        /// </summary>
        /// <param name="messagesObjToSend"></param>
        /// <returns></returns>
        public async Task SendMessageWithinBatch<T>(List<T> messagesObjToSend)
        {
            // the client that owns the connection and can be used to create senders and receivers
            ServiceBusClient client;

            // the sender used to publish messages to the queue
            ServiceBusSender sender;

            // The Service Bus client types are safe to cache and use as a singleton for the lifetime
            // of the application, which is best practice when messages are being published or read
            // regularly.
            //
            // set the transport type to AmqpWebSockets so that the ServiceBusClient uses the port 443. 
            // If you use the default AmqpTcp, you will need to make sure that the ports 5671 and 5672 are open

            var clientOptions = new ServiceBusClientOptions()
            {
                TransportType = ServiceBusTransportType.AmqpWebSockets
            };
            client = new ServiceBusClient(_stringConnectionSas, clientOptions);
            sender = client.CreateSender(_queueName);

            // create a batch 
            using ServiceBusMessageBatch messageBatch = await sender.CreateMessageBatchAsync();

            // try adding messages to the batch
            foreach (var messageObjToSend in messagesObjToSend)
            {
                var messageToSend = JsonConvert.SerializeObject(messageObjToSend);

                if (!messageBatch.TryAddMessage(new ServiceBusMessage(messageToSend)))
                {
                    // if it is too large for the batch
                    throw new Exception($"The message {messagesObjToSend.IndexOf(messageObjToSend)} is too large to fit in the batch.");
                }
            }

            try
            {
                // Use the producer client to send the batch of messages to the Service Bus queue
                await sender.SendMessagesAsync(messageBatch);
            }
            finally
            {
                // Calling DisposeAsync on client types is required to ensure that network
                // resources and other unmanaged objects are properly cleaned up.
                await sender.DisposeAsync();
                await client.DisposeAsync();
            }
        }

        #endregion

    }
}
