using System;
using System.Collections.Generic;

namespace QueueProcess
{
    /// <summary>
    /// Class program to test features.
    /// </summary>
    class Program
    {
        static readonly string stringConnectionSas = 
            @"Endpoint=sb://idgordersrct.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=o3poGu91Yvglzl+ofhncCtolPMcyQFFUDwM145J2xQs=";
        static readonly string queueName =
            @"capturenotification";

        static void Main(string[] args)
        {
            //init.
            var msgSender = new MessageSender(stringConnectionSas, queueName);
            var msgReceiver = new MessageReceiver(stringConnectionSas, queueName);

            Console.WriteLine(@"///////**** Sending ****\\\\\\\\");

            //first method.
            Console.WriteLine("First message");
            msgSender.SendMessage("Simple message to test").GetAwaiter().GetResult();

            //second method.
            var listMessage = new List<string>() { "Second message", "Third message" , "Fourth message" };
            Console.WriteLine("List of message");
            msgSender.SendMessageWithinBatch(listMessage).GetAwaiter().GetResult();

            Console.WriteLine(@"///////**** Receiving ****\\\\\\\\");

            //receive the first message sended.
            var body  = msgReceiver.ReceiveMessageAsync().GetAwaiter().GetResult();
            Console.WriteLine($"Received: {body}");

            //receive all message sended in the queue by order.
            msgReceiver.ReceiveAllMessageAsync().GetAwaiter().GetResult();

            Console.WriteLine("Press any key to end the application");
            Console.ReadKey();
        }
    }
}
