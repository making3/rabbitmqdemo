using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Demo
{
    public class BasicExample
    {
        private const string _queue = "basic";

        public static void RunBasic() 
        {
            // Initialize a new thread for the consumer.
            new Thread(basicConsumer).Start();

            Console.WriteLine("Press enter to send a new message.");

            while (true) 
            {
                Console.ReadLine();
                basicSender();
                Console.WriteLine();
            }
        }

        public static void RunWorkQueue() 
        {
            // Initialize a new thread for three work consumer.
            new Thread(() => basicConsumer(1)).Start();
            new Thread(() => basicConsumer(2)).Start();
            new Thread(() => basicConsumer(3)).Start();

            Console.WriteLine("Press enter to send a new message.");

            while (true) 
            {
                Console.ReadLine();
                basicSender();
                Console.WriteLine();
            }
        }

        private static void basicSender()
        {
            var factory = new ConnectionFactory() { HostName = Program.HostName };

            using (var connection = factory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    byte[] body = Encoding.UTF8.GetBytes("Test");

                    channel.QueueDeclare(_queue, false, false, false, null);
                    channel.BasicPublish(string.Empty, _queue, null, body);
                    Console.WriteLine("Sent Message.");
                }
            }
        }

        private static void basicConsumer()
        {
            basicConsumer(0);
        }

        private static void basicConsumer(int workerId)
        {
            int receivedCount = 0;
            var factory = new ConnectionFactory() { HostName = Program.HostName };

            using (var connection = factory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    channel.QueueDeclare(_queue, false, false, false, null);

                    var consumer = new QueueingBasicConsumer(channel);
                    channel.BasicConsume(_queue, true, consumer);

                    Console.WriteLine("Waiting for messages.To exit press CTRL+C");

                    while (true)
                    {
                        var ea = (BasicDeliverEventArgs)consumer.Queue.Dequeue();

                        var message = Encoding.UTF8.GetString(ea.Body);
                        Console.WriteLine("[{2}] Received Message [{1}]: {0}", message, ++receivedCount, workerId);
                    }
                }
            }
        }
    }
}
