using System;
using System.Text;
using RabbitMQ.Client;

namespace Sender
{
    class Program
    {
        static void Main(string[] args)
        {
            var factory = new ConnectionFactory();
            using (var connection = factory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    //sendToFanoutExchange(channel);
                    //sendBasicToQueue(channel);
                    //sendToRoutingExchange(channel);
                    sendToTopicExchange(channel);
                }
            }
        }

        private static void sendBasicToQueue(IModel channel)
        {
            Console.WriteLine("Basic Queue Sender");
            const string queue = "Basic";

            byte[] body = Encoding.UTF8.GetBytes("Test");

            channel.QueueDeclare(queue, false, false, false, null);

            while (true)
            {
                // Uses the default, direct exchange to publish a message to a given queue.
                channel.BasicPublish(string.Empty, queue, null, body);
                Console.WriteLine("Message Sent.");
                Console.ReadLine();
            }
        }

        private static void sendToFanoutExchange(IModel channel)
        {
            Console.WriteLine("FanoutExchange Sender");
            const string exchange = "FanoutExample";

            byte[] body = Encoding.UTF8.GetBytes("Fanout Message");

            channel.ExchangeDeclare(exchange, "fanout");

            while (true)
            {
                channel.BasicPublish(exchange, string.Empty, null, body);
                Console.WriteLine("Message Sent.");
                Console.ReadLine();
            }
        }

        private static void sendToRoutingExchange(IModel channel)
        {
            Console.WriteLine("Routing Sender");
            const string exchange = "Routing";

            byte[] body = Encoding.UTF8.GetBytes("Routed Message");
            channel.ExchangeDeclare(exchange, "direct");

            while (true)
            {
                Console.Write("Routing Key: ");
                string routingKey = Console.ReadLine();
                channel.BasicPublish(exchange, routingKey, null, body);
                Console.WriteLine("Message Sent.");
            }
        }

        private static void sendToTopicExchange(IModel channel)
        {
            Console.WriteLine("Topic Sender");
            const string exchange = "Topic";

            byte[] body = Encoding.UTF8.GetBytes("Topic Message");
            channel.ExchangeDeclare(exchange, "topic");

            while (true)
            {
                Console.Write("Topic: ");
                string topic = Console.ReadLine();
                channel.BasicPublish(exchange, topic, null, body);
                Console.WriteLine("Message Sent.");
            }
        }
    }
}
