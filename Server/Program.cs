using System;
using System.Text;
using System.Threading;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Server
{
    public class Program
    {
        static void Main(string[] args)
        {
            var factory = new ConnectionFactory();

            using (var connection = factory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    // First argument should be the queue or routing key.
                    string input = args.Length > 0 ? args[0] : string.Empty;
                    int sleep = args.Length > 1 ? int.Parse(args[1]) : -1;
                    //startBasicQueueConsumer(channel, input);
                    //startFanoutConsumer(channel, input);
                    //startRoutingConsumer(channel, input);
                    //startTopicConsumer(channel, input);

                    startBasicQueueConsumer(channel, input, true, sleep);
                }
            }
        }

        private static void startBasicQueueConsumer(IModel channel, string queue)
        {
            startBasicQueueConsumer(channel, queue, false, -1);
        }

        private static void startBasicQueueConsumer(IModel channel, string queue, bool requireAcknowledgement, int sleep)
        {
            if (queue == string.Empty)
                queue = "Basic";

            channel.QueueDeclare(queue, false, false, false, null);

            startConsuming(channel, queue, requireAcknowledgement, sleep);
        }

        private static void startFanoutConsumer(IModel channel)
        {
            startFanoutConsumer(channel, string.Empty);
        }

        private static void startFanoutConsumer(IModel channel, string queue)
        {
            const string exchange = "FanoutExample";
            channel.ExchangeDeclare(exchange, "fanout");

            if (queue == string.Empty)
            {
                // Creates a generated, temporary queue.
                queue = channel.QueueDeclare();
            }
            else
            {
                // Creates a named, durable, non auto deleting, non exlusive queue.
                queue = channel.QueueDeclare(queue, true, false, false, null);
            }
            
            channel.QueueBind(queue, exchange, string.Empty);
            Console.WriteLine("Queue: {0}", queue);

            startConsuming(channel, queue);
        }

        private static void startRoutingConsumer(IModel channel, string routingKey)
        {
            const string exchange = "Routing";
            channel.ExchangeDeclare(exchange, "direct");

            string queue = channel.QueueDeclare();
            channel.QueueBind(queue, exchange, routingKey);

            startConsuming(channel, queue);
        }

        private static void startTopicConsumer(IModel channel, string topic)
        {
            const string exchange = "Topic";
            channel.ExchangeDeclare(exchange, "topic");

            string queue = channel.QueueDeclare();
            channel.QueueBind(queue, exchange, topic);

            startConsuming(channel, queue   );
        }

        private static void startConsuming(IModel channel, string queue)
        {
            startConsuming(channel, queue, false, -1);
        }

        private static void startConsuming(IModel channel, string queue, bool requireAcknowledgement, int sleep)
        {
            var consumer = new QueueingBasicConsumer(channel);
            channel.BasicConsume(queue, !requireAcknowledgement, consumer);

            Console.WriteLine("Waiting for messages.To exit press CTRL+C");

            int receivedCount = 0;

            while (true)
            {
                var eventArgs = consumer.Queue.Dequeue();

                var message = Encoding.UTF8.GetString(eventArgs.Body);
                Console.WriteLine("Received Message [{1}]: {0}", message, ++receivedCount);

                if (requireAcknowledgement)
                {
                    if (sleep > 0)
                        Thread.Sleep(sleep * 1000);
                    channel.BasicAck(eventArgs.DeliveryTag, false);
                }
            }
        }
    }
}
