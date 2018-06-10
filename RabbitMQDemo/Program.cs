using RabbitMQ.Client;
using System;
using System.Text;

namespace RabbitMQDemo.Client
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Starting Client...");

            //SendSimpleMessage();
            StartQueue();

            Console.WriteLine("Finished Client. Press any key to exit...");
            Console.ReadKey();
        }

        private static void SendSimpleMessage()
        {
            var factory = new ConnectionFactory() { HostName = "192.168.99.101" };
            using (var connection = factory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    channel.QueueDeclare(queue: "hello",
                                 durable: false,
                                 exclusive: false,
                                 autoDelete: false,
                                 arguments: null);

                    string message = "Hello World!";
                    var body = Encoding.UTF8.GetBytes(message);

                    channel.BasicPublish(exchange: "",
                                         routingKey: "hello",
                                         basicProperties: null,
                                         body: body);
                    Console.WriteLine(" [x] Sent {0}", message);
                }
            }
        }

        private static void StartQueue()
        {
            var factory = new ConnectionFactory()
            {
                HostName = "192.168.99.101",
                UserName = "test_user",
                Password = "test_user",
                VirtualHost = "test"
            };

            using (var connection = factory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    channel.QueueDeclare(queue: "task_queue",
                                 durable: true, // persist queue on server failure
                                 exclusive: false,
                                 autoDelete: false,
                                 arguments: null);

                    var properties = channel.CreateBasicProperties();
                    // persist messages on server failure
                    properties.Persistent = true;

                    string message = "Hello World!";
                    var body = Encoding.UTF8.GetBytes(message);
                    SendQueueItem(channel, properties, body);
                    Console.WriteLine(" [x] Sent {0}", message);

                    message = "Hello World!!";
                    body = Encoding.UTF8.GetBytes(message);
                    SendQueueItem(channel, properties, body);
                    Console.WriteLine(" [x] Sent {0}", message);

                    message = "Hello World!!!";
                    body = Encoding.UTF8.GetBytes(message);
                    SendQueueItem(channel, properties, body);
                    Console.WriteLine(" [x] Sent {0}", message);

                    message = "Hello World!!!!";
                    body = Encoding.UTF8.GetBytes(message);
                    SendQueueItem(channel, properties, body);
                    Console.WriteLine(" [x] Sent {0}", message);
                }
            }
        }

        private static void SendQueueItem(IModel channel, IBasicProperties properties, byte[] data)
        {
            channel.BasicPublish(exchange: "",
                                    routingKey: "task_queue",
                                    basicProperties: properties,
                                    body: data);
        }
    }
}
