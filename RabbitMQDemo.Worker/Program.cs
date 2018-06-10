using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;
using System.Threading;

namespace RabbitMQDemo.Worker
{
    class Program
    {
        static void Main(string[] args)
        {
            Guid workerId = Guid.NewGuid();
            Console.WriteLine($"Starting Worker {workerId}");

            var factory = new ConnectionFactory()
            {
                HostName = "192.168.99.101",
                UserName = "test_user",
                Password = "test_user",
                VirtualHost = "test",
                AutomaticRecoveryEnabled = true, // automatically try to recover connection if fails (default 5s wait)
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

                    // send single item to workers only when they are free
                    channel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);

                    Console.WriteLine($" [{workerId}] Waiting for messages.");

                    var consumer = new EventingBasicConsumer(channel);
                    consumer.Received += (model, ea) =>
                    {
                        var body = ea.Body;
                        var message = Encoding.UTF8.GetString(body);
                        Console.WriteLine($" [{workerId}] Received {0}", message);

                        int dots = message.Split('!').Length - 1;
                        Thread.Sleep(dots * 1000);

                        Console.WriteLine($" [{workerId}] finished processing {message}");

                        // send acknowledgement to server to notify finished processing
                        channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                    };
                    // manually send acknowledgements back to the server
                    channel.BasicConsume(queue: "task_queue", autoAck: false, consumer: consumer);

                    Console.WriteLine(" Press [enter] to exit.");
                    Console.ReadLine();
                }
            }

            Console.WriteLine($"Stopped Worker {workerId}.");
        }
    }
}
