using System;
using System.Text;
using Confluent.Kafka;

class KafkaProducer
{
    public static void Main(string[] args)
    {
        // Setup for vietnamese utf-8
        Console.InputEncoding = Encoding.UTF8;
        Console.OutputEncoding = Encoding.UTF8;

        var config = new ProducerConfig { BootstrapServers = "localhost:9092" };

        using var producer = new ProducerBuilder<Null, string>(config).Build();
        Console.WriteLine("Nhập thông điệp (gõ 'exit' để thoát):");
        string message;
        while ((message = Console.ReadLine()) != "exit")
        {
            producer.Produce("my-topic", new Message<Null, string> { Value = message });
            Console.WriteLine($"Đã gửi: {message}");
        }

        producer.Flush(TimeSpan.FromSeconds(10));
    }
}
