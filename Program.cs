using Confluent.Kafka;
using System;
using System.IO;
using System.Net.Http;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace WikiEditStream
{
    class Program
    {
        static async Task Main(string[] args)
        {
            // Configure the client with credentials for connecting to Confluent.
            // Don't do this in production code. For more information, see 
            // https://docs.microsoft.com/en-us/aspnet/core/security/app-secrets.
            var clientConfig = new ClientConfig();
            clientConfig.BootstrapServers="<bootstrap-host-port-pair>";
            clientConfig.SecurityProtocol=Confluent.Kafka.SecurityProtocol.SaslSsl;
            clientConfig.SaslMechanism=Confluent.Kafka.SaslMechanism.Plain;
            clientConfig.SaslUsername="<api-key>";
            clientConfig.SaslPassword="<api-secret>";
            clientConfig.SslCaLocation = "probe"; // /etc/ssl/certs

            await Produce("recent_changes", clientConfig);
            //Consume("recent_changes", clientConfig);

            // To demonstrate cross-platform production and consumption,
            // comment out the previous calls to Produce and Consume, and 
            // uncomment the following lines.

            // var platform = System.Environment.OSVersion.Platform; // Unix, Win32NT, or Other
            // var version = System.Environment.OSVersion.VersionString;
            // Console.WriteLine($"Running on {version}");

            // if(platform == System.PlatformID.Unix)
            // {
            //     Produce("recent_changes", clientConfig);
            //     //Consume("recent_changes", clientConfig);
            //     //Consume("pksqlc-gnponEDITS_PER_PAGE", clientConfig);
            // }
            // else if(platform == System.PlatformID.Win32NT) 
            // {
            //     Consume("recent_changes", clientConfig);
            //     //Produce("recent_changes", clientConfig);
            // }

            Console.WriteLine("Exiting");
        }

        // Produce recent-change messages from Wikipedia to a Kafka topic.
        // The messages are sent from the RCFeed https://www.mediawiki.org/wiki/Manual:RCFeed
        // to the topic with the specified name. 
        static async Task Produce(string topicName, ClientConfig config)
        {
            Console.WriteLine($"{nameof(Produce)} starting");

            // The URL of the EventStreams service.
            string eventStreamsUrl = "https://stream.wikimedia.org/v2/stream/recentchange";

            // Declare the producer reference here to enable calling the Flush
            // method in the finally block, when the app shuts down.
            IProducer<string, string> producer = null;

            try
            {
                // Build a producer based on the provided configuration.
                // It will be disposed in the finally block.
                producer = new ProducerBuilder<string, string>(config).Build();

                using(var httpClient = new HttpClient())

                using (var stream = await httpClient.GetStreamAsync(eventStreamsUrl))
                                
                using (var reader = new StreamReader(stream))
                {
                    // Read continuously until interrupted by Ctrl+C.
                    while (!reader.EndOfStream)
                    {
                        var line = reader.ReadLine();

                        // The Wikimedia service sends a few lines, but the lines
                        // of interest for this demo start with the "data:" prefix. 
                        if(!line.StartsWith("data:"))
                        {
                            continue;
                        }

                        // Extract and deserialize the JSON payload.
                        int openBraceIndex = line.IndexOf('{');
                        string jsonData = line.Substring(openBraceIndex);
                        Console.WriteLine($"Data string: {jsonData}");

                        // Parse the JSON to extract the URI of the edited page.
                        var jsonDoc = JsonDocument.Parse(jsonData);
                        var metaElement = jsonDoc.RootElement.GetProperty("meta");
                        var uriElement = metaElement.GetProperty("uri");
                        var key = uriElement.GetString(); // Use the URI as the message key.

                        // For higher throughput, use the non-blocking Produce call
                        // and handle delivery reports out-of-band, instead of awaiting
                        // the result of a ProduceAsync call.
                        producer.Produce(topicName, new Message<string, string> { Key = key, Value = jsonData },
                            (deliveryReport) =>
                            {
                                if (deliveryReport.Error.Code != ErrorCode.NoError)
                                {
                                    Console.WriteLine($"Failed to deliver message: {deliveryReport.Error.Reason}");
                                }
                                else
                                {
                                    Console.WriteLine($"Produced message to: {deliveryReport.TopicPartitionOffset}");
                                }
                            });
                    }
                }
            }
            catch(Exception ex)
            {
                Console.WriteLine(ex);
            }
            finally
            {
                var queueSize = producer.Flush(TimeSpan.FromSeconds(5));
                if (queueSize > 0)
                {
                    Console.WriteLine("WARNING: Producer event queue has " + queueSize + " pending events on exit.");
                }
                producer.Dispose();
            }
        }

        // Consume messages from the specified Kafka topic.
        static void Consume(string topicName, ClientConfig config)
        {
            Console.WriteLine($"{nameof(Consume)} starting");
            
            // Configure the consumer group based on the provided configuration. 
            var consumerConfig = new ConsumerConfig(config);
            consumerConfig.GroupId = "wiki-edit-stream-group-1";
            consumerConfig.AutoOffsetReset = AutoOffsetReset.Earliest;
            consumerConfig.EnableAutoCommit = false;

            // Enable canceling the consumer loop with Ctrl+C.
            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) => {
                e.Cancel = true; // prevent the process from terminating.
                cts.Cancel();
            };

            // Build a consumer that uses the provided configuration.
            using (var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build())
            {
                // Subscribe to events from the topic.
                consumer.Subscribe(topicName);

                try
                {
                    // Run until the terminal receives Ctrl+C. 
                    while (true)
                    {
                        // Consume and deserialize the next message.
                        var cr = consumer.Consume(cts.Token);

                        // Parse the JSON to extract the URI of the edited page.
                        var jsonDoc = JsonDocument.Parse(cr.Message.Value);

                        // For consuming from the recent_changes topic. 
                        var metaElement = jsonDoc.RootElement.GetProperty("meta");
                        var uriElement = metaElement.GetProperty("uri");
                        var uri = uriElement.GetString();

                        // For consuming from the ksqlDB sink topic.
                        // var editsElement = jsonDoc.RootElement.GetProperty("NUM_EDITS");
                        // var edits = editsElement.GetInt32();
                        // var uri = $"{cr.Message.Key}, edits = {edits}";

                        Console.WriteLine($"Consumed record with URI {uri}");
                    }
                }
                catch (OperationCanceledException)
                {
                    // Ctrl+C was pressed.
                    Console.WriteLine($"Ctrl+C pressed, consumer exiting");
                }
                finally
                {
                    consumer.Close();
                }
            }
        }           
    }
}
