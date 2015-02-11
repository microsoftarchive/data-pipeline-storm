// Copyright (c) Microsoft Corporation. All rights reserved. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Microsoft.ServiceBus.Messaging;
using Newtonsoft.Json;
using Microsoft.ServiceBus;
using System.Threading;
using System.Runtime.Serialization;

namespace SendEvents
{
    class Program
    {
        static int numberOfDevices = 1000;
        static string eventHubName = "hanzeventhub1";
        static string eventHubNamespace = "hanzeventhub1-ns";
        static string devicesSharedAccessPolicyName = "devices";
        static string devicesSharedAccessPolicyKey = "XghKStf9qfyX8iDrCE0lVPmh4fvl9ldzBqe33bcaC/Q=";
        static string rootManageSharedAccessKey = "VremOpcEIYzpxXLIkqjgzT2ZJXBVdTSYxFhkRW6SiY8=";
        static string eventHubConnectionStr = "Endpoint=sb://" + eventHubNamespace + ".servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=" + rootManageSharedAccessKey;
        static void Main(string[] args)
        {
            var settings = new MessagingFactorySettings()
            {
                TokenProvider = TokenProvider.CreateSharedAccessSignatureTokenProvider(devicesSharedAccessPolicyName, devicesSharedAccessPolicyKey),
                TransportType = TransportType.Amqp
            };
            var factory = MessagingFactory.Create(ServiceBusEnvironment.CreateServiceUri("sb", eventHubNamespace, ""), settings);

            EventHubClient client = EventHubClient.CreateFromConnectionString(eventHubConnectionStr, eventHubName);

            try
            {
                List<Task> tasks = new List<Task>();
                Console.WriteLine("Sending messages to Event Hub {0}", client.Path);
                Random random = new Random();
                while (!Console.KeyAvailable)
                {
                    // One event per device
                    for (int devices = 0; devices < numberOfDevices; devices++)
                    {
                        // Create the event
                        Event info = new Event()
                        {
                            id = devices.ToString(),
                            lat = -30 + random.Next(75),
                            lng = -120+random.Next(70),
                            time = DateTime.UtcNow.Ticks,
                            code = (310 + random.Next(20)).ToString()
                        };
                        // Serialize to JSON
                        var serializedString = JsonConvert.SerializeObject(info);
                        Console.WriteLine(serializedString);
                        EventData data = new EventData(Encoding.UTF8.GetBytes(serializedString))
                        {
                            PartitionKey = info.id
                        };

                        // Send the message to Event Hub
                        tasks.Add(client.SendAsync(data));
                    }
                    //Thread.Sleep(1000);
                };

                Task.WaitAll(tasks.ToArray());
            }
            catch (Exception exp)
            {
                Console.WriteLine("Error on send: " + exp.Message);
            }

        }
    }

    [DataContract]
    public class Event
    {
        [DataMember]
        public string id { get; set; }
        [DataMember]
        public double lat { get; set; }
        [DataMember]
        public double lng { get; set; }
        [DataMember]
        public long time { get; set; }
        [DataMember]
        public string code { get; set; }

    }
}