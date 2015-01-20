// Copyright (c) Microsoft Corporation. All rights reserved. See License.txt in the project root for license information.

package com.contoso.app.trident;
import java.util.Properties;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import com.microsoft.eventhubs.spout.EventHubSpoutConfig;
import com.microsoft.eventhubs.trident.OpaqueTridentEventHubSpout;
import storm.trident.Stream;
import storm.trident.TridentTopology;

public class BlobWriterTopology {
	static Properties properties = null;
	static int numWorkers = 1;

	public static void main(String[] args) throws Exception {
		System.out.println("Loding properties from Config.properties");
		properties = new Properties();
		properties.load(BlobWriterTopology.class.getClassLoader().getResourceAsStream("Config.properties"));
		// properties.load(new FileReader(args[1])); // load properties from a different file

		if ((args != null) && (args.length > 0)) { // if running in storm cluster, the first argument is the topology name
			String topologyName = args[0];
			StormTopology stormTopology = buildTopology(topologyName);
			Config config = new Config();
			numWorkers = Integer.parseInt(properties.getProperty("eventhubspout.partitions.count"));
			config.setNumWorkers(numWorkers);
			System.out.println("Number of workers = " + numWorkers);
			StormSubmitter.submitTopology(topologyName, config, stormTopology);
		} else {// if running in local development environment, there is no argument for topology name
			String topologyName = "localTopology";
			StormTopology stormTopology = buildTopology(topologyName);
			Config config = new Config();
			config.setMaxTaskParallelism(10);
			LocalCluster localCluster = new LocalCluster();
			localCluster.submitTopology(topologyName, config, stormTopology);
			Thread.sleep(5000000L);
			localCluster.shutdown();
		}
	}

	static StormTopology buildTopology(String topologyName) {
		Redis.flushDB(Redis.getHost(properties), Redis.getPassword(properties));
		TridentTopology tridentTopology = new TridentTopology();
		Stream inputStream = null;
		// TestSpout spout = new TestSpout();
		OpaqueTridentEventHubSpout spout = createOpaqueTridentEventHubSpout(topologyName);
		inputStream = tridentTopology.newStream("message", spout);
		inputStream.parallelismHint(numWorkers).partitionAggregate(new Fields("message"), new ByteAggregator(properties), new Fields("blobname"));
		return tridentTopology.build();
	}

	static OpaqueTridentEventHubSpout createOpaqueTridentEventHubSpout(String topologyName) {
		EventHubSpoutConfig spoutConfig = readConfig();
		spoutConfig.setTopologyName(topologyName);
		OpaqueTridentEventHubSpout spout = new OpaqueTridentEventHubSpout(spoutConfig);
		return spout;
	}

	static EventHubSpoutConfig readConfig() {
		EventHubSpoutConfig spoutConfig;
		String username = properties.getProperty("eventhubspout.username");
		String password = properties.getProperty("eventhubspout.password");
		String namespaceName = properties.getProperty("eventhubspout.namespace");
		String entityPath = properties.getProperty("eventhubspout.entitypath");
		String zkEndpointAddress = properties.getProperty("zookeeper.connectionstring");
		int partitionCount = Integer.parseInt(properties.getProperty("eventhubspout.partitions.count"));
		int checkpointIntervalInSeconds = Integer.parseInt(properties.getProperty("eventhubspout.checkpoint.interval"));
		int receiverCredits = Integer.parseInt(properties.getProperty("eventhub.receiver.credits"));
		System.out.println("Eventhub spout config: ");
		System.out.println("  username: " + username);
		System.out.println("  password: " + password);
		System.out.println("  namespaceName: " + namespaceName);
		System.out.println("  entityPath: " + entityPath);
		System.out.println("  zkEndpointAddress: " + zkEndpointAddress);
		System.out.println("  partition count: " + partitionCount);
		System.out.println("  checkpoint interval: " + checkpointIntervalInSeconds);
		System.out.println("  receiver credits: " + receiverCredits);
		spoutConfig = new EventHubSpoutConfig(username, password, namespaceName, entityPath, partitionCount, zkEndpointAddress, checkpointIntervalInSeconds,
				receiverCredits);
		return spoutConfig;
	}
}
