// Copyright (c) Microsoft Corporation. All rights reserved. See License.txt in the project root for license information.

package com.contoso.app.trident;

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

	public static void main(String[] args) throws Exception {
		boolean isLocalCluster = true;
		String topologyName = "localTopology";

		if ((args != null) && (args.length > 0)) { // if running in storm
													// cluster, the first
													// argument is the topology
													// name
			topologyName = args[0];
			isLocalCluster = false;
		}

		int numWorkers = Integer.parseInt(ConfigProperties.getProperty("eventhubspout.partitions.count"));
		Config config = new Config();
		config.setNumWorkers(numWorkers);
		config.setMaxTaskParallelism(numWorkers);

		StormTopology stormTopology = buildTopology(topologyName);

		if (isLocalCluster) {
			LocalCluster localCluster = new LocalCluster();
			localCluster.submitTopology(topologyName, config, stormTopology);
		} else {
			StormSubmitter.submitTopology(topologyName, config, stormTopology);
		}
	}

	static StormTopology buildTopology(String topologyName) {
		Redis.flushDB();
		TridentTopology tridentTopology = new TridentTopology();
		Stream inputStream = null;

		EventHubSpoutConfig spoutConfig = readConfig();
		spoutConfig.setTopologyName(topologyName);
		OpaqueTridentEventHubSpout spout = new OpaqueTridentEventHubSpout(spoutConfig);
		inputStream = tridentTopology.newStream("message", spout);
		int numWorkers = Integer.parseInt(ConfigProperties.getProperty("eventhubspout.partitions.count"));
		inputStream.parallelismHint(numWorkers).partitionAggregate(new Fields("message"), new ByteAggregator(), new Fields("blobname"));
		return tridentTopology.build();
	}

	static EventHubSpoutConfig readConfig() {
		EventHubSpoutConfig spoutConfig;
		String username = ConfigProperties.getProperty("eventhubspout.username");
		String password = ConfigProperties.getProperty("eventhubspout.password");
		String namespaceName = ConfigProperties.getProperty("eventhubspout.namespace");
		String entityPath = ConfigProperties.getProperty("eventhubspout.entitypath");
		String zkEndpointAddress = ConfigProperties.getProperty("zookeeper.connectionstring");
		int partitionCount = Integer.parseInt(ConfigProperties.getProperty("eventhubspout.partitions.count"));
		int checkpointIntervalInSeconds = Integer.parseInt(ConfigProperties.getProperty("eventhubspout.checkpoint.interval"));
		int receiverCredits = Integer.parseInt(ConfigProperties.getProperty("eventhub.receiver.credits"));
		spoutConfig = new EventHubSpoutConfig(username, password, namespaceName, entityPath, partitionCount, zkEndpointAddress, checkpointIntervalInSeconds,
				receiverCredits);
		return spoutConfig;
	}
}
