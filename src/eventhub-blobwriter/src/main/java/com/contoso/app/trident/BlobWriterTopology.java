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

	public static void main(String[] args) throws Exception {
		Properties properties = null;
		boolean isLocalCluster = true;
		String topologyName = "localTopology";

		if ((args != null) && (args.length > 0)) { // if running in storm cluster, the first argument is the topology name
			topologyName = args[0];
			isLocalCluster = false;
		}
		
		properties = new Properties();
		properties.load(BlobWriterTopology.class.getClassLoader().getResourceAsStream("Config.properties"));
		// properties.load(new FileReader(args[1])); // load properties from a different file

		int numWorkers = Integer.parseInt(properties.getProperty("eventhubspout.partitions.count"));
		Config config = new Config();
		config.setNumWorkers(numWorkers);
		config.setMaxTaskParallelism(numWorkers);
		
		StormTopology stormTopology = buildTopology(topologyName, numWorkers, properties);

		if(isLocalCluster){
			LocalCluster localCluster = new LocalCluster();
			localCluster.submitTopology(topologyName, config, stormTopology);
		}else{
			StormSubmitter.submitTopology(topologyName, config, stormTopology);			
		}
	}

	static StormTopology buildTopology(String topologyName, int numWorkers,Properties properties) {
		Redis.flushDB(Redis.getHost(properties), Redis.getPassword(properties));
		TridentTopology tridentTopology = new TridentTopology();
		Stream inputStream = null;

		EventHubSpoutConfig spoutConfig = readConfig(properties);
		spoutConfig.setTopologyName(topologyName);
		OpaqueTridentEventHubSpout spout = new OpaqueTridentEventHubSpout(spoutConfig);
		inputStream = tridentTopology.newStream("message", spout);
		inputStream.parallelismHint(numWorkers).partitionAggregate(new Fields("message"), new ByteAggregator(properties), new Fields("blobname"));
		return tridentTopology.build();
	}


	static EventHubSpoutConfig readConfig(Properties properties) {
		EventHubSpoutConfig spoutConfig;
		String username = properties.getProperty("eventhubspout.username");
		String password = properties.getProperty("eventhubspout.password");
		String namespaceName = properties.getProperty("eventhubspout.namespace");
		String entityPath = properties.getProperty("eventhubspout.entitypath");
		String zkEndpointAddress = properties.getProperty("zookeeper.connectionstring");
		int partitionCount = Integer.parseInt(properties.getProperty("eventhubspout.partitions.count"));
		int checkpointIntervalInSeconds = Integer.parseInt(properties.getProperty("eventhubspout.checkpoint.interval"));
		int receiverCredits = Integer.parseInt(properties.getProperty("eventhub.receiver.credits"));
		spoutConfig = new EventHubSpoutConfig(username, password, namespaceName, entityPath, partitionCount, zkEndpointAddress, checkpointIntervalInSeconds,
				receiverCredits);
		return spoutConfig;
	}
}
