# Getting Started with the Reference Implementation
Store Event Hub Messages to Microsoft Azure Blob with Trident

[Microsoft patterns & practices](http://aka.ms/mspnp)

## Prerequisites
- An Azure subscription
- Java and JDK
- Maven
- Visual Studio with the Microsoft Azure SDK for .NET
- Git tool

## Configure Microsoft Azure

### Create the Azure Storage Account
- Create Azure Storage account

### Create the Azure Redis Cache
- Create Azure Redis Cache following [How to Use Azure Redis Cache](http://azure.microsoft.com/en-us/documentation/articles/cache-dotnet-how-to-use-azure-redis-cache/ )

### Configure Event Hub
- Create Event Hub with partition count 10 and message retention of 1 days.
- Once the event hub has been created, select the event hub you created. Select *Configure*, then create two new *shared access policies* using the following information.

NAME    | PERMISSIONS
----    | -----------
devices | Send
storm   | Listen

### Create the HDInsight Storm cluster
- Sign in to the azure and create a storm cluster and create a Storm cluster

## Install Java Dependencies

### Install Microsoft Azure SDK for Java
- Clone [Microsoft Azure SDK for Java](https://github.com/Azure/azure-sdk-for-java)
- Use the following command to install the package into the local Maven store. This will allow us to easily add it as a reference in the Storm project in a later step.

```
mvn clean install -Dmaven.test.skip=true
```

### Install Microsoft Azure Storage libraries for Java
- Clone [Microsoft Azure Storage libraries for Java](https://github.com/Azure/azure-storage-java)
- Use the following command to install the package into the local Maven store. This will allow us to easily add it as a reference in the Storm project in a later step.

```
mvn clean install -Dmaven.test.skip=true
```

### Install the Event Hub spout
Several of the dependencies used in this project must be downloaded and built individually, then installed into the local Maven repository on your development environment.
In order to receive data from Event Hub, we will use the eventhubs-storm-spout.
- Use Remote Desktop to connect to your Storm cluster, then copy the %STORM_HOME%\examples\eventhubspout\eventhubs-storm-spout-0.9-jar-with-dependencies.jar file to your local development environment.
- Use the following command to install the package into the local Maven store. This will allow us to easily add it as a reference in the Storm project in a later step.

```
mvn install:install-file -Dfile=eventhubs-storm-spout-0.9-jar-with-dependencies.jar -DgroupId=com.microsoft.eventhubs -DartifactId=eventhubs-storm-spout -Dversion=0.9 -Dpackaging=jar
```

### Install Jedis SDK with SSL Support
- Clone [jedis sdk with SSL support](https://github.com/RedisLabs/jedis)
- Use the following command to install the package into the local Maven store. This will allow us to easily add it as a reference in the Storm project in a later step.

```
mvn clean install -Dmaven.test.skip=true
```
## Clone the source code for the Reference Implementation
Clone the code from https://github.com/mspnp/storm-trident.git. There are two projects in the src directory.
* SendEvents: C# Console App which send messages to Azure Event Hub
* eventhub-blobwriter: The java implementation of Strom/Trident topology.
* The document [Create Java Topology project eventhub-blobwriter from Scratch](/docs/step-by-step-walkthrough.md) walks you through the steps on how the above project is created.

## Open eventhub-blobwriter in Eclipse (optional)
* Start Eclipse IDE
* Switch Workspace to \data-pipeline-storm\src folder
* import Existing Maven Project: /eventhub-blobwriter/pom.xml
* You should see a list of java files under srcmain/java folder for package com.contoso.app.trident

## Modify the configurations

### Modify Config.properties
Open Config.properties file under conf folder in eventhub-blobwriter project. Modify the values according to your setting:

```
eventhubspout.username = storm
# shared access key for the above event hub policy
eventhubspout.password = [shared access key for the above event hub read policy]
eventhubspout.namespace = [YourServicebusNamespace]
eventhubspout.entitypath = [YourEventhubName]
eventhubspout.partitions.count = 10
# if not provided, will use storm's zookeeper settings
# zookeeper.connectionstring=localhost:2181
eventhubspout.checkpoint.interval = 10
eventhub.receiver.credits = 1024
storage.blob.account.name = [YourStorageAccountName]
storage.blob.account.key =[YourStorageAccountKey]
storage.blob.account.container = [YourStorageAccountContainerName]
#number of blocks in each blob default to 50000
storage.blob.block.number.max = 50000
#max bytes in each block default to 4194304 Byte
storage.blob.block.bytes.max = 4194304
#Redis cache
redis.host = [YourRedisName].redis.cache.windows.net
redis.password = [YourRedisKey]
redis.port = 6380
redis.timeout = 3600
#string format
blobidBlockidStrFormat = %05d_%05d
blobNameFormat = aaa/blobwriter/%05d/%05d
blockIdStrFormat = %05d
partitionTxidLogStrFormat = partition=%05d_Txid=%05d:
partitionTxidKeyStrFormat = p_%05d_Txid
partitionBlocklistKeyStrFormat = p_%05d__Blocklist
```

### Modify LogSetting class

Modify LogSetting.java in directory \eventhub-blobwriter\src\main\java\com\contoso\app\trident\ to fit your needs.

``` java
package com.contoso.app.trident;

public class LogSetting {
	public static final boolean LOG_MESSAGE = false;

	public static final boolean LOG_BATCH = true;
	public static final boolean LOG_INSTANCE = true;

	public static final boolean LOG_BLOCK_ROLL_OVER = false;

	public static final boolean LOG_BLOCK = false;
	public static final boolean LOG_PERSIST = true;
	public static final boolean LOG_GET_LAST_BLOCK = true;
	public static final boolean LOG_GET_FIRST_BLOCK = true;

	public static final boolean LOG_BLOB_WRITER = false;
	public static final boolean LOG_BLOB_WRITER_DATA = false;
	public static final boolean LOG_BLOB_WRITER_BLOCKLIST_BEFORE_UPLOAD = false;
	public static final boolean LOG_BLOB_WRITER_BLOCKLIST_AFTER_UPLOAD = false;

	public static final boolean LOG_REDIS = true;

	public static final boolean LOG_METHOD_BEGIN = false;
	public static final boolean LOG_METHOD_END = false;
}
```

### Modify Configuration for SendEvent

Start Visual Studio, and open SendEvents.sln, modify Program.cs using your Event hub settings.

``` C#
namespace SendEvents
{
    class Program
    {
			static int numberOfDevices = 1000;
			static string eventHubName = "[YourEventHubName]";
			static string eventHubNamespace = "[YourServiceBusNamespaces]";
			static string devicesSharedAccessPolicyName = "devices";
			static string devicesSharedAccessPolicyKey = "[YourdevicesSharedAccessPolicyKey]";
			static string rootManageSharedAccessKey = "YourRootManageSharedAccessKey";
			...
    }
}
```

## Test the topology

### Test the topology locally

To test on your development machine, use the following steps.
- Start the SendEvent .NET application to begin sending events, so that you have something to read from Event Hub.
- Start the topology locally Option 1:
In eclipse, open the eventhub-blobwriter package, and then open BlobWriterTopology.java file. Press F11 to start the topology.
This will start the topology, read messages from Event Hub, and upload them to azure blob storage.
- Verify that the message are uploaded to azure blob.
Start [Azure Storage Explorer](https://azurestorageexplorer.codeplex.com/). Click **refresh** button and then click on the container for the uploaded blobs.
- Note: to restart the topology, you need to delete the existing blobs. A simple way is just delete the container each time you start the topology.

- Start the topology locally Option 2:
You can also start the topology by running the following command line:

```
mvn compile exec:java  -Dstorm.topology=com.contoso.app.trident.BlobWriterTopology
```

You can stop the topology by entering Ctrl-C.



### Test the topology in HDInsight Storm
On your development environment, use the following steps to run the Temperature topology on your HDInsight Storm Cluster.
- Use the following command to create a JAR package from your project.

```
mvn package
```

This will create a file named eventhub-blobwriter-1.0-SNAPSHOT.jar in the target directory of your project.
- On your local development machine, start the SendEvents .NET application, so that we have some events to read.
- Connect to your HDInsight Storm cluster using Remote Desktop, and copy the jar file to the c:\apps\dist\storm<version number> directory.
- Use the HDInsight Command Line icon on the cluster desktop to open a new command prompt, and use the following commands to run the topology.

```
cd %storm_home%
bin\storm jar eventhub-blobwriter-1.0-SNAPSHOT.jar com.contoso.app.trident.BlobWriterTopology  MyTopologyName  
```

- To verify that the message are uploaded to azure blob.
Start [Azure Storage Explorer](https://azurestorageexplorer.codeplex.com/). Click **refresh** button and then click on the container for the uploaded blobs.

- Note: to restart the topology, you need to delete the existing blobs. A simple way is just delete the container each time you start the topology.



- To stop the topology, go to the Remote Desktop session with the Storm cluster and enter the following in the HDInsight Command Line.

```
bin\storm kill MyTopologyName  
```
