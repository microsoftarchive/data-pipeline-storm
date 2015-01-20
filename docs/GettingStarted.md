# Getting Started with the Reference Implementation
Store Event Hub Messages to Microsoft Azure Blob with Trident

*Microsoft patterns & practices*

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
Devices | Send
Storm   | Listen

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
* eventhub-blobwriter: The java implementation of Strom/Trident topology .

## Modify the configurations

### Modify Config.properties
modify the Config.properties file in eventhub-blobwriter project according to you settings:

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
storage.blob.block.number.max = 2
#max bytes in each block default to 4194304 Byte
storage.blob.block.bytes.max = 1024
#Redis cache
redis.host = [YourRedisName].redis.cache.windows.net
redis.password = [YourRedisKey]
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

Modify Program.cs in SendEvent C# Console App using your Event hub settings.

``` C#
namespace SendEvents
{
    class Program
    {
			static int numberOfDevices = 1000;
			static string eventHubName = "[YourEventHubName]";
			static string eventHubNamespace = "[YourServiceBusNamespaces]";
			static string sharedAccessPolicyName = "devices";
			static string sharedAccessPolicyKey = "[YoursharedAccessPolicyKey]";
			static string eventHubConnectionStr = "Endpoint=sb://[eventHubNamespace].servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=[YourSharedAccessKey]";
			static void Main(string[] args)
			...
    }
}
```

## Test the topology with Event Hub

### Test the topology locally

To compile and test the file on your development machine, use the following steps.
- Start the SendEvent .NET application to begin sending events, so that we have something to read from Event Hub.
- Start the topology locally using the following command
- mvn compile exec:java -Dstorm.topology=com.contoso.app.trident.BlobWriterTopology
- This will start the topology, read messages from Event Hub, and upload them to azure blob storage
- After verifying that this works, stop the topology by entering Ctrl-C. To stop the SendEvent app, select the window and press any key.

### Test the topology in HDInsight Storm
On your development environment, use the following steps to run the Temperature topology on your HDInsight Storm Cluster.
- Use the following command to create a JAR package from your project.
mvn package
This will create a file named eventhub-blobwriter-1.0-SNAPSHOT.jar in the target directory of your project.
- On your local development machine, start the SendEvents .NET application, so that we have some events to read.
- Connect to your HDInsight Storm cluster using Remote Desktop, and copy the jar file to the c:\apps\dist\storm<version number> directory.
- Use the HDInsight Command Line icon on the cluster desktop to open a new command prompt, and use the following commands to run the topology.
cd %storm_home%
bin\storm jar eventhub-blobwriter-1.0-SNAPSHOT.jar com.contoso.app.trident.BlobWriterTopology  BlobWriterTopology  
- To stop the topology, go to the Remote Desktop session with the Storm cluster and enter the following in the HDInsight Command Line.

```
bin\storm kill BlobWriterTopology  
```

## Test the topology without Event Hub

To compile and test the file on your development machine without using Event hub, use the following steps.
- Open file javaBlobWriterTopoloty.java
- Uncomment the line under method  buildTopology (String topologyName)

``` java
// TestSpout spout = new TestSpout();
```
- Comment the line

``` java
OpaqueTridentEventHubSpout spout = createOpaqueTridentEventHubSpout(topologyName);
```

- Run the topology either locally or in HDInsight Storm
