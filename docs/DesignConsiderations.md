# Design Considerations for Data Pipeline Guidance (with Apache Storm)

**Note: this is an unfinished draft. I'll add a lot more content to it**

## Business Scenario: Store Event Hub Messages to Microsoft Azure Blob

Connected cars send their status (diagnosis code) to event hub together with their location (latitude and longitude) and timestamp. We want to combine those status information and store them in Microsoft Azure blob.

## Batching

If we don’t need batching, we can use regular storm spouts and bots.
If we need batching, we should consider using Trident.

## Dropped messages

If dropped messages is allowed, we can use non-transactional spout, which will not replay when messages processing fails. In that case, some messages will not be stored in Azure blob.
If dropped messages is not allowed, we need at-least-once guarantee of message processing.  We can considering using a transactional or opaque transactional spout, which can replay the messages if process fails.

## Duplicated Messages in at-least-once semantics

If we can tolerate dups, don’t need batching, but want to guarantee at-least-once process, use Storm’s reliability capabilities.   We must tell Storm when new edges in a tuple tree are being created and tell Storm whenever we have finished processing an individual tuple. These are done using the OutputCollector object that bolts use to emit tuples. Anchoring is done in the emit method, and we declare that we are finished with a tuple using the ack method.
If duplicated messages are allowed, but we need batching, we can use transactional or opaque transactional trident spout and don’t need any additional logic to handle the replay. The replay will be handled the same way as the first play.

## De-duplication logic in exactly-once semantics

If we use Trident transactional or opaque transactional spout, we can be sure that the state updates ordered among batches (i.e. the state updates for batch 3 won’t be applied until the state updates for batch 2 have succeeded). If the state updates for batch 2 failed, batch 2 will be replayed.
However, the replay only guarantees at-least-once processing of the messages. It up to us to implement the de- duplication logic during a replay.

## How do we know whether we are handing a batch the first time or we are inside a replay?

If we are using Trident, we can get the **TransactionId** from **TransactionAttempt** object, which is passed in as an Object in the **Aggregator** init method.  During a replay, the value of TransactionId will be the same as the that of the previous batch.  If we save the previous TransactionId, all we need to do if to compare the current value with the saved value. If the value is the same, we are inside a replay. If there are different, we are inside a fresh new batch.

## Should we use a Storm topology or Trident topology

A Storm structure for distributed computation is called topology.  A topology consists of stream of data, spouts that produce the data, and bots that process the data. Storm topologies run forever, until explicitly killed.

Trident is a high-level abstraction for doing real time computing on top of Storm. Trident process messages in batches and it consists of joins, aggregations, grouping, functions, and filters. Trident support exactly-once semantics.
The core scenario is to aggregate individual messages in to an Azure block, the batching feature provided by Trident make it a good choice. We can simply aggregate a batch into a block if the batch can fits. If a batch is bigger than the max size of a block, we can simply split the batch up in to several blocks. The only limitation is that when the batch is smaller than a block, in which case, the block will not be filled up unless we add more customer logic to handle combining multiple batches into a single block.

## What are built-in stream groupings in Storm and what kind of grouping do we need for our scenario
Storm has seven built-in stream groupings:
1. Shuffle grouping
2. Fields grouping
3. All grouping
4. Global grouping
5. None grouping
6. Direct grouping
7. Local or shuffle grouping

We need to have all messaged in an event hub partition stored in the same Azure blobs. So if we decide to use Storm, it's natural that we want use Fields grouping with partitionId. That means that the spout need to emit the partitionID.

However, if we use Trident, the core data model in Trident is the "Stream", processed as a series of batches. A stream is partitioned among the nodes in the cluster, and operations applied to a stream are applied in parallel across each partition. No explicit grouping is needed for partitionID.

## What are Trident the operations and which are need for our scenario?
There are five kinds of operations in Trident:
1.  Partition-local Operations:
2.  Repartitioning operations
3.  Aggregation operations
4.  Operations on grouped streams
5.  Merges and joins

We want to messages in each partition to be aggregated and stored in its corresponding Azure blobs. So we should pick the first one: Operations that apply locally to each partition and cause no network transfer.

## What are Partition-local Operations in Trident
Partition-local operations involve no network transfer and are applied to each batch partition independently. They include **Functions**, **Filters**, and **partitionAggregate**.
To support the exact once semantics, we need to know whether we are in the replay, whether all the tuples in a batch are processed. We decide to use the most general interface: **Aggregator""

## What is an Aggregator and why it fits our needs
The most general interface for performing aggregations is Aggregator, which looks like this:

``` java
public interface Aggregator<T> extends Operation {
  T init(Object batchId, TridentCollector collector);
  void aggregate(T state, TridentTuple tuple, TridentCollector collector);
  void complete(T state, TridentCollector collector);
}
```
Aggregators can emit any number of tuples with any number of fields. They can emit tuples at any point during execution. Aggregators execute in the following way:

1.  The init method is called before processing the batch.
2.  The aggregate method is called for each input tuple in the batch partition.
3.  The complete method is called when all tuples for the batch partition have been processed by aggregate.

Let's call our aggregator ByteAggregator, we can extends the BaseAggregator instead of directly implemenat the Aggregator interface:

``` java
public class ByteAggregator extends BaseAggregator<T>
```

The following code shows a stripped-down implementation of ByteAggregator:

``` java
public class ByteAggregator extends BaseAggregator<BlockState> {
  private long txid;
  private int partitionIndex;
  private Properties properties;
  public ByteAggregator(Properties properties) {
    this.properties = properties;
  }
  @Override
  public void prepare(@SuppressWarnings("rawtypes") Map conf,TridentOperationContext context) {
    this.partitionIndex = context.getPartitionIndex();
    super.prepare(conf, context);
  }
  public BlockState init(Object batchId, TridentCollector collector) {
    this.txid = ((TransactionAttempt) batchId).getTransactionId();
    BlockState state = new BlockState(this.partitionIndex, this.txid,this.properties);
    return state;
  }
  public void aggregate(BlockState state, TridentTuple tuple,TridentCollector collector) {
    state.block.addData(tuple.getString(0));
  }
  public void complete(BlockState state, TridentCollector collector) {
    state.persist();
    collector.emit(new Values(1));
  }
}
```

Here are the key point of ByteAggregate class:
1. paritionIndex is retrieved in the the prepare method, which is defined in the operation interface. prepare is called once for each partition when the topology starts.
2. txid is retrieved in the init method, which get called by the Trident before processing each batch.
3. tuple string is added into to the block data in aggregate method.
4. the state is persisted in the complete method.

## How to create a Storm or Trident project
1.	mvn archetype:generate -DgroupId=com.mycompany.app -DartifactId=my-app -DarchetypeArtifactId=maven-archetype-quickstart -DinteractiveMode=false
2.	Edit the pom.xml  file and add the Storm dependency:
<dependency> <groupId>org.apache.storm</groupId> <artifactId>storm-core</artifactId> <version>0.9.1-incubating</version> </dependency>
3.	mvn clean package
4.	for Strom: add java class for spout, bolt, and topology
5.  for Trident: add java class for operations
6.  add class to build the topology
5.	F11 to test locally (in Eclipse) or deploy jar file to a storm headnote

## How to send event to Event Hub
We decided to develop the emulator for send event to event hub based on:
[Analyzing sensor data with Storm and HBase in HDInsight (Hadoop)](http://azure.microsoft.com/en-us/documentation/articles/hdinsight-storm-sensor-data-analysis/)

and here is the GitHub repo for it:

[Blackmist/hdinsight-eventhub-example](https://github.com/Blackmist/hdinsight-eventhub-example)


**... to be continued ...**
