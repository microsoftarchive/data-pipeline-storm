// Copyright (c) Microsoft Corporation. All rights reserved. See License.txt in the project root for license information.

package com.contoso.app.trident;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.eventhubs.spout.IStateStore;
import com.microsoft.eventhubs.spout.ZookeeperStateStore;

import backtype.storm.topology.FailedException;
import backtype.storm.tuple.Values;
import storm.trident.operation.BaseAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.topology.TransactionAttempt;
import storm.trident.tuple.TridentTuple;

public class ByteAggregator extends BaseAggregator<BlockList> {
	private static final long serialVersionUID = 1L;
	private static final Logger logger = (Logger) LoggerFactory.getLogger(ByteAggregator.class);
	private static String PARTITION_TXID_KEY_FORMATTER = "p_%05d_Txid";
	private static String PARTITION_BLOCKLIST_KEY_FORMATTER = "p_%05d__Blocklist";

	private long txid;
	private int partitionIndex;
	private long msgCount;
	private static IStateStore stateStore = null;
	public static String partitionTxidKeyStr = null;
	public static String partitionBlocklistKeyStr = null;
	
	static {
		String _PARTITION_TXID_KEY_FORMATTER = ConfigProperties.getProperty("PARTITION_TXID_KEY_FORMATTER");
		if (_PARTITION_TXID_KEY_FORMATTER != null) {
			PARTITION_TXID_KEY_FORMATTER = _PARTITION_TXID_KEY_FORMATTER; 
		}
		String _PARTITION_BLOCKLIST_KEY_FORMATTER = ConfigProperties.getProperty("PARTITION_BLOCKLIST_KEY_FORMATTER");
		if (_PARTITION_BLOCKLIST_KEY_FORMATTER != null) {
			PARTITION_BLOCKLIST_KEY_FORMATTER = _PARTITION_BLOCKLIST_KEY_FORMATTER; 
		}
	}

	public ByteAggregator() {
		if (LogSetting.LOG_BATCH) {
			logger.info("Constructor");
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map conf, TridentOperationContext context) {
		if (LogSetting.LOG_BATCH) {
			logger.info("prepare Begin");
		}
		this.partitionIndex = context.getPartitionIndex();
		ByteAggregator.partitionTxidKeyStr = String.format(PARTITION_TXID_KEY_FORMATTER, partitionIndex);
		ByteAggregator.partitionBlocklistKeyStr = String.format(PARTITION_BLOCKLIST_KEY_FORMATTER, partitionIndex);

		if (ByteAggregator.stateStore == null) {

			@SuppressWarnings("rawtypes")
			List<String> zkServers = (List) conf.get("storm.zookeeper.servers");
			Integer zkPort = Integer.valueOf(((Number) conf.get("storm.zookeeper.port")).intValue());
			StringBuilder sb = new StringBuilder();
			for (String zk : zkServers) {
				if (sb.length() > 0) {
					sb.append(',');
				}
				sb.append(zk + ":" + zkPort);
			}
			String zkEndpointAddress = sb.toString();
			ByteAggregator.stateStore = new ZookeeperStateStore(zkEndpointAddress, ((Integer) conf.get("storm.zookeeper.retry.times")).intValue(),
					((Integer) conf.get("storm.zookeeper.retry.interval")).intValue());
			ByteAggregator.stateStore.open();
		}
		BlobWriterState.clearState(ByteAggregator.partitionTxidKeyStr, ByteAggregator.partitionBlocklistKeyStr);

		super.prepare(conf, context);
		if (LogSetting.LOG_BATCH) {
			logger.info("p" + this.partitionIndex + ": prepare End");
		}
	}

	public BlockList init(Object batchId, TridentCollector collector) {
		if (LogSetting.LOG_BATCH) {
			logger.info("p" + this.partitionIndex + ": init End");
		}
		if (batchId instanceof TransactionAttempt) {
			this.txid = ((TransactionAttempt) batchId).getTransactionId();
		} else {
			throw new FailedException("Error configuring ByteAggregator");
		}
		msgCount = 0;
		BlockList blockList = new BlockList(this.partitionIndex, this.txid);
		if (LogSetting.LOG_BATCH) {
			logger.info(blockList.partitionTxidLogStr + "init End");
		}
		return blockList;
	}

	public void aggregate(BlockList blockList, TridentTuple tuple, TridentCollector collector) {
		if (LogSetting.LOG_MESSAGE) {
			logger.info(blockList.partitionTxidLogStr + "aggregate Begin");
		}
		String tupleStr = tuple.getString(0);
		if (tupleStr != null && tupleStr.length() > 0) {
			if (LogSetting.LOG_MESSAGE) {
				logger.info(blockList.partitionTxidLogStr + "Message= " + tupleStr);
			}
			String msg = tupleStr + "\r\n";
			if (Block.isMessageSizeWithnLimit(msg)) {
				if (blockList.currentBlock.willMessageFitCurrentBlock(msg)) {
					blockList.currentBlock.addData(msg);
				} else {
					// since the new msg will not fit into the current block, we will upload the current block,
					// and then get the next block, and add the new msg to the next block
					blockList.currentBlock.upload();
					blockList.needPersist = true;
					if (LogSetting.LOG_MESSAGEROLLOVER) {
						logger.info(blockList.partitionTxidLogStr + "Roll over from : blobname = " + blockList.currentBlock.blobname + ", blockid = "
								+ blockList.currentBlock.blockid);
					}
					blockList.currentBlock = blockList.getNextBlock(blockList.currentBlock);
					if (LogSetting.LOG_MESSAGEROLLOVER) {
						logger.info(blockList.partitionTxidLogStr + "Roll over to:    blobname = " + blockList.currentBlock.blobname + ", blockid = "
								+ blockList.currentBlock.blockid);
					}
					blockList.currentBlock.addData(msg);
				}
				msgCount++;
			} else {
				// message size is not within the limit, skip the message and log it.
				logger.error(blockList.partitionTxidLogStr + "message skiped: message size exceeds the size limit, message= " + tupleStr);
			}
		}
		if (LogSetting.LOG_MESSAGE) {
			logger.info(blockList.partitionTxidLogStr + "aggregate End");
		}
	}

	public void complete(BlockList blockList, TridentCollector collector) {
		if (LogSetting.LOG_BATCH) {
			logger.info(blockList.partitionTxidLogStr + "complete Begin");
		}
		if (blockList.currentBlock.blockdata.length() > 0) {
			blockList.currentBlock.upload();
			blockList.needPersist = true;
		}
		if (blockList.needPersist) {
			blockList.persistState();
		}
		collector.emit(new Values(msgCount));
		if (LogSetting.LOG_BATCH) {
			logger.info(blockList.partitionTxidLogStr + "message count = " + msgCount);
			logger.info(blockList.partitionTxidLogStr + "complete End");
		}
	}
}
