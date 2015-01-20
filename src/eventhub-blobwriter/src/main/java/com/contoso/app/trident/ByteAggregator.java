// Copyright (c) Microsoft Corporation. All rights reserved. See License.txt in the project root for license information.

package com.contoso.app.trident;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.topology.FailedException;
import backtype.storm.tuple.Values;
import storm.trident.operation.BaseAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.topology.TransactionAttempt;
import storm.trident.tuple.TridentTuple;
@SuppressWarnings("unused")
public class ByteAggregator extends BaseAggregator<BlockState> {

	private static final long serialVersionUID = 1L;
	private static final Logger logger = (Logger) LoggerFactory.getLogger(ByteAggregator.class);

	private long txid;
	private int partitionIndex;
	private Properties properties;

	public ByteAggregator(Properties properties) {
		logger.info("Constructor Begin");
		this.properties = properties;
		logger.info("Constructor End");
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map conf, TridentOperationContext context) {

		if (LogSetting.LOG_INSTANCE && LogSetting.LOG_METHOD_BEGIN) {
			logger.info("prepare Begin");
		}

		this.partitionIndex = context.getPartitionIndex();

		super.prepare(conf, context);

		if (LogSetting.LOG_INSTANCE && LogSetting.LOG_METHOD_END) {
			logger.info("p" + this.partitionIndex + ": prepare End");
		}
	}

	public BlockState init(Object batchId, TridentCollector collector) {
		if (LogSetting.LOG_BATCH && LogSetting.LOG_METHOD_BEGIN) {
			logger.info("p" + this.partitionIndex + ": init End");
		}

		if (batchId instanceof TransactionAttempt) {
			this.txid = ((TransactionAttempt) batchId).getTransactionId();
		}
		BlockState state = new BlockState(this.partitionIndex, this.txid, this.properties);
		// BlobWriter.remove(this.properties, state.blockIdStrFormat, state.block.blobname, state.block.blockidStr);

		if (LogSetting.LOG_BATCH && LogSetting.LOG_METHOD_END) {
			logger.info(state.partition_tx_logStr + "init End");
		}
		return state;
	}

	public void aggregate(BlockState state, TridentTuple tuple, TridentCollector collector) {
		if (LogSetting.LOG_MESSAGE && LogSetting.LOG_METHOD_BEGIN) {
			logger.info(state.partition_tx_logStr + "aggregate Begin");
		}

		String tupleStr = tuple.getString(0);
		if (tupleStr != null && tupleStr.length() > 0) {
			String msg = tupleStr + "\r\n";
			if (state.block.isMessageSizeWithnLimit(msg)) {
				if (state.block.willMessageFitCurrentBlock(msg)) {
					state.block.addData(msg);
				} else {
					// since the new msg will not fit into the current block, we will upload the current block,
					// and then get the next block, and add the new msg to the next block
					state.block.upload(this.properties);
					state.needPersist = true;

					if (LogSetting.LOG_BLOCK_ROLL_OVER) {
						logger.info(state.partition_tx_logStr + "Roll over from : blobname = " + state.block.blobname + ", blockid = " + state.block.blockid);
					}

					state.block = state.block.next();

					if (LogSetting.LOG_BLOCK_ROLL_OVER) {
						logger.info(state.partition_tx_logStr + "Roll over to:    blobname = " + state.block.blobname + ", blockid = " + state.block.blockid);
					}

					state.block.addData(msg);
				}
			} else {
				// message size is not within the limit, skip the message 
				logger.info(state.partition_tx_logStr + "message skiped: message size exceeds the size limit, message= " + tupleStr);
			}
		}

		if (LogSetting.LOG_MESSAGE && LogSetting.LOG_METHOD_END) {
			logger.info(state.partition_tx_logStr + "aggregate End");
		}
	}
	public void complete(BlockState state, TridentCollector collector) {
		if (LogSetting.LOG_BATCH && LogSetting.LOG_METHOD_BEGIN) {
			logger.info(state.partition_tx_logStr + "complete Begin");
		}

		if (state.block.blockdata.length() > 0) {
			state.block.upload(this.properties); // upload the last block in the batch
			state.needPersist = true;
		}

		if (state.needPersist) {
			state.persist();
		}
		collector.emit(new Values(1)); // just emit a value

		if (LogSetting.LOG_BATCH && LogSetting.LOG_METHOD_END) {
			logger.info(state.partition_tx_logStr + "complete End");
		}
	}
}
