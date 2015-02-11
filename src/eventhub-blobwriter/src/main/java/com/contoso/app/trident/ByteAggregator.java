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
public class ByteAggregator extends BaseAggregator<BlockList> {

	private static final long serialVersionUID = 1L;
	private static final Logger logger = (Logger) LoggerFactory.getLogger(ByteAggregator.class);

	private long txid;
	private int partitionIndex;

	public ByteAggregator() {
		if (LogSetting.LOG_CONSTRUCTOR) {
			logger.info("Constructor");
		}
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map conf, TridentOperationContext context) {

		if (LogSetting.LOG_METHOD_BEGIN) {
			logger.info("prepare Begin");
		}

		this.partitionIndex = context.getPartitionIndex();

		super.prepare(conf, context);

		if (LogSetting.LOG_METHOD_END) {
			logger.info("p" + this.partitionIndex + ": prepare End");
		}
	}

	public BlockList init(Object batchId, TridentCollector collector) {
		if (LogSetting.LOG_BATCH && LogSetting.LOG_METHOD_BEGIN) {
			logger.info("p" + this.partitionIndex + ": init End");
		}

		if (batchId instanceof TransactionAttempt) {
			this.txid = ((TransactionAttempt) batchId).getTransactionId();
		}
		BlockList blockList = new BlockList(this.partitionIndex, this.txid);
		// BlobWriter.remove(state.blockIdStrFormat, state.block.blobname,
		// state.block.blockidStr);

		if (LogSetting.LOG_BATCH && LogSetting.LOG_METHOD_END) {
			logger.info(blockList.partitionTxidLogStr + "init End");
		}
		return blockList;
	}

	public void aggregate(BlockList blockList, TridentTuple tuple, TridentCollector collector) {
		if (LogSetting.LOG_MESSAGE && LogSetting.LOG_METHOD_BEGIN) {
			logger.info(blockList.partitionTxidLogStr + "aggregate Begin");
		}

		String tupleStr = tuple.getString(0);
		if (tupleStr != null && tupleStr.length() > 0) {
			String msg = tupleStr + "\r\n";
			if (blockList.currentBlock.isMessageSizeWithnLimit(msg)) {
				if (blockList.currentBlock.willMessageFitCurrentBlock(msg)) {
					blockList.currentBlock.addData(msg);
				} else {
					// since the new msg will not fit into the current block, we
					// will upload the current block,
					// and then get the next block, and add the new msg to the
					// next block
					blockList.currentBlock.upload();
					blockList.needPersist = true;

					if (LogSetting.LOG_BLOCK_ROLL_OVER) {
						logger.info(blockList.partitionTxidLogStr + "Roll over from : blobname = " + blockList.currentBlock.blobname + ", blockid = "
								+ blockList.currentBlock.blockid);
					}

					blockList.currentBlock = blockList.getNextBlock(blockList.currentBlock);

					if (LogSetting.LOG_BLOCK_ROLL_OVER) {
						logger.info(blockList.partitionTxidLogStr + "Roll over to:    blobname = " + blockList.currentBlock.blobname + ", blockid = "
								+ blockList.currentBlock.blockid);
					}

					blockList.currentBlock.addData(msg);
				}
			} else {
				// message size is not within the limit, skip the message
				logger.info(blockList.partitionTxidLogStr + "message skiped: message size exceeds the size limit, message= " + tupleStr);
			}
		}

		if (LogSetting.LOG_MESSAGE && LogSetting.LOG_METHOD_END) {
			logger.info(blockList.partitionTxidLogStr + "aggregate End");
		}
	}

	public void complete(BlockList blockList, TridentCollector collector) {
		if (LogSetting.LOG_BATCH && LogSetting.LOG_METHOD_BEGIN) {
			logger.info(blockList.partitionTxidLogStr + "complete Begin");
		}

		if (blockList.currentBlock.blockdata.length() > 0) {
			blockList.currentBlock.upload(); // upload the last block in the
												// batch
			blockList.needPersist = true;
		}

		if (blockList.needPersist) {
			blockList.persistState();
		}
		collector.emit(new Values(1)); // just emit a value

		if (LogSetting.LOG_BATCH && LogSetting.LOG_METHOD_END) {
			logger.info(blockList.partitionTxidLogStr + "complete End");
		}
	}
}
