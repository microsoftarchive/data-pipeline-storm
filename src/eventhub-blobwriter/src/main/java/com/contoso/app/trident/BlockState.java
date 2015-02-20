// Copyright (c) Microsoft Corporation. All rights reserved. See License.txt in the project root for license information.
package com.contoso.app.trident;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BlockState {
	private static final Logger logger = (Logger) LoggerFactory.getLogger(BlockState.class);

	private static String blockLogFormatter = "partition=%05d_Txid=%05d:";
	private static String blockNameFormatter = "partition_%05d/blob_%05d";
	private static int maxNumberBlocks = 50000;
	static {
		String blockLogFormatterStr = ConfigProperties.getProperty("PARTITION_TXID_LOG_FORMATTER");
		if (blockLogFormatterStr != null) {
			blockLogFormatter = blockLogFormatterStr;
		}
		String blockNameFormatterStr = ConfigProperties.getProperty("BLOBNAME_FORMATTER");
		if (blockNameFormatterStr != null) {
			blockNameFormatter = blockNameFormatterStr;
		}

		String maxNumberBlocksStr = ConfigProperties.getProperty("storage.blob.block.number.max");
		if (maxNumberBlocksStr != null) {
			int maxNumberBlocksInt = Integer.parseInt(maxNumberBlocksStr);
			if (maxNumberBlocksInt > 0 && maxNumberBlocksInt <= 50000) {
				maxNumberBlocks = maxNumberBlocksInt;
			}
		}
	}
	public ByteAggregator byteAggregator;
	public Block firstBlock;
	public Block currentBlock;
	public boolean needPersist = false;
	public String partitionTxidLogStr;
	private int partitionIndex;
	public long txid;

	public BlockState(ByteAggregator aggregator) {
		byteAggregator = aggregator;
		partitionTxidLogStr = String.format(blockLogFormatter, partitionIndex, txid);
		if (LogSetting.LOG_BLOCK) {
			logger.info(partitionTxidLogStr + "Constructor Begin");
		}
		partitionIndex = byteAggregator.partitionIndex;
		txid = byteAggregator.txid;
		String lastTxidStr = BlockStateStore.get(byteAggregator.txidKey);
		if (lastTxidStr == null) {
			// the very first time the topology is running
			currentBlock = getNewBlock();
			if (LogSetting.LOG_BLOCK) {
				logger.info("First Batch: partition= " + partitionIndex + " last txid= " + lastTxidStr + " current txid= " + txid);
			}
		} else {
			long lastTxid = Long.parseLong(lastTxidStr);
			if (txid != lastTxid) {
				// this is a new batch, not a replay, last batch is successful, we just need to get the next block
				currentBlock = getNextBlockAfterLastSuccessBatch();
				if (LogSetting.LOG_BLOCK) {
					logger.info("New Batch: partition= " + partitionIndex + " last txid= " + lastTxidStr + " current txid= " + txid);
				}
			} else {
				// since txid == lastTxid, this is a replay, we need to restart from the first block in the last failed batch
				currentBlock = getFirstBlockInLastFailedBatch();
				if (LogSetting.LOG_BLOCK) {
					logger.info("Replay: partition= " + partitionIndex + " last txid= " + lastTxidStr + " current txid= " + txid);
				}
			}
		}
		firstBlock = currentBlock;
		if (LogSetting.LOG_BLOCK) {
			logger.info(partitionTxidLogStr + "Constructor End with blobid=" + currentBlock.blobid + ", blockid=" + currentBlock.blockid);
			logger.info(partitionTxidLogStr + "Constructor End");
		}
	}

	private Block getNewBlock() {
		if (LogSetting.LOG_BLOCK) {
			logger.info(partitionTxidLogStr + "getNewBlock Begin");
		}
		Block block = new Block();
		String blobname = String.format(blockNameFormatter, partitionIndex, block.blobid);
		block.build(blobname);
		if (LogSetting.LOG_BLOCK) {
			logger.info(partitionTxidLogStr + "getNewBlock End");
		}
		return block;
	}

	public Block getNextBlock(Block current) {
		if (LogSetting.LOG_BLOCK) {
			logger.info("getNextBlock Begin");
		}
		Block block = new Block();
		if (current.blockid < maxNumberBlocks) {
			block.blobid = current.blobid;
			block.blockid = current.blockid + 1;
		} else {
			block.blobid = current.blobid + 1;
			block.blockid = 1;
		}
		String blobname = String.format(blockNameFormatter, partitionIndex, block.blobid);
		block.build(blobname);
		if (LogSetting.LOG_BLOCK) {
			logger.info("getNextBlock returns blobid=" + block.blobid + ", blockid=" + block.blockid);
			logger.info("getNextBlock End");
		}
		return block;
	}

	private Block getNextBlockAfterLastSuccessBatch() {
		if (LogSetting.LOG_BLOCK) {
			logger.info(partitionTxidLogStr + "getNextBlockAfterLastSuccessBatch Begin");
		}
		Block block = new Block();
		String lastBlockStr = BlockStateStore.get(byteAggregator.lastblockKey);
		if (lastBlockStr != null) {
			String[] strArray = lastBlockStr.split("_");
			block.blobid = Integer.parseInt(strArray[0]);
			block.blockid = Integer.parseInt(strArray[1]);
			if (LogSetting.LOG_BLOCK) {
				logger.info(partitionTxidLogStr + " value for " + byteAggregator.lastblockKey + " is " + lastBlockStr);
			}
		} else {
			if (LogSetting.LOG_BLOCK) {
				logger.info(partitionTxidLogStr + " value for " + byteAggregator.lastblockKey + " is null or empty");
			}
		}
		String blobname = String.format(blockNameFormatter, partitionIndex, block.blobid);
		block.build(blobname);
		block = getNextBlock(block);
		if (LogSetting.LOG_BLOCK) {
			logger.info(partitionTxidLogStr + "getNextBlockAfterLastSuccessBatch returns blobid=" + block.blobid + ", blockid=" + block.blockid);
			logger.info(partitionTxidLogStr + "getNextBlockAfterLastSuccessBatch End");
		}
		return block;
	}

	private Block getFirstBlockInLastFailedBatch() {
		if (LogSetting.LOG_BLOCK) {
			logger.info(partitionTxidLogStr + "getFirstBlockInLastFailedBatch Begin");
		}
		Block block = new Block();
		String firstBlockStr = BlockStateStore.get(byteAggregator.firstblockKey);
		if (firstBlockStr != null) {
			String[] strArray = firstBlockStr.split("_");
			block.blobid = Integer.parseInt(strArray[0]);
			block.blockid = Integer.parseInt(strArray[1]);
			if (LogSetting.LOG_BLOCK) {
				logger.info(partitionTxidLogStr + " value for " + byteAggregator.firstblockKey + " is " + firstBlockStr);
			}
		} else {
			logger.info(partitionTxidLogStr + " value for " + byteAggregator.firstblockKey + " is null or empty");
		}
		String blobname = String.format(blockNameFormatter, partitionIndex, block.blobid);
		block.build(blobname);

		if (LogSetting.LOG_BLOCK) {
			logger.info(partitionTxidLogStr + "getFirstBlockInLastFailedBatch returns blobid=" + block.blobid + ", blockid=" + block.blockid);
			logger.info(partitionTxidLogStr + "getFirstBlockInLastFailedBatch End");
		}
		return block;
	}

	public void persistState() {
		if (LogSetting.LOG_BLOCK) {
			logger.info(partitionTxidLogStr + " persistState Begin");
		}
		BlockStateStore.setState(this);
		if (LogSetting.LOG_BLOCK) {
			logger.info(partitionTxidLogStr + " persistState End");
		}
	}
}
