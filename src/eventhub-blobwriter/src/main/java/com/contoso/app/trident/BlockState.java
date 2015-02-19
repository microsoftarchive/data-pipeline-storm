// Copyright (c) Microsoft Corporation. All rights reserved. See License.txt in the project root for license information.
package com.contoso.app.trident;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BlockState {
	private static final Logger logger = (Logger) LoggerFactory.getLogger(BlockState.class);

	private static String PARTITION_TXID_LOG_FORMATTER = "partition=%05d_Txid=%05d:";
	private static String BLOBNAME_FORMATTER = "partition_%05d/blob_%05d";
	private static int MAX_NUMBER_OF_BLOCKS = 50000;
	static {
		String _PARTITION_TXID_LOG_FORMATTER = ConfigProperties.getProperty("PARTITION_TXID_LOG_FORMATTER");
		if (_PARTITION_TXID_LOG_FORMATTER != null) {
			PARTITION_TXID_LOG_FORMATTER = _PARTITION_TXID_LOG_FORMATTER;
		}
		String _BLOBNAME_FORMATTER = ConfigProperties.getProperty("BLOBNAME_FORMATTER");
		if (_BLOBNAME_FORMATTER != null) {
			BLOBNAME_FORMATTER = _BLOBNAME_FORMATTER;
		}

		String _MAX_NUMBER_OF_BLOCKS = ConfigProperties.getProperty("storage.blob.block.number.max");
		if (_MAX_NUMBER_OF_BLOCKS != null) {
			int i_MAX_NUMBER_OF_BLOCKS = Integer.parseInt(_MAX_NUMBER_OF_BLOCKS);
			if (i_MAX_NUMBER_OF_BLOCKS > 0 && i_MAX_NUMBER_OF_BLOCKS <= 50000) {
				MAX_NUMBER_OF_BLOCKS = i_MAX_NUMBER_OF_BLOCKS;
			}
		}
	}

	public Block firstBlock;
	public Block currentBlock;
	public boolean needPersist = false;
	public String partitionTxidLogStr;
	private int partitionIndex;
	private long txid;

	public BlockState(int partitionIndex, long txid) {
		this.partitionTxidLogStr = String.format(PARTITION_TXID_LOG_FORMATTER, partitionIndex, txid);
		if (LogSetting.LOG_BLOCKLIST) {
			logger.info(partitionTxidLogStr + "Constructor Begin");
		}
		this.partitionIndex = partitionIndex;
		this.txid = txid;
		String lastTxidStr = BlockStateStore.get(ByteAggregator.partitionTxidKeyStr);
		if (lastTxidStr == null) {
			// the very first time the topology is running
			this.currentBlock = getNewBlock();
			if (LogSetting.LOG_BLOCKLIST) {
				logger.info("First Batch: partition= " + this.partitionIndex + " last txid= " + lastTxidStr + " current txid= " + txid);
			}
		} else {
			long lastTxid = Long.parseLong(lastTxidStr);
			if (txid != lastTxid) {
				// this is a new batch, not a replay, last batch is successful, we just need to get the next block
				this.currentBlock = getNextBlockAfterLastSuccessBatch();
				if (LogSetting.LOG_BLOCKLIST) {
					logger.info("New Batch: partition= " + this.partitionIndex + " last txid= " + lastTxidStr + " current txid= " + txid);
				}
			} else {
				// since txid == lastTxid, this is a replay, we need to restart from the first block in the last failed batch
				this.currentBlock = getFirstBlockInLastFailedBatch();
				if (LogSetting.LOG_BLOCKLIST) {
					logger.info("Replay: partition= " + this.partitionIndex + " last txid= " + lastTxidStr + " current txid= " + txid);
				}
			}
		}
		firstBlock = currentBlock;
		if (LogSetting.LOG_BLOCKLIST) {
			logger.info(this.partitionTxidLogStr + "Constructor End with blobid=" + this.currentBlock.blobid + ", blockid=" + this.currentBlock.blockid);
			logger.info(this.partitionTxidLogStr + "Constructor End");
		}
	}

	private Block getNewBlock() {
		if (LogSetting.LOG_BLOCK) {
			logger.info(this.partitionTxidLogStr + "getNewBlock Begin");
		}
		Block block = new Block();
		String blobname = String.format(BLOBNAME_FORMATTER, this.partitionIndex, block.blobid);
		block.build(blobname);
		if (LogSetting.LOG_BLOCK) {
			logger.info(this.partitionTxidLogStr + "getNewBlock End");
		}
		return block;
	}

	public Block getNextBlock(Block current) {
		if (LogSetting.LOG_BLOCK) {
			logger.info("getNextBlock Begin");
		}
		Block block = new Block();
		if (current.blockid < MAX_NUMBER_OF_BLOCKS) {
			block.blobid = current.blobid;
			block.blockid = current.blockid + 1;
		} else {
			block.blobid = current.blobid + 1;
			block.blockid = 1;
		}
		String blobname = String.format(BLOBNAME_FORMATTER, this.partitionIndex, block.blobid);
		block.build(blobname);
		if (LogSetting.LOG_BLOCK) {
			logger.info("getNextBlock returns blobid=" + block.blobid + ", blockid=" + block.blockid);
			logger.info("getNextBlock End");
		}
		return block;
	}

	private Block getNextBlockAfterLastSuccessBatch() {
		if (LogSetting.LOG_BLOCK) {
			logger.info(this.partitionTxidLogStr + "getNextBlockAfterLastSuccessBatch Begin");
		}
		Block block = new Block();
		String lastBlockStr = BlockStateStore.get(ByteAggregator.partitionLastblockKeyStr);
		if (lastBlockStr != null) {
			String[] strArray = lastBlockStr.split("_");
			block.blobid = Integer.parseInt(strArray[0]);
			block.blockid = Integer.parseInt(strArray[1]);
			if (LogSetting.LOG_BLOCK) {
				logger.info(this.partitionTxidLogStr + " value for " + ByteAggregator.partitionLastblockKeyStr + " is " + lastBlockStr);
			}
		} else {
			if (LogSetting.LOG_BLOCK) {
				logger.info(this.partitionTxidLogStr + " value for " + ByteAggregator.partitionLastblockKeyStr + " is null or empty");
			}
		}
		String blobname = String.format(BLOBNAME_FORMATTER, this.partitionIndex, block.blobid);
		block.build(blobname);
		block = getNextBlock(block);
		if (LogSetting.LOG_BLOCK) {
			logger.info(this.partitionTxidLogStr + "getNextBlockAfterLastSuccessBatch returns blobid=" + block.blobid + ", blockid=" + block.blockid);
			logger.info(this.partitionTxidLogStr + "getNextBlockAfterLastSuccessBatch End");
		}
		return block;
	}

	private Block getFirstBlockInLastFailedBatch() {
		if (LogSetting.LOG_BLOCK) {
			logger.info(this.partitionTxidLogStr + "getFirstBlockInLastFailedBatch Begin");
		}
		Block block = new Block();
		String firstBlockStr = BlockStateStore.get(ByteAggregator.partitionFirstblockKeyStr);
		if (firstBlockStr != null) {
			String[] strArray = firstBlockStr.split("_");
			block.blobid = Integer.parseInt(strArray[0]);
			block.blockid = Integer.parseInt(strArray[1]);
			if (LogSetting.LOG_BLOCK) {
				logger.info(this.partitionTxidLogStr + " value for " + ByteAggregator.partitionFirstblockKeyStr + " is " + firstBlockStr);
			}
		} else {
			logger.info(this.partitionTxidLogStr + " value for " + ByteAggregator.partitionFirstblockKeyStr + " is null or empty");
		}
		String blobname = String.format(BLOBNAME_FORMATTER, this.partitionIndex, block.blobid);
		block.build(blobname);

		if (LogSetting.LOG_BLOCK) {
			logger.info(this.partitionTxidLogStr + "getFirstBlockInLastFailedBatch returns blobid=" + block.blobid + ", blockid=" + block.blockid);
			logger.info(this.partitionTxidLogStr + "getFirstBlockInLastFailedBatch End");
		}
		return block;
	}

	public void persistState() {
		if (LogSetting.LOG_BLOCK) {
			logger.info(this.partitionTxidLogStr + "persistState Begin");
			logger.info(this.partitionTxidLogStr + "set(" + ByteAggregator.partitionTxidKeyStr       + ") to" + this.txid);
			logger.info(this.partitionTxidLogStr + "set(" + ByteAggregator.partitionFirstblockKeyStr + ") to" + firstBlock.blobidAndBlockidStr);
			logger.info(this.partitionTxidLogStr + "set(" + ByteAggregator.partitionLastblockKeyStr  + ") to" + currentBlock.blobidAndBlockidStr);
		}
		BlockStateStore.setState(ByteAggregator.partitionTxidKeyStr, String.valueOf(this.txid), 
				ByteAggregator.partitionFirstblockKeyStr, firstBlock.blobidAndBlockidStr,
				ByteAggregator.partitionLastblockKeyStr, currentBlock.blobidAndBlockidStr);
		if (LogSetting.LOG_BLOCK) {
			logger.info(this.partitionTxidLogStr + "this.partition_tx_logStr + persistState End");
		}
	}
}
