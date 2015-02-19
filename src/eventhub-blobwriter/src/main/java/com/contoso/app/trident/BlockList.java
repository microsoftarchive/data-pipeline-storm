// Copyright (c) Microsoft Corporation. All rights reserved. See License.txt in the project root for license information.
package com.contoso.app.trident;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BlockList {
	private static final Logger logger = (Logger) LoggerFactory.getLogger(BlockList.class);

	private static String PARTITION_TXID_LOG_FORMATTER = "partition=%05d_Txid=%05d:";
	private static String PARTITION_TXID_KEY_FORMATTER = "p_%05d_Txid";
	private static String PARTITION_BLOCKLIST_KEY_FORMATTER = "p_%05d__Blocklist";
	private static String BLOBNAME_FORMATTER = "BlobWriterTopology/%05d/%05d";
	private static int MAX_NUMBER_OF_BLOCKS = 50000;
	static {
		String _PARTITION_TXID_LOG_FORMATTER = ConfigProperties.getProperty("PARTITION_TXID_LOG_FORMATTER");
		if (_PARTITION_TXID_LOG_FORMATTER != null) {
			PARTITION_TXID_LOG_FORMATTER = _PARTITION_TXID_LOG_FORMATTER; 
		}
		String _PARTITION_TXID_KEY_FORMATTER = ConfigProperties.getProperty("PARTITION_TXID_KEY_FORMATTER");
		if (_PARTITION_TXID_KEY_FORMATTER != null) {
			PARTITION_TXID_KEY_FORMATTER = _PARTITION_TXID_KEY_FORMATTER; 
		}
		String _PARTITION_BLOCKLIST_KEY_FORMATTER = ConfigProperties.getProperty("PARTITION_BLOCKLIST_KEY_FORMATTER");
		if (_PARTITION_BLOCKLIST_KEY_FORMATTER != null) {
			PARTITION_BLOCKLIST_KEY_FORMATTER = _PARTITION_BLOCKLIST_KEY_FORMATTER; 
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

	public Block currentBlock;
	public boolean needPersist = false;
	public String partitionTxidLogStr;
	private String partitionTxidKeyStr;
	private String partitionBlocklistKeyStr;
	// blockList stores a list of block id string
	private List<String> blockList;
	private int partitionIndex;
	private long txid;

	public BlockList(int partitionIndex, long txid) {
		this.partitionTxidLogStr = String.format(PARTITION_TXID_LOG_FORMATTER, partitionIndex, txid);
		if (LogSetting.LOG_BLOCKLIST) {
			logger.info(partitionTxidLogStr + "Constructor Begin");
		}
		this.blockList = new ArrayList<String>();
		this.partitionIndex = partitionIndex;
		this.txid = txid;
		this.partitionTxidKeyStr = String.format(PARTITION_TXID_KEY_FORMATTER, partitionIndex);
		this.partitionBlocklistKeyStr = String.format(PARTITION_BLOCKLIST_KEY_FORMATTER, partitionIndex);
		String lastTxidStr = BlobWriterState.get(this.partitionTxidKeyStr);
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
		String blobidAndBlockidStr = block.build(blobname);
		this.blockList.add(blobidAndBlockidStr);
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
		String blobidAndBlockidStr = block.build(blobname);
		this.blockList.add(blobidAndBlockidStr);
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
		List<String> lastBlobidBlockidList = BlobWriterState.getList(this.partitionBlocklistKeyStr, 50000);
		if (lastBlobidBlockidList != null && lastBlobidBlockidList.size() > 0) {
			int lastIndex = lastBlobidBlockidList.size() - 1;
			String blockStr = lastBlobidBlockidList.get(lastIndex);
			String[] strArray = blockStr.split("_");
			block.blobid = Integer.parseInt(strArray[0]);
			block.blockid = Integer.parseInt(strArray[1]);
			if (LogSetting.LOG_BLOCK) {
				logger.info(this.partitionTxidLogStr + "Last record in List(" + this.partitionBlocklistKeyStr + "): " + blockStr);
				logger.info(this.partitionTxidLogStr + "Last record in List( " + this.partitionBlocklistKeyStr + "): blobid=" + block.blobid + ", blockid = "
						+ block.blockid);
			}
		} else {
			if (LogSetting.LOG_BLOCK) {
				logger.info(this.partitionTxidLogStr + "List(" + this.partitionBlocklistKeyStr + ") is null or empty");
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
		List<String> lastblocks = BlobWriterState.getList(this.partitionBlocklistKeyStr, 50000);
		if (lastblocks != null && lastblocks.size() > 0) {
			String blockStr = lastblocks.get(0);
			String[] strArray = blockStr.split("_");
			block.blobid = Integer.parseInt(strArray[0]);
			block.blockid = Integer.parseInt(strArray[1]);
			if (LogSetting.LOG_BLOCK) {
				logger.info(this.partitionTxidLogStr + "First record in List(" + this.partitionBlocklistKeyStr + "): " + blockStr);
				logger.info(this.partitionTxidLogStr + "First record in List(" + this.partitionBlocklistKeyStr + "): blobid=" + block.blobid + ", blockid = "
						+ block.blockid);
			}
		} else {
			logger.info(this.partitionTxidLogStr + "List(" + this.partitionBlocklistKeyStr + ") is null or empty");
		}
		String blobname = String.format(BLOBNAME_FORMATTER, this.partitionIndex, block.blobid);
		String blobidAndBlockidStr = block.build(blobname);
		this.blockList.add(blobidAndBlockidStr);

		if (LogSetting.LOG_BLOCK) {
			logger.info(this.partitionTxidLogStr + "getFirstBlockInLastFailedBatch returns blobid=" + block.blobid + ", blockid=" + block.blockid);
			logger.info(this.partitionTxidLogStr + "getFirstBlockInLastFailedBatch End");
		}
		return block;
	}

	public void persistState() {
		if (LogSetting.LOG_BLOCK) {
			logger.info(this.partitionTxidLogStr + "persistState Begin");
			logger.info("persistState: set(" + this.partitionTxidKeyStr + ")= " + this.txid + ")");
			logger.info("persistState: number of blocks= " + this.blockList.size());
			for (String s : this.blockList) {
				logger.info("persistState: " + this.partitionTxidLogStr + " addToList(" + this.partitionBlocklistKeyStr + ", " + s + ")");
			}
		}
		BlobWriterState.setState(this.partitionTxidKeyStr, String.valueOf(this.txid), this.partitionBlocklistKeyStr, this.blockList);
		if (LogSetting.LOG_BLOCK) {
			logger.info(this.partitionTxidLogStr + "this.partition_tx_logStr + persistState End");
		}
	}
}
