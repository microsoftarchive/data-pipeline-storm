// Copyright (c) Microsoft Corporation. All rights reserved. See License.txt in the project root for license information.

package com.contoso.app.trident;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("unused")
public class BlockList {
	public Block currentBlock;
	public boolean needPersist = false;
	public String partitionTxidLogStr;	
	private String partitionTxidKeyStr;

	private List<String> blockList;	// blockList stores a list of block id string
	private static final Logger logger = (Logger) LoggerFactory.getLogger(BlockList.class);
	private String key_partitionBlocklist;
	private int partitionIndex;
	private long txid;

	public BlockList(int partitionIndex, long txid) {
		String partitionTxidLogStrFormat = ConfigProperties.getProperty("partitionTxidLogStrFormat");
		this.partitionTxidLogStr = String.format(partitionTxidLogStrFormat, partitionIndex, txid);
		if ((LogSetting.LOG_INSTANCE || LogSetting.LOG_BLOCK) && LogSetting.LOG_METHOD_BEGIN) {
			logger.info(partitionTxidLogStr + "Constructor Begin");
		}

		this.blockList = new ArrayList<String>();
		this.partitionTxidKeyStr = "p_" + String.valueOf(partitionIndex) + "_txid";
		this.key_partitionBlocklist = "p_" + String.valueOf(partitionIndex) + "_blocklist";
		this.partitionIndex = partitionIndex;
		this.txid = txid;	
		String lastTxidStr = Redis.get(this.partitionTxidKeyStr);
		if (lastTxidStr == null) { // the very first time the topology is running
			this.currentBlock = getNewBlock();
		} else {
			long lastTxid = Long.parseLong(lastTxidStr);
			if (txid != lastTxid) { // a new batch, not a replay, last batch is successful
				this.currentBlock = getNextBlockAfterLastSuccessBatch();
			} else {// if(txid == lastTxid), a replay, overwrite old block
				this.currentBlock = getFirstBlockInLastFailedBatch();
			}
		}

		if ((LogSetting.LOG_INSTANCE || LogSetting.LOG_BLOCK) && LogSetting.LOG_METHOD_END) {
			logger.info(this.partitionTxidLogStr + "Constructor End with blobid=" + this.currentBlock.blobid + ", blockid=" + this.currentBlock.blockid);
			logger.info(this.partitionTxidLogStr + "Constructor End");
		}
	}
	private Block getNewBlock() {
		if (LogSetting.LOG_BLOCK && LogSetting.LOG_METHOD_BEGIN) {
			logger.info(this.partitionTxidLogStr + "getNewBlock Begin");
		}

		Block block = new Block();
		String blobNameFormat = ConfigProperties.getProperty("blobNameFormat");
		String blobname = String.format(blobNameFormat, this.partitionIndex, block.blobid);

		String blobidAndBlockidStr = block.build(blobname);
		this.blockList.add(blobidAndBlockidStr);		

		if (LogSetting.LOG_BLOCK && LogSetting.LOG_METHOD_END) {
			logger.info(this.partitionTxidLogStr + "getNewBlock End");
		}
		return block;
	}
	public Block getNextBlock(Block current) {
		if (LogSetting.LOG_BLOCK && LogSetting.LOG_METHOD_BEGIN) {
			logger.info("getNextBlock Begin");
		}

		Block block = new Block();
		int maxNumberOfBlocks = ConfigProperties.getMaxNumberOfblocks();
		if (current.blockid < maxNumberOfBlocks) {
			block.blobid = current.blobid;
			block.blockid = current.blockid + 1;
		} else {
			block.blobid = current.blobid + 1;
			block.blockid = 1;
		}

		String blobNameFormat = ConfigProperties.getProperty("blobNameFormat");
		String blobname = String.format(blobNameFormat, this.partitionIndex, block.blobid);

		String blobidAndBlockidStr = block.build(blobname);
		this.blockList.add(blobidAndBlockidStr);		


		if (LogSetting.LOG_BLOCK && LogSetting.LOG_METHOD_END) {
			logger.info("getNextBlock returns blobid=" + block.blobid + ", blockid=" + block.blockid);
			logger.info("getNextBlock End");
		}

		return block;
	}
	private Block getNextBlockAfterLastSuccessBatch() {
		if (LogSetting.LOG_BLOCK && LogSetting.LOG_METHOD_BEGIN) {
			logger.info(this.partitionTxidLogStr + "getNextBlockAfterLastSuccessBatch Begin");
		}

		Block block = new Block();
		List<String> lastBlobidBlockidList = Redis.getList(this.key_partitionBlocklist, 50000);
		if (lastBlobidBlockidList != null && lastBlobidBlockidList.size() > 0) {
			String blockStr = lastBlobidBlockidList.get(0);
			for (String s : lastBlobidBlockidList) {
				if (s.compareTo(blockStr) > 0) {// find the last block written in the last batch
					blockStr = s;
				}
			}
			String[] strArray = blockStr.split("_");
			block.blobid = Integer.parseInt(strArray[0]);
			block.blockid = Integer.parseInt(strArray[1]);
			if (LogSetting.LOG_GET_LAST_BLOCK) {
				logger.info(this.partitionTxidLogStr + "Last record in List(" + this.key_partitionBlocklist + "): " + blockStr);
				logger.info(this.partitionTxidLogStr + "Last record in List( " + this.key_partitionBlocklist + "): blobid=" + block.blobid + ", blockid = "
						+ block.blockid);
			}
		} else {
			if (LogSetting.LOG_GET_LAST_BLOCK) {
				logger.info(this.partitionTxidLogStr + "List(" + this.key_partitionBlocklist + ") is null or empty");
			}
		}
		
		String blobNameFormat = ConfigProperties.getProperty("blobNameFormat");
		String blobname = String.format(blobNameFormat, this.partitionIndex, block.blobid);
		String blobidAndBlockidStr = block.build(blobname);
		block = getNextBlock(block);

		if (LogSetting.LOG_BLOCK && LogSetting.LOG_METHOD_END) {
			logger.info(this.partitionTxidLogStr + "getNextBlockAfterLastSuccessBatch returns blobid=" + block.blobid + ", blockid=" + block.blockid);
			logger.info(this.partitionTxidLogStr + "getNextBlockAfterLastSuccessBatch End");
		}
		return block;
	}
	private Block getFirstBlockInLastFailedBatch() {
		if (LogSetting.LOG_BLOCK && LogSetting.LOG_METHOD_BEGIN) {
			logger.info(this.partitionTxidLogStr + "getFirstBlockInLastFailedBatch Begin");
		}

		Block block = new Block();
		List<String> lastblocks = Redis.getList(this.key_partitionBlocklist, 50000);
		if (lastblocks != null && lastblocks.size() > 0) {
			String blockStr = lastblocks.get(0);
			for (String s : lastblocks) {
				if (s.compareTo(blockStr) < 0) {// find the first block written in the last batch
					blockStr = s;
				}
			}
			String[] strArray = blockStr.split("_");
			block.blobid = Integer.parseInt(strArray[0]);
			block.blockid = Integer.parseInt(strArray[1]);
			if (LogSetting.LOG_GET_FIRST_BLOCK) {
				logger.info(this.partitionTxidLogStr + "First record in List(" + this.key_partitionBlocklist + "): " + blockStr);
				logger.info(this.partitionTxidLogStr + "First record in List(" + this.key_partitionBlocklist + "): blobid=" + block.blobid + ", blockid = "
						+ block.blockid);
			}
		} else {
			logger.info(this.partitionTxidLogStr + "List(" + this.key_partitionBlocklist + ") is null or empty");
		}
		String blobNameFormat = ConfigProperties.getProperty("blobNameFormat");
		String blobname = String.format(blobNameFormat, this.partitionIndex, block.blobid);
		String blobidAndBlockidStr = block.build(blobname);
		this.blockList.add(blobidAndBlockidStr);		

		if (LogSetting.LOG_BLOCK && LogSetting.LOG_METHOD_END) {
			logger.info(this.partitionTxidLogStr + "getFirstBlockInLastFailedBatch returns blobid=" + block.blobid + ", blockid=" + block.blockid);
			logger.info(this.partitionTxidLogStr + "getFirstBlockInLastFailedBatch End");
		}
		return block;
	}

	public void persistState() {
		if (LogSetting.LOG_BLOCK && LogSetting.LOG_METHOD_BEGIN) {
			logger.info(this.partitionTxidLogStr + "persistState Begin");
		}

		Redis.set(this.partitionTxidKeyStr, String.valueOf(this.txid));
		Redis.setList(this.key_partitionBlocklist, this.blockList);

		if (LogSetting.LOG_PERSIST) {
			logger.info(this.partitionTxidLogStr + "set(" + this.partitionTxidKeyStr + ", " + this.txid + ")");
			for (String s : this.blockList) {
				logger.info(this.partitionTxidLogStr + "addToList(" + this.key_partitionBlocklist + ", " + s + ")");
			}
		}

		if (LogSetting.LOG_BLOCK && LogSetting.LOG_METHOD_END) {
			logger.info(this.partitionTxidLogStr + "this.partition_tx_logStr + persistState End");
		}
	}
}
