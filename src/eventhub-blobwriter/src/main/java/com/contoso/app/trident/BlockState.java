// Copyright (c) Microsoft Corporation. All rights reserved. See License.txt in the project root for license information.

package com.contoso.app.trident;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("unused")
public class BlockState {
	public Block block;
	public boolean needPersist = false;

	private int maxNumberOfBlocks = 50000;
	public String blobidBlockidStrFormat = null;
	public String blobNameFormat = null;
	public String blockIdStrFormat = null;
	private static final Logger logger = (Logger) LoggerFactory.getLogger(BlockState.class);
	private String redisHost = null;
	private String redisPassword = null;
	private String key_partition_txid;
	private String key_partitionBlocklist;

	private int partitionIndex;
	private long txid;
	private List<String> blocklist;
	private int maxBlockBytes;
	String partition_tx_logStr;

	public BlockState(int partitionIndex, long txid) {
		this.partition_tx_logStr = "p" + partitionIndex + "_tx" + txid + ": ";
		if ((LogSetting.LOG_INSTANCE || LogSetting.LOG_BLOCK) && LogSetting.LOG_METHOD_BEGIN) {
			logger.info(partition_tx_logStr + "Constructor Begin");
		}

		this.partitionIndex = partitionIndex;
		this.txid = txid;
		this.maxNumberOfBlocks = getMaxNumberOfblocks();
		this.maxBlockBytes = getMaxBlockBytes();
		
		this.blobidBlockidStrFormat = ConfigProperties.getProperty("blobidBlockidStrFormat");
		this.blobNameFormat = ConfigProperties.getProperty("blobNameFormat");
		this.blockIdStrFormat = ConfigProperties.getProperty("blockIdStrFormat");

		redisHost = Redis.getHost();
		redisPassword = Redis.getPassword();
		this.key_partition_txid = "p_" + String.valueOf(partitionIndex) + "_txid";
		this.key_partitionBlocklist = "p_" + String.valueOf(partitionIndex) + "_blocklist";
		this.blocklist = new ArrayList<String>();

		String lastTxidStr = Redis.get(redisHost, redisPassword, this.key_partition_txid);
		if (lastTxidStr == null) { // the very first time the topology is running
			this.block = getNewBlock();
		} else {
			long lastTxid = Long.parseLong(lastTxidStr);
			if (txid != lastTxid) { // a new batch, not a replay
				Block lastblock = getLastBlockInLastFailedBatch();
				this.block = lastblock.next();
			} else {// if(txid == lastTxid) a replay, overwrite old block
				this.block = getFirstBlockInLastFailedBatch();
			}
		}

		if ((LogSetting.LOG_INSTANCE || LogSetting.LOG_BLOCK) && LogSetting.LOG_METHOD_END) {
			logger.info(this.partition_tx_logStr + "Constructor End with blobid=" + this.block.blobid + ", blockid=" + this.block.blockid);
			logger.info(this.partition_tx_logStr + "Constructor End");
		}
	}
	public void persist() {
		if (LogSetting.LOG_BLOCK && LogSetting.LOG_METHOD_BEGIN) {
			logger.info(this.partition_tx_logStr + "persist Begin");
		}

		Redis.set(redisHost, redisPassword, this.key_partition_txid, String.valueOf(this.txid));
		Redis.setList(redisHost, redisPassword, this.key_partitionBlocklist, this.blocklist);

		if (LogSetting.LOG_PERSIST) {
			logger.info(this.partition_tx_logStr + "set(" + this.key_partition_txid + ", " + this.txid + ")");
			for (String s : this.blocklist) {
				logger.info(this.partition_tx_logStr + "addToList(" + this.key_partitionBlocklist + ", " + s + ")");
			}
		}

		if (LogSetting.LOG_BLOCK && LogSetting.LOG_METHOD_END) {
			logger.info(this.partition_tx_logStr + "this.partition_tx_logStr + persist End");
		}
	}
	private Block getNewBlock() {
		if (LogSetting.LOG_BLOCK && LogSetting.LOG_METHOD_BEGIN) {
			logger.info(this.partition_tx_logStr + "getNewBlock Begin");
		}

		Block block = new Block();
		block.build();

		if (LogSetting.LOG_BLOCK && LogSetting.LOG_METHOD_END) {
			logger.info(this.partition_tx_logStr + "getNewBlock End");
		}
		return block;
	}

	private Block getLastBlockInLastFailedBatch() {
		if (LogSetting.LOG_BLOCK && LogSetting.LOG_METHOD_BEGIN) {
			logger.info(this.partition_tx_logStr + "getLastBlockInLastFailedBatch Begin");
		}

		Block block = new Block();
		List<String> lastBlobidBlockidList = Redis.getList(redisHost, redisPassword, this.key_partitionBlocklist, 50000);
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
				logger.info(this.partition_tx_logStr + "Last record in List(" + this.key_partitionBlocklist + "): " + blockStr);
				logger.info(this.partition_tx_logStr + "Last record in List( " + this.key_partitionBlocklist + "): blobid=" + block.blobid + ", blockid = "
						+ block.blockid);
			}
		} else {
			if (LogSetting.LOG_GET_LAST_BLOCK) {
				logger.info(this.partition_tx_logStr + "List(" + this.key_partitionBlocklist + ") is null or empty");
			}
		}
		block.build();

		if (LogSetting.LOG_BLOCK && LogSetting.LOG_METHOD_END) {
			logger.info(this.partition_tx_logStr + "getLastBlockInLastFailedBatch returns blobid=" + block.blobid + ", blockid=" + block.blockid);
			logger.info(this.partition_tx_logStr + "getLastBlockInLastFailedBatch End");
		}
		return block;
	}
	private Block getFirstBlockInLastFailedBatch() {
		if (LogSetting.LOG_BLOCK && LogSetting.LOG_METHOD_BEGIN) {
			logger.info(this.partition_tx_logStr + "getFirstBlockInLastFailedBatch Begin");
		}

		Block block = new Block();
		List<String> lastblocks = Redis.getList(redisHost, redisPassword, this.key_partitionBlocklist, 50000);
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
				logger.info(this.partition_tx_logStr + "First record in List(" + this.key_partitionBlocklist + "): " + blockStr);
				logger.info(this.partition_tx_logStr + "First record in List(" + this.key_partitionBlocklist + "): blobid=" + block.blobid + ", blockid = "
						+ block.blockid);
			}
		} else {
			logger.info(this.partition_tx_logStr + "List(" + this.key_partitionBlocklist + ") is null or empty");
		}
		block.build();

		if (LogSetting.LOG_BLOCK && LogSetting.LOG_METHOD_END) {
			logger.info(this.partition_tx_logStr + "getFirstBlockInLastFailedBatch returns blobid=" + block.blobid + ", blockid=" + block.blockid);
			logger.info(this.partition_tx_logStr + "getFirstBlockInLastFailedBatch End");
		}
		return block;
	}
	private int getMaxNumberOfblocks() {
		if (LogSetting.LOG_BLOCK && LogSetting.LOG_METHOD_BEGIN) {
			logger.info(this.partition_tx_logStr + "getMaxNumberOfblocks Begin");
		}

		int maxNumberOfBlocks = 50000;
		String maxNumberOfBlocksStr = ConfigProperties.getProperty("storage.blob.block.number.max");
		if (maxNumberOfBlocksStr != null) {
			maxNumberOfBlocks = Integer.parseInt(maxNumberOfBlocksStr);
		}

		if (LogSetting.LOG_BLOCK && LogSetting.LOG_METHOD_END) {
			logger.info(this.partition_tx_logStr + "getMaxNumberOfblocks returns " + maxNumberOfBlocks);
			logger.info(this.partition_tx_logStr + "getMaxNumberOfblocks End");
		}

		return maxNumberOfBlocks;
	}
	private int getMaxBlockBytes() {
		if (LogSetting.LOG_BLOCK && LogSetting.LOG_METHOD_BEGIN) {
			logger.info(this.partition_tx_logStr + "getMaxBlockBytes Begin");
		}

		int maxBlockBytes = 1024;
		String maxBlockBytesStr = ConfigProperties.getProperty("storage.blob.block.bytes.max");
		if (maxBlockBytesStr != null) {
			maxBlockBytes = Integer.parseInt(maxBlockBytesStr);
		}

		if (LogSetting.LOG_BLOCK && LogSetting.LOG_METHOD_END) {
			logger.info(this.partition_tx_logStr + "getMaxBlockBytes returns " + maxBlockBytes);
			logger.info(this.partition_tx_logStr + "getMaxBlockBytes End");
		}
		return maxBlockBytes;
	}

	public class Block {
		public int blobid = 1;
		public int blockid = 1;
		public String blobname;
		public String blockidStr;
		public String blockdata;

		public Block() {
			if (LogSetting.LOG_BLOCK && LogSetting.LOG_METHOD_BEGIN) {
				logger.info(BlockState.this.partition_tx_logStr + "Block Constructor Begin");
			}

			this.blobid = 1;
			this.blockid = 1;
			this.blockdata = "";

			if (LogSetting.LOG_BLOCK && LogSetting.LOG_METHOD_END) {
				logger.info(BlockState.this.partition_tx_logStr + "Block Constructor End");
			}
		}

		public void addData(String msg) {
			if (LogSetting.LOG_MESSAGE && LogSetting.LOG_METHOD_BEGIN) {
				logger.info(BlockState.this.partition_tx_logStr + "Block.addData Begin");
			}

			this.blockdata = this.blockdata + msg;

			if (LogSetting.LOG_MESSAGE && LogSetting.LOG_METHOD_END) {
				logger.info(BlockState.this.partition_tx_logStr + "Block.addData End");
			}
		}

		public boolean isMessageSizeWithnLimit(String msg) {
			if (LogSetting.LOG_MESSAGE && LogSetting.LOG_METHOD_BEGIN) {
				logger.info(BlockState.this.partition_tx_logStr + "Block.isMessageSizeWithnLimit Begin");
			}

			boolean result = false;
			if (msg.getBytes().length <= maxBlockBytes) {
				result = true;
			}

			if (LogSetting.LOG_MESSAGE && LogSetting.LOG_METHOD_END) {
				logger.info(BlockState.this.partition_tx_logStr + "Block.isMessageSizeWithnLimit End");
			}
			return result;
		}

		public boolean willMessageFitCurrentBlock(String msg) {
			if (LogSetting.LOG_MESSAGE && LogSetting.LOG_METHOD_BEGIN) {
				logger.info(BlockState.this.partition_tx_logStr + "Block.willMessageFitCurrentBlock Begin");
			}
			boolean result = false;
			int byteSize = (this.blockdata + msg).getBytes().length;
			if (byteSize <= maxBlockBytes) {
				result = true;
			}
			if (LogSetting.LOG_MESSAGE && LogSetting.LOG_METHOD_END) {
				logger.info(BlockState.this.partition_tx_logStr + "Block.willMessageFitCurrentBlock End");
			}
			return result;
		}

		public void upload() {
			if (LogSetting.LOG_BLOCK && LogSetting.LOG_METHOD_BEGIN) {
				logger.info(BlockState.this.partition_tx_logStr + "Block.upload Begin");
			}

			BlobWriter.upload(this.blobname, this.blockidStr, this.blockdata);

			if (LogSetting.LOG_BLOCK && LogSetting.LOG_METHOD_END) {
				logger.info(BlockState.this.partition_tx_logStr + "BlobState.upload End");
			}
		}

		private void build() {
			if (LogSetting.LOG_BLOCK && LogSetting.LOG_METHOD_BEGIN) {
				logger.info(BlockState.this.partition_tx_logStr + "Block.build Begin");
			}

			this.blockdata = new String("");
			this.blobname = String.format(BlockState.this.blobNameFormat, BlockState.this.partitionIndex, this.blobid);
			this.blockidStr = String.format(BlockState.this.blockIdStrFormat, this.blockid);
			String blobidBlockidStr = String.format(BlockState.this.blobidBlockidStrFormat, this.blobid, this.blockid);
			BlockState.this.blocklist.add(blobidBlockidStr);

			if (LogSetting.LOG_BLOCK && LogSetting.LOG_METHOD_END) {
				logger.info(BlockState.this.partition_tx_logStr + "Block.build End");
			}
		}

		public Block next() {
			Block current = this;
			if (LogSetting.LOG_BLOCK && LogSetting.LOG_METHOD_BEGIN) {
				logger.info(BlockState.this.partition_tx_logStr + "Block.next Begin");
			}

			Block nextBlock = new Block();
			if (current.blockid < maxNumberOfBlocks) {
				nextBlock.blobid = current.blobid;
				nextBlock.blockid = current.blockid + 1;
			} else {
				nextBlock.blobid = current.blobid + 1;
				nextBlock.blockid = 1;
			}
			nextBlock.build();

			if (LogSetting.LOG_BLOCK && LogSetting.LOG_METHOD_END) {
				logger.info(BlockState.this.partition_tx_logStr + "Block.next returns blobid=" + nextBlock.blobid + ", blockid=" + nextBlock.blockid);
				logger.info(BlockState.this.partition_tx_logStr + "Block.next End");
			}

			return nextBlock;
		}

	}
}
