// Copyright (c) Microsoft Corporation. All rights reserved. See License.txt in the project root for license information.

package com.contoso.app.trident;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Block {
	private static final Logger logger = (Logger) LoggerFactory.getLogger(Block.class);
	private static String blockidFormatter = "%05d";
	private static String blobidAndblockidFormatter = "%05d_%05d";
	private static int maxBlockBytes = 4194304;
	static {
		String blockidFormatterStr = ConfigProperties.getProperty("BLOCKID_FORMATTER");
		if (blockidFormatterStr != null) {
			blockidFormatter = blockidFormatterStr;
		}
		String blobidAndblockidFormatterStr = ConfigProperties.getProperty("BLOBID_BLOCKID_FORMATTER");
		if (blobidAndblockidFormatterStr != null) {
			blobidAndblockidFormatter = blobidAndblockidFormatterStr;
		}

		String maxBlockBytesStr = ConfigProperties.getProperty("storage.blob.block.bytes.max");
		if (maxBlockBytesStr != null) {
			int maxBlockBytesStrInt = Integer.parseInt(maxBlockBytesStr);
			if (maxBlockBytesStrInt > 0 && maxBlockBytesStrInt <= 4194304) {
				maxBlockBytes = maxBlockBytesStrInt;
			}
		}
	}

	public int blobid = 1;
	public int blockid = 1;
	public String blobname;
	public String blockidStr;
	public String blockdata;
	public String blobidAndBlockidStr;

	public Block() {
		if (LogSetting.LOG_BLOCK) {
			logger.info("Block Constructor Begin");
		}
		blobid = 1;
		blockid = 1;
		blockdata = "";
		if (LogSetting.LOG_BLOCK) {
			logger.info("Block Constructor End");
		}
	}

	public void addData(String msg) {
		if (LogSetting.LOG_MESSAGE) {
			logger.info("Block.addData Begin");
		}
		blockdata = blockdata + msg;
		if (LogSetting.LOG_MESSAGE) {
			logger.info("Block.addData End");
		}
	}

	public static boolean isMessageSizeWithnLimit(String msg) {
		if (LogSetting.LOG_MESSAGE) {
			logger.info("Block.isMessageSizeWithnLimit Begin");
		}
		boolean result = false;
		if (msg.getBytes().length <= maxBlockBytes) {
			result = true;
		}
		if (LogSetting.LOG_MESSAGE) {
			logger.info("Block.isMessageSizeWithnLimit End");
		}
		return result;
	}

	public boolean willMessageFitCurrentBlock(String msg) {
		if (LogSetting.LOG_MESSAGE) {
			logger.info("Block.willMessageFitCurrentBlock Begin");
		}
		boolean result = false;
		int byteSize = (blockdata + msg).getBytes().length;
		if (byteSize <= maxBlockBytes) {
			result = true;
		}
		if (LogSetting.LOG_MESSAGE) {
			logger.info("Block.willMessageFitCurrentBlock End");
		}
		return result;
	}

	public void upload() {
		if (LogSetting.LOG_BLOCK) {
			logger.info("Block.upload Begin");
		}
		BlobWriter.upload(blobname, blockidStr, blockdata);
		if (LogSetting.LOG_BLOCK) {
			logger.info("BlobState.upload End");
		}
	}

	public void build(String name) {
		if (LogSetting.LOG_BLOCK) {
			logger.info("Block.build Begin");
		}
		blockdata = new String("");
		blobname = name;
		blockidStr = String.format(blockidFormatter, blockid);
		blobidAndBlockidStr = String.format(blobidAndblockidFormatter, blobid, blockid);
		if (LogSetting.LOG_BLOCK) {
			logger.info("Block.build End");
		}
	}
}