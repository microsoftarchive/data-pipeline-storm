// Copyright (c) Microsoft Corporation. All rights reserved. See License.txt in the project root for license information.

package com.contoso.app.trident;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Block {
	private static final Logger logger = (Logger) LoggerFactory.getLogger(Block.class);
	private static String BLOCKID_FORMATTER = "%05d";
	private static String BLOBID_BLOCKID_FORMATTER = "%05d_%05d";
	private static int MAX_BLOCK_BYTES = 4194304;
	static {
		String _BLOCKID_FORMATTER = ConfigProperties.getProperty("BLOCKID_FORMATTER");
		if (_BLOCKID_FORMATTER != null) {
			BLOCKID_FORMATTER = _BLOCKID_FORMATTER;
		}
		String _BLOBID_BLOCKID_FORMATTER = ConfigProperties.getProperty("BLOBID_BLOCKID_FORMATTER");
		if (_BLOBID_BLOCKID_FORMATTER != null) {
			BLOBID_BLOCKID_FORMATTER = _BLOBID_BLOCKID_FORMATTER;
		}

		String _MAX_BLOCK_BYTES = ConfigProperties.getProperty("storage.blob.block.bytes.max");
		if (_MAX_BLOCK_BYTES != null) {
			int i_MAX_BLOCK_BYTES = Integer.parseInt(_MAX_BLOCK_BYTES);
			if (i_MAX_BLOCK_BYTES > 0 && i_MAX_BLOCK_BYTES <= 4194304) {
				MAX_BLOCK_BYTES = i_MAX_BLOCK_BYTES;
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
		this.blobid = 1;
		this.blockid = 1;
		this.blockdata = "";
		if (LogSetting.LOG_BLOCK) {
			logger.info("Block Constructor End");
		}
	}

	public void addData(String msg) {
		if (LogSetting.LOG_MESSAGE) {
			logger.info("Block.addData Begin");
		}
		this.blockdata = this.blockdata + msg;
		if (LogSetting.LOG_MESSAGE) {
			logger.info("Block.addData End");
		}
	}

	public static boolean isMessageSizeWithnLimit(String msg) {
		if (LogSetting.LOG_MESSAGE) {
			logger.info("Block.isMessageSizeWithnLimit Begin");
		}
		boolean result = false;
		if (msg.getBytes().length <= MAX_BLOCK_BYTES) {
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
		int byteSize = (this.blockdata + msg).getBytes().length;
		if (byteSize <= MAX_BLOCK_BYTES) {
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
		BlobWriter.upload(this.blobname, this.blockidStr, this.blockdata);
		if (LogSetting.LOG_BLOCK) {
			logger.info("BlobState.upload End");
		}
	}

	public void build(String blobname) {
		if (LogSetting.LOG_BLOCK) {
			logger.info("Block.build Begin");
		}
		this.blockdata = new String("");
		this.blobname = blobname;
		this.blockidStr = String.format(BLOCKID_FORMATTER, this.blockid);
		this.blobidAndBlockidStr = String.format(BLOBID_BLOCKID_FORMATTER, this.blobid, this.blockid);
		if (LogSetting.LOG_BLOCK) {
			logger.info("Block.build End");
		}
	}
}