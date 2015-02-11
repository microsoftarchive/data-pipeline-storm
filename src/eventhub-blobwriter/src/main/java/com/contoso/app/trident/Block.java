// Copyright (c) Microsoft Corporation. All rights reserved. See License.txt in the project root for license information.

package com.contoso.app.trident;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("unused")
public class Block {
	private static final Logger logger = (Logger) LoggerFactory.getLogger(Block.class);

	public int blobid = 1;
	public int blockid = 1;
	public String blobname;
	public String blockidStr;
	public String blockdata;
	int maxBlockBytes = ConfigProperties.getMaxBlockBytes();

	public Block() {
		if (LogSetting.LOG_BLOCK && LogSetting.LOG_METHOD_BEGIN) {
			logger.info("Block Constructor Begin");
		}

		this.blobid = 1;
		this.blockid = 1;
		this.blockdata = "";

		if (LogSetting.LOG_BLOCK && LogSetting.LOG_METHOD_END) {
			logger.info("Block Constructor End");
		}
	}

	public void addData(String msg) {
		if (LogSetting.LOG_MESSAGE && LogSetting.LOG_METHOD_BEGIN) {
			logger.info("Block.addData Begin");
		}

		this.blockdata = this.blockdata + msg;

		if (LogSetting.LOG_MESSAGE && LogSetting.LOG_METHOD_END) {
			logger.info("Block.addData End");
		}
	}

	public boolean isMessageSizeWithnLimit(String msg) {
		if (LogSetting.LOG_MESSAGE && LogSetting.LOG_METHOD_BEGIN) {
			logger.info("Block.isMessageSizeWithnLimit Begin");
		}

		boolean result = false;
		if (msg.getBytes().length <= maxBlockBytes) {
			result = true;
		}

		if (LogSetting.LOG_MESSAGE && LogSetting.LOG_METHOD_END) {
			logger.info("Block.isMessageSizeWithnLimit End");
		}
		return result;
	}

	public boolean willMessageFitCurrentBlock(String msg) {
		if (LogSetting.LOG_MESSAGE && LogSetting.LOG_METHOD_BEGIN) {
			logger.info("Block.willMessageFitCurrentBlock Begin");
		}
		boolean result = false;
		int byteSize = (this.blockdata + msg).getBytes().length;
		if (byteSize <= maxBlockBytes) {
			result = true;
		}
		if (LogSetting.LOG_MESSAGE && LogSetting.LOG_METHOD_END) {
			logger.info("Block.willMessageFitCurrentBlock End");
		}
		return result;
	}

	public void upload() {
		if (LogSetting.LOG_BLOCK && LogSetting.LOG_METHOD_BEGIN) {
			logger.info("Block.upload Begin");
		}

		BlobWriter.upload(this.blobname, this.blockidStr, this.blockdata);

		if (LogSetting.LOG_BLOCK && LogSetting.LOG_METHOD_END) {
			logger.info("BlobState.upload End");
		}
	}

	public String build(String blobname) {
		if (LogSetting.LOG_BLOCK && LogSetting.LOG_METHOD_BEGIN) {
			logger.info("Block.build Begin");
		}

		this.blockdata = new String("");
		this.blobname = blobname;

		String blockIdStrFormat = ConfigProperties.getProperty("blockIdStrFormat");
		this.blockidStr = String.format(blockIdStrFormat, this.blockid);
		String blobidBlockidStrFormat = ConfigProperties.getProperty("blobidBlockidStrFormat");
		String blobidAndBlockidStr = String.format(blobidBlockidStrFormat, this.blobid, this.blockid);

		if (LogSetting.LOG_BLOCK && LogSetting.LOG_METHOD_END) {
			logger.info("Block.build End");
		}
		return blobidAndBlockidStr;
	}
}