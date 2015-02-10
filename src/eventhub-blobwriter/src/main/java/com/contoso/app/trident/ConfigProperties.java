package com.contoso.app.trident;

import java.io.IOException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ConfigProperties {
	private static final Logger logger = (Logger) LoggerFactory.getLogger(ConfigProperties.class);
	private static final Properties properties = new Properties();
	static {
		try {
			properties.load(ConfigProperties.class.getClassLoader().getResourceAsStream(
					"Config.properties"));
		} catch (IOException e) {
			throw new ExceptionInInitializerError(e);
		}
	}

	public static String getProperty(String key) {
		return properties.getProperty(key);
	}
	
	public static int getMaxNumberOfblocks() {
		if (LogSetting.LOG_METHOD_BEGIN) {
			logger.info("getMaxNumberOfblocks Begin");
		}

		int maxNumberOfBlocks = 50000;
		String maxNumberOfBlocksStr = ConfigProperties.getProperty("storage.blob.block.number.max");
		if (maxNumberOfBlocksStr != null) {
			maxNumberOfBlocks = Integer.parseInt(maxNumberOfBlocksStr);
		}

		if (LogSetting.LOG_METHOD_END) {
			logger.info("getMaxNumberOfblocks returns " + maxNumberOfBlocks);
			logger.info("getMaxNumberOfblocks End");
		}

		return maxNumberOfBlocks;
	}
	public static int getMaxBlockBytes() {
		if (LogSetting.LOG_METHOD_BEGIN) {
			logger.info("getMaxBlockBytes Begin");
		}

		int maxBlockBytes = 1024;
		String maxBlockBytesStr = ConfigProperties.getProperty("storage.blob.block.bytes.max");
		if (maxBlockBytesStr != null) {
			maxBlockBytes = Integer.parseInt(maxBlockBytesStr);
		}

		if (LogSetting.LOG_METHOD_END) {
			logger.info("getMaxBlockBytes returns " + maxBlockBytes);
			logger.info("getMaxBlockBytes End");
		}
		return maxBlockBytes;
	}


}
