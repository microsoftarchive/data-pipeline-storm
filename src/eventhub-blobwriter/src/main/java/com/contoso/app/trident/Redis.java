// Copyright (c) Microsoft Corporation. All rights reserved. See License.txt in the project root for license information.

package com.contoso.app.trident;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.topology.FailedException;
import redis.clients.jedis.Jedis;

@SuppressWarnings("unused")
public class Redis {
	private static String host;
	private static String password;
	private static int port;
	private static int timeout;
	private static boolean useSSL = true;
	// private static Jedis jedis = null;

	static {
		host = ConfigProperties.getProperty("redis.host");
		password = ConfigProperties.getProperty("redis.password");
		port = Integer.parseInt(ConfigProperties.getProperty("redis.port"));
		timeout = Integer.parseInt(ConfigProperties.getProperty("redis.timeout"));
		// jedis = new Jedis(host, port, timeout, useSSL);
		// connect();
	}

	private static final Logger logger = (Logger) LoggerFactory.getLogger(Redis.class);

	// private static void connect() {
	// jedis = new Jedis(host, port, timeout, useSSL);
	// jedis.auth(password);
	// jedis.connect();
	// }

	static public void flushDB() {
		if (LogSetting.LOG_REDIS || LogSetting.LOG_METHOD_BEGIN) {
			logger.info("flushDB Begin");
		}
		try (Jedis jedis = new Jedis(host, port, timeout, useSSL)) {
			jedis.auth(password);
			jedis.connect();
			if (jedis.isConnected()) {
				jedis.flushDB();
			} else {
				if (LogSetting.LOG_REDIS) {
					logger.info("Error: can't cannect to Redis !!!!!");
				}
				throw new FailedException("can't cannect to Redis");
			}
		}
		if (LogSetting.LOG_REDIS || LogSetting.LOG_METHOD_END) {
			logger.info("flushDB End");
		}
	}

	static public String get(String key) {
		String value = null;
		if (LogSetting.LOG_REDIS || LogSetting.LOG_METHOD_BEGIN) {
			logger.info("get Begin params: key= " + key);
		}
		if (key != null) {

			try (Jedis jedis = new Jedis(host, port, timeout, useSSL)) {
				jedis.auth(password);
				jedis.connect();
				if (jedis.isConnected()) {
					value = jedis.get(key);
				} else {
					if (LogSetting.LOG_REDIS) {
						logger.info("Error: can't cannect to Redis !!!!!");
					}
					throw new FailedException("can't cannect to Redis");
				}
			}
		}
		if (LogSetting.LOG_REDIS || LogSetting.LOG_METHOD_END) {
			logger.info("get End returns " + value);
		}
		return value;
	}

	static public void set(String key, String value) {
		if (LogSetting.LOG_REDIS || LogSetting.LOG_METHOD_BEGIN) {
			logger.info("set Begin: key= " + key + " value= " + value);
		}
		if (key != null && value != null) {
			try (Jedis jedis = new Jedis(host, port, timeout, useSSL)) {
				jedis.auth(password);
				jedis.connect();
				if (jedis.isConnected()) {
					jedis.set(key, value);
				} else {
					if (LogSetting.LOG_REDIS) {
						logger.info("Error: can't cannect to Redis !!!!!");
					}
					throw new FailedException("can't cannect to Redis");
				}
			}
		}
		if (LogSetting.LOG_REDIS || LogSetting.LOG_METHOD_END) {
			logger.info("set End");
		}
	}

	static public List<String> getList(String key, long maxLength) {
		List<String> stringList = null;
		if (LogSetting.LOG_REDIS || LogSetting.LOG_METHOD_BEGIN) {
			logger.info("getList Begin with params: key= " + key + " maxLength= " + maxLength);
		}
		if (key != null && maxLength > 0) {

			try (Jedis jedis = new Jedis(host, port, timeout, useSSL)) {
				jedis.auth(password);
				jedis.connect();
				if (jedis.isConnected()) {
					stringList = jedis.lrange(key, 0, maxLength - 1);
				} else {
					if (LogSetting.LOG_REDIS) {
						logger.info("Error: can't cannect to Redis !!!!!");
					}
					throw new FailedException("can't cannect to Redis");
				}
			}
		}
		if (LogSetting.LOG_REDIS) {
			if (stringList == null || stringList.size() == 0) {
				logger.info("getList returns 0 record");
			} else {
				logger.info("getList returns " + stringList.size() + " record");
				for (String s : stringList) {
					logger.info("getList return record: " + s);
				}
			}
		}
		if (LogSetting.LOG_REDIS || LogSetting.LOG_METHOD_END) {
			logger.info("getList End");
		}
		return stringList;
	}

	static public void setList(String key, List<String> stringList) {
		if (LogSetting.LOG_REDIS || LogSetting.LOG_METHOD_BEGIN) {
			logger.info("setList Begin");
			logger.info("setList params: key= " + key);
			if (stringList == null || stringList.isEmpty()) {
				logger.info("setList params stringList is empty!");
			} else {
				for (String s : stringList) {
					logger.info("setList params stringlist: " + s);
				}
			}
		}
		if (key != null && stringList != null && !stringList.isEmpty()) {
			try (Jedis jedis = new Jedis(host, port, timeout, useSSL)) {
				jedis.auth(password);
				jedis.connect();
				if (jedis.isConnected()) {
					jedis.del(key);
					for (String str : stringList) {
						jedis.lpush(key, str);
					}
				} else {
					if (LogSetting.LOG_REDIS) {
						logger.info("Error: can't cannect to Redis !!!!!");
					}
					throw new FailedException("can't cannect to Redis");
				}
			}
		}
		if (LogSetting.LOG_REDIS || LogSetting.LOG_METHOD_END) {
			logger.info("setList End");
		}
	}
}