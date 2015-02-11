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
	private static Jedis jedis = null;

	static {
		host = ConfigProperties.getProperty("redis.host");
		password = ConfigProperties.getProperty("redis.password");
		port = Integer.parseInt(ConfigProperties.getProperty("redis.port"));
		timeout = Integer.parseInt(ConfigProperties.getProperty("redis.timeout"));
		connect();
	}

	private static final Logger logger = (Logger) LoggerFactory.getLogger(Redis.class);

	private static void connect() {
		jedis = new Jedis(host, port, timeout, useSSL);
		jedis.auth(password);
		jedis.connect();
	}

	static private Object callJedis(String operation, String key, Object value) {
		Object result = null;
		if (!jedis.isConnected()) {
			connect();
		}
		if (jedis.isConnected()) {
			switch (operation) {
			case "flushDB":
				result = jedis.flushDB();
				break;
			case "get":
				result = jedis.get(key);
				break;
			case "set":
				result = jedis.set(key, (String) value);
				break;
			case "getList":
				int maxLength = (int) value;
				result = jedis.lrange(key, 0, maxLength - 1);
				break;
			case "setList":
				jedis.del(key);
				@SuppressWarnings("unchecked")
				List<String> stringList = (List<String>) value;
				for (String str : stringList) {
					jedis.lpush(key, str);
				}
				break;
			default:
				break;
			}
		} else {
			if (LogSetting.LOG_REDIS) {
				logger.info("Error: can't cannect to Redis !!!!!");
				throw new FailedException("can't cannect to Redis");
			}
		}
		return result;
	}

	static public void flushDB() {
		if (LogSetting.LOG_REDIS && LogSetting.LOG_METHOD_BEGIN) {
			logger.info("flushDB Begin");
		}

		Redis.callJedis("flushDB", "", null);

		if (LogSetting.LOG_REDIS && LogSetting.LOG_METHOD_END) {
			logger.info("flushDB End");
		}
	}

	static public String get(String key) {
		if (LogSetting.LOG_REDIS && LogSetting.LOG_METHOD_BEGIN) {
			logger.info("get Begin");
			logger.info("get params: key= " + key);
		}

		String value = (String) Redis.callJedis("get", key, null);

		if (LogSetting.LOG_REDIS && LogSetting.LOG_METHOD_END) {
			logger.info("get returns " + value);
			logger.info("get Begin");
		}
		return value;
	}

	static public void set(String key, String value) {
		if (LogSetting.LOG_REDIS && LogSetting.LOG_METHOD_BEGIN) {
			logger.info("set Begin");
			logger.info("set params: host= " + host + " key= " + key + " value= " + value);
		}

		Redis.callJedis("set", key, null);

		if (LogSetting.LOG_REDIS && LogSetting.LOG_METHOD_END) {
			logger.info("set End");
		}
	}

	static public List<String> getList(String key, int maxLength) {
		if (LogSetting.LOG_REDIS && LogSetting.LOG_METHOD_BEGIN) {
			logger.info("getList Begin");
			logger.info("getList params: key= " + key + " maxLength= " + maxLength);
		}
		@SuppressWarnings("unchecked")
		List<String> stringList = (List<String>) Redis.callJedis("getList", key, maxLength);
		if (LogSetting.LOG_REDIS) {
			if (stringList == null) {
				logger.info("getList returns 0 record");
			} else {
				for (String s : stringList) {
					logger.info("getList returns: " + s);
				}
			}
		}
		if (LogSetting.LOG_REDIS && LogSetting.LOG_METHOD_END) {
			logger.info("getList End");
		}
		return stringList;
	}

	static public void setList(String key, List<String> stringList) {
		if (LogSetting.LOG_REDIS && LogSetting.LOG_METHOD_BEGIN) {
			logger.info("setList Begin");
			logger.info("getList params: key= " + key);
		}
		if (stringList == null || stringList.isEmpty()) {
			if (LogSetting.LOG_REDIS) {
				logger.info("setList params stringList is empty  !!!!!!");
			}
		} else {
			if (LogSetting.LOG_REDIS) {
				for (String s : stringList) {
					logger.info("setList params stringlist: " + s);
				}
			}
			Redis.callJedis("setList", key, stringList);
		}
		if (LogSetting.LOG_REDIS && LogSetting.LOG_METHOD_END) {
			logger.info("setList End");
		}
	}
}