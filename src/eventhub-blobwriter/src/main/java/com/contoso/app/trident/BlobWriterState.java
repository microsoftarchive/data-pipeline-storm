package com.contoso.app.trident;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;
import backtype.storm.topology.FailedException;

public class BlobWriterState {
	static public void flush() {
		Redis.flush();
	}

	static public String get(String key) {
		return Redis.get(key);
	}

	static public List<String> getList(String key, long maxLength) {
		return Redis.getList(key, maxLength);
	}

	static public void setState(String key, String value, String keyToList, List<String> stringList) {
		Redis.setState(key, value, keyToList, stringList);
	}

	private static class Redis {
		private static final Logger logger = (Logger) LoggerFactory.getLogger(Redis.class);
		private static String host;
		private static String password;
		private static int port;
		private static int timeout;
		private static boolean useSSL = true;

		static {
			host = ConfigProperties.getProperty("redis.host");
			password = ConfigProperties.getProperty("redis.password");
			port = Integer.parseInt(ConfigProperties.getProperty("redis.port"));
			timeout = Integer.parseInt(ConfigProperties.getProperty("redis.timeout"));
		}

		private static void flush() {
			if (LogSetting.LOG_REDIS) {
				logger.info("flushDB Begin");
			}
			try (Jedis jedis = new Jedis(host, port, timeout, useSSL)) {
				jedis.auth(password);
				jedis.connect();
				if (jedis.isConnected()) {
					// TODO: check redis doc to see if we need to loop through all servers
					jedis.flushDB();
				} else {
					if (LogSetting.LOG_REDIS) {
						logger.info("Error: can't cannect to Redis !!!!!");
					}
					throw new FailedException("can't cannect to Redis");
				}
			}
			if (LogSetting.LOG_REDIS) {
				logger.info("flushDB End");
			}
		}

		private static String get(String key) {
			String value = null;
			if (LogSetting.LOG_REDIS) {
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
			if (LogSetting.LOG_REDIS) {
				logger.info("get End returns " + value);
			}
			return value;
		}

		private static List<String> getList(String key, long maxLength) {
			List<String> stringList = null;
			if (LogSetting.LOG_REDIS) {
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
			if (LogSetting.LOG_REDIS) {
				logger.info("getList End");
			}
			return stringList;
		}
		
		static void setState(String key, String value, String keyToList, List<String> stringList) {
			if (LogSetting.LOG_REDIS) {
				logger.info("setState Begin");
				logger.info("setState params: key= " + key);
				if (key == null || value == null || keyToList == null || stringList == null || stringList.isEmpty()) {
					logger.info("setState params stringList is empty!");
				} else {
					logger.info("setState Begin: key= " + key + " value= " + value + " keyToList= " + keyToList);
					for (String s : stringList) {
						logger.info("setState params stringlist: " + s);
					}
				}
			}
			if (key != null && value != null && keyToList != null && stringList != null && !stringList.isEmpty()) {
				try (Jedis jedis = new Jedis(host, port, timeout, useSSL)) {
					jedis.auth(password);
					jedis.connect();
					if (jedis.isConnected()) {
						Transaction trans = jedis.multi();
						try {
							trans.set(key, value);
							trans.del(keyToList);
							for (String str : stringList) {
								trans.lpush(keyToList, str);
							}
							trans.exec();
						} catch (Exception e) {
							trans.discard();
							throw new FailedException(e.getMessage());
						}

					} else {
						if (LogSetting.LOG_REDIS) {
							logger.info("Error: can't cannect to Redis !!!!!");
						}
						throw new FailedException("can't cannect to Redis");
					}
				}
			}
			if (LogSetting.LOG_REDIS) {
				logger.info("setList End");
			}
		}
	}
}
