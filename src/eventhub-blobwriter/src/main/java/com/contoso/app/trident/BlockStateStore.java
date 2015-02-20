package com.contoso.app.trident;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;
import backtype.storm.topology.FailedException;

public class BlockStateStore {
	// TODO: replace Redis with zookeeper to store BlobWriter State
	static public String get(String key) {
		return Redis.get(key);
	}

	static public void setState(String key1, String value1, String key2, String value2, String key3, String value3) {
		Redis.setState(key1, value1, key2, value2, key3, value3);
	}
	
	static public void clearState(String key1, String key2,String key3) {
		Redis.clearState(key1, key2,key3);
	}

	private static class Redis {
		private static final Logger logger = (Logger) LoggerFactory.getLogger(Redis.class);
		private static String host = null;
		private static String password= null;
		private static int port = -1;
		private static int timeout = -1;
		private static boolean useSSL = true;

		static {
			host = ConfigProperties.getProperty("redis.host");
			password = ConfigProperties.getProperty("redis.password");
			port = Integer.parseInt(ConfigProperties.getProperty("redis.port"));
			timeout = Integer.parseInt(ConfigProperties.getProperty("redis.timeout"));
			if(host == null)
			{
				throw new ExceptionInInitializerError("Error: host is missing" );
			}
			if(password == null)
			{
				throw new ExceptionInInitializerError("Error: password is missing" );
			}
			if(port == -1)
			{
				throw new ExceptionInInitializerError("Error: port is missing" );
			}
			if(timeout == -1)
			{
				throw new ExceptionInInitializerError("Error: timeout is missing" );
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

		public static void clearState(String key1, String key2, String key3) {
			if (LogSetting.LOG_REDIS) {
				logger.info("clearState Begin");
				logger.info("clear keys " + key1 + ", "+ key2 + ", "+ key3 + ", ");
			}
			if (key1 != null && key2 != null && key3 != null) {
				try (Jedis jedis = new Jedis(host, port, timeout, useSSL)) {
					jedis.auth(password);
					jedis.connect();
					if (jedis.isConnected()) {
						Transaction trans = jedis.multi();
						try {
							trans.del(key1);
							trans.del(key2);
							trans.del(key3);
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
				logger.info("clearState End");
			}
		}

		static void setState(String key1, String value1, String key2, String value2, String key3, String value3) {
			if (LogSetting.LOG_REDIS) {
				logger.info("setState Begin");
			}
			if (key1 != null && value1 != null ) {
				try (Jedis jedis = new Jedis(host, port, timeout, useSSL)) {
					jedis.auth(password);
					jedis.connect();
					if (jedis.isConnected()) {
						Transaction trans = jedis.multi();
						try {
							trans.set(key1, value1);
							trans.set(key2, value2);
							trans.set(key3, value3);
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
