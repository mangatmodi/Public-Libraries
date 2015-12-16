package com.bidstalk.tools.common;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;

import org.apache.commons.io.FileUtils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.exceptions.JedisConnectionException;

import com.google.common.base.Joiner;

/**
 * 
 * @author mangat This class provides client to a Redis Master
 */

public class RedisHost {
	ArrayList<Jedis> connections;
	HashMap<Jedis, Integer> counts;

	public RedisHost(int id, String redisHost, int redisPort, String comment) {
		super();
		this.id = id;
		this.redisHost = redisHost;
		this.redisPort = redisPort;
		this.comment = comment;
		connections = new ArrayList<>(53);
		counts = new HashMap<>(53);

	}

	@Override
	public String toString() {
		return redisHost + ":" + redisPort + " " + comment;

	}

	// protected void finalize() throws Throwable {
	// if(jedis!=null){
	//
	// }
	// }
	//
	protected String redisHost;
	protected int redisPort;
	protected int id;
	protected String comment = "";

	public synchronized Jedis getJedis() {
		Jedis jedis = null;
		if (connections.size() < 53) {
			jedis = new Jedis(redisHost, redisPort);
			connections.add(jedis);
			counts.put(jedis, 1);

		} else {
			int idx = ((int) (Math.random() * 999996541) % 53); // Co-Primes
			jedis = connections.get(idx);
			if (jedis == null || (!counts.containsKey(jedis))) {
				counts.remove(jedis);
				jedis = new Jedis(redisHost, redisPort);
				connections.add(idx, jedis);
				counts.put(jedis, 0);
			}
			counts.put(jedis, counts.get(jedis) + 1);
		}
		if (!jedis.isConnected()) {
			jedis.connect();
		}
		System.out.println("Making Jedis connection at:" + redisHost + ":" + redisPort);
		return jedis;
	}

	public String getRedisHost() {
		return this.redisHost;
	}

	public int getRedisPort() {
		return this.redisPort;
	}

	public synchronized void close(Jedis jedis) {
		if (!counts.containsKey(jedis)) {
			if (jedis != null) {
				jedis.close();
			}
		} else if (counts.get(jedis) == 1) {
			counts.remove(jedis);
			jedis.close();
		} else {
			counts.put(jedis, counts.get(jedis) - 1);
		}
	}

	public boolean removeFromRedis(Set<String> redisKeys) throws InterruptedException {
		int repeat = 0;
		while (repeat < 2) {
			Jedis jedis = getJedis();
			try {
				final Pipeline pipeline = jedis.pipelined();
				for (String redisKey : redisKeys) {
					if (redisKey != null) {
						pipeline.del(redisKey);
					}
				}
				List<Object> results = pipeline.syncAndReturnAll();
				return true;
			} catch (Exception e) {
				repeat++;
				if (repeat == 3) {
					// Retry failed
					Utils.processFailure(
							"Exception while removing from redis at: " + redisHost + ":" + redisPort + "\n"
									+ e.getMessage(),
							RedisManager.logger, Level.SEVERE, true, Constants.SERVERALERTS_MAILING_LIST,
							"**Redis Manager Exception**");

					return false;
				}
				Thread.sleep(5 * 1000);
			} finally {
				close(jedis);
			}
		}
		return false;
	}

	public <T> boolean insertCommaSeparatedIntoRedis(Map<String, Set<T>> redisData, Integer expirySeconds)
			throws InterruptedException {
		if (redisData == null || redisData.size() == 0) {
			return true;
		}
		System.out.println("Updating for Redis at: " + redisHost + ":" + redisPort);
		int repeat = 0;
		while (repeat < 2) {
			// If fails try is one more time after 5 seconds
			Jedis jedis = getJedis();
			try {
				Joiner commaJoiner = Joiner.on(",").skipNulls();

				Pipeline pipeline = jedis.pipelined();
				for (Map.Entry<String, Set<T>> entry : redisData.entrySet()) {
					String redisKey = entry.getKey();
					String redisValue = commaJoiner.join(entry.getValue());
					if (expirySeconds == null || expirySeconds < 1)
						pipeline.set(redisKey, redisValue);
					else
						pipeline.setex(redisKey, expirySeconds, redisValue);
				}
				List<Object> results = pipeline.syncAndReturnAll();
				return Utils.analyseRedisSetResults(results);
			} catch (Exception e) {
				repeat++;
				if (repeat == 3) {
					// Retry failed
					Utils.processFailure(
							"Exception while inserting into redis at: " + redisHost + ":" + redisPort + "\n"
									+ e.getMessage(),
							RedisManager.logger, Level.SEVERE, true, Constants.SERVERALERTS_MAILING_LIST,
							"**Redis Manager Exception**");
					return false;
				}
				Thread.sleep(5 * 1000);
			} finally {
				close(jedis);
			}
		}
		return false;
	}

	public boolean insertIntoRedis(Map<String, String> redisData, Integer expirySeconds) throws InterruptedException {
		int repeat = 0;
		while (repeat < 2) {
			Jedis jedis = getJedis();
			try {
				final Pipeline pipeline = jedis.pipelined();
				for (Map.Entry<String, String> entry : redisData.entrySet()) {
					if (expirySeconds == null || expirySeconds < 1)
						pipeline.set(entry.getKey(), entry.getValue());
					else
						pipeline.setex(entry.getKey(), expirySeconds, entry.getValue());

				}
				List<Object> results = pipeline.syncAndReturnAll();
				repeat = 3;
				return Utils.analyseRedisSetResults(results);
			} catch (Exception e) {
				repeat++;
				if (repeat == 3) {
					// Retry failed
					Utils.processFailure(
							"Exception while inserting into redis at: " + redisHost + ":" + redisPort + "\n"
									+ e.getMessage(),
							RedisManager.logger, Level.SEVERE, true, Constants.SERVERALERTS_MAILING_LIST,
							"**Redis Manager Exception**");

					return false;
				}
				Thread.sleep(5 * 1000);
			} finally {
				close(jedis);
			}
		}
		// Reach here only when all retries failed
		return false;
	}

	public List<String> getFromRedis(String... keys) throws InterruptedException {
		if (keys == null) {
			return null;
		}

		List<Response<String>> responses = new ArrayList<Response<String>>();
		int repeat = 0;
		while (repeat < 2) {
			Jedis jedis = getJedis();
			try {
				final Pipeline pipeline = jedis.pipelined();
				for (String key : keys) {
					Response<String> response = pipeline.get(key);
					responses.add(response);
				}
				pipeline.syncAndReturnAll();

				List<String> redisResults = new ArrayList<String>();
				for (Response<String> response : responses) {
					redisResults.add(response.get());
				}
				return redisResults;
			} catch (Exception e) {
				repeat++;
				if (repeat == 3) {
					// Retry failed
					Utils.processFailure(
							"Exception while retreiving from redis at: " + redisHost + ":" + redisPort + "\n"
									+ e.getMessage(),
							RedisManager.logger, Level.SEVERE, true, Constants.SERVERALERTS_MAILING_LIST,
							"**Redis Manager Exception**");

					return null;
				}
				Thread.sleep(5 * 1000);
			} finally {

			}
		}
		// Reach here only when all retries failed
		return null;
	}

	public HashSet<String> getKeysFromRedis(String pattern) throws InterruptedException {
		if (pattern == null || pattern.isEmpty()) {
			return null;
		}

		List<Response<String>> responses = new ArrayList<Response<String>>();
		int repeat = 0;
		while (repeat < 2) {
			Jedis jedis = getJedis();
			try {
				final Pipeline pipeline = jedis.pipelined();
				Response<Set<String>> keysResponse = pipeline.keys(pattern);
				pipeline.syncAndReturnAll();

				return (HashSet<String>) keysResponse.get();
			} catch (JedisConnectionException e) {
				repeat++;
				if (repeat == 3) {
					// Retry failed
					Utils.processFailure(
							"Exception while retrieivng from redis at: " + redisHost + ":" + redisPort + "\n"
									+ e.getMessage(),
							RedisManager.logger, Level.SEVERE, true, Constants.SERVERALERTS_MAILING_LIST,
							"**Redis Manager Exception**");

					return null;
				}
				Thread.sleep(5 * 1000);
			} finally {

			}
		}
		// Reach here only when all retries failed
		return null;
	}

	public String scriptCheckAndUpload(String luaHash) throws IOException, InterruptedException {
		int repeat = 0;
		while (repeat < 2) {
			// If fails try is one more time after 5 seconds
			Jedis jedis = getJedis();
			try {

				if (!jedis.scriptExists(luaHash)) {
					// File doesn't exists
					System.out.println("Script missing in Redis at: " + this.redisHost);
					String sha1 = jedis.scriptLoad(FileUtils.readFileToString(new File(Constants.luaScriptPath)));
					System.out.println("Script succesfully uploaded with hash: " + sha1);
					return sha1;
				} else {
					System.out.println("Script exists in Redis at: " + this.redisHost);
					return luaHash;
				}
			} catch (Exception e) {
				repeat++;
				if (repeat == 3) {
					// Retry failed
					Utils.processFailure("Cannot Upload script to redis at: " + redisHost + ":" + redisPort,
							RedisManager.logger, Level.SEVERE, true, Constants.SERVERALERTS_MAILING_LIST,
							"**Redis Alert**");

					return null;
				}
				Thread.sleep(5 * 1000);
			} finally {
				close(jedis);
			}
		}
		return null;
	}

	// TODO: expiry not working
	public boolean evalLua(String luaHash, int nKeys, Integer expirySeconds, ArrayList<ArrayList<String>> args)
			throws InterruptedException {
		int repeat = 0;
		while (repeat < 2) {
			Jedis jedis = getJedis();
			try {
				final Pipeline pipeline = jedis.pipelined();
				for (ArrayList<String> keyargs : args) {
					String[] argv = null;
					if (expirySeconds == null || expirySeconds < 1) {
						argv = new String[keyargs.size()];
						int i = 0;
						for (String s : keyargs) {
							argv[i++] = s;
						}
					} else {
						argv = new String[keyargs.size() + 1];
						int i = 0;
						for (String s : keyargs) {
							argv[i++] = s;
						}
						argv[keyargs.size()] = expirySeconds + "";
					}

					pipeline.evalsha(luaHash, nKeys, argv);
				}
				List<Object> results = pipeline.syncAndReturnAll();
				repeat = 3;
				return Utils.analyseRedisSetResults(results);
			} catch (Exception e) {
				e.printStackTrace();
				repeat++;
				if (repeat == 3) {
					// Retry failed
					Utils.processFailure("Cannot execute lua redis at: " + redisHost + ":" + redisPort,
							RedisManager.logger, Level.SEVERE, true, Constants.SERVERALERTS_MAILING_LIST,
							"**Redis Master Down**");

					return false;
				}
				Thread.sleep(5 * 1000);
			} finally {
				close(jedis);
			}
		}
		// Reach here only when all retries failed
		return false;
	}

	public ArrayList<String> getFromRedis(ArrayList<String> keys) throws InterruptedException {
		if (keys == null || keys.isEmpty()) {
			return null;
		}

		List<Response<String>> responses = new ArrayList<Response<String>>();
		int repeat = 0;
		Jedis jedis = getJedis();
		while (repeat < 2) {
			try {
				final Pipeline pipeline = jedis.pipelined();
				for (String key : keys) {
					Response<String> response = pipeline.get(key);
					responses.add(response);
				}
				System.out.println("pipeline" + pipeline.toString());
				pipeline.syncAndReturnAll();

				ArrayList<String> redisResults = new ArrayList<String>();
				for (Response<String> response : responses) {
					redisResults.add(response.get());
				}
				return redisResults;
			} catch (JedisConnectionException e) {
				repeat++;
				if (repeat == 3) {
					// Retry failed
					Utils.processFailure(
							"Exception get data from redis at: " + redisHost + ":" + redisPort + "\n" + e.getMessage(),
							RedisManager.logger, Level.SEVERE, true, Constants.SERVERALERTS_MAILING_LIST,
							"**Redis Manager Exception**");

					return null;
				}
				Thread.sleep(5 * 1000);
			} finally {
				close(jedis);
			}
		}
		// Reach here only when all retries failed
		return null;
	}
}
