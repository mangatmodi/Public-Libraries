package com.bidstalk.tools.common;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang.builder.ToStringBuilder;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.exceptions.JedisConnectionException;

import com.bidstalk.tools.common.Constants;
import com.google.common.base.Joiner;

/**
 * @author mangat This class provides and interface to all the Redis Masters in
 *         the Multi DC setup
 *
 */
public class RedisManager {

	@Override
	public String toString() {
		return "RedisManager [allRedisHosts=" + allRedisHosts + "]";
	}

	protected static String sqlUrl, sqlUser, sqlPassword;
	public final static Logger logger = Logger.getLogger(RedisManager.class.getSimpleName());
	protected String redisHost;
	protected int redisPort;

	protected HashMap<Integer, RedisHost> allRedisHosts;

	public RedisManager(String environment, String hostport) {
		allRedisHosts = new HashMap<>();
		if (hostport != null) {
			// Update host port from command line.
			String[] parts = hostport.split(":");
			redisHost = parts[0];
			redisPort = Integer.parseInt(parts[1]);
		} else if (environment.equals(Constants.PROD_ENV_NAME)) {
			redisHost = Constants.PROD_REDIS_HOST;
			redisPort = Constants.PROD_REDIS_PORT;
		} else {
			redisHost = Constants.STAGING_REDIS_HOST;
			redisPort = Constants.STAGING_REDIS_PORT;
		}

		// TODO
		allRedisHosts.put(0, new RedisHost(0, redisHost, redisPort, ""));
		logger.log(Level.INFO, "Using redis host:" + redisHost + " port:" + redisPort);
	}

	/**
	 * @param confFilePath
	 *            RedisManager: It creates redis connection for each redis host
	 *            entry given in the configuration file
	 */

	public RedisManager(String confFilePath) {
		redisHost = "";
		redisPort = -1;
		String comment = "";
		int port = -1;
		String redisIp = "";
		int id = -1;

		// Invalid parameter
		if (confFilePath == null || confFilePath.isEmpty()) {
			logger.log(Level.SEVERE, "Invalid redis configuration file path parameter");
			return;
		}

		File confFile = new File(confFilePath);
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(confFile));
		} catch (FileNotFoundException e) {
			Utils.processFailure("redis configuration file: " + confFilePath + " doesn't exist", logger, Level.SEVERE,
					true, Constants.SERVERALERTS_MAILING_LIST, "**Redis Manager wrong configuration**");

			return;
		}

		String line;
		allRedisHosts = new HashMap<>();

		try {
			while ((line = br.readLine()) != null) {

				if (line.isEmpty() || line.trim().equals("") || line.trim().equals("\n")) {
					continue;
				}
				if (line.charAt(0) == '#') {
					continue;
				}

				String[] redisEntry = line.trim().split("\\s+", 3);
				if (redisEntry.length < 2) {
					logger.log(Level.WARNING, "Invalid Redis Entry [hostId hostIp:hostPort \"comment\"]: " + line);
					continue;
				}
				if (redisEntry.length == 3) {
					comment = redisEntry[2];
				}
				try {
					id = Integer.parseInt(redisEntry[0]);
				} catch (NumberFormatException e) {
					logger.log(Level.WARNING, "Invalid hostId [hostId hostIp:hostPort \"comment\"]: " + line);
					continue;

				}

				String[] redisURL = redisEntry[1].split(":");

				if (redisURL.length < 2) {
					logger.log(Level.WARNING,
							"Invalid Redis Host [hostId hostIp:hostPort \"comment\"]: " + redisEntry[1]);
					continue;
				}
				redisIp = redisURL[0];
				try {
					port = Integer.parseInt(redisURL[1]);
				} catch (NumberFormatException e) {
					logger.log(Level.WARNING, "Invalid Redis Port [hostId hostIp:hostPort \"comment\"]: " + line);
					continue;
				}

				redisHost = redisIp;
				redisPort = port;

				if (id >= 0) {
					allRedisHosts.put(id, new RedisHost(id, redisIp, port, comment));
				}
			}
		} catch (IOException e) {
			Utils.processFailure("Unable to read redis configuration file: " + confFilePath, logger, Level.SEVERE, true,
					Constants.SERVERALERTS_MAILING_LIST, "**Redis Manager wrong configuration**");

			return;
		}

	}

	/**
	 * Remove the keys in redisKeys in all the Redis Servers in the conf file
	 * marked true by the redisBitMap
	 * 
	 * @param redisKeys
	 * @param redisBitMap
	 * @return boolean
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public BitSet removeFromRedis(final Set<String> redisKeys, BitSet redisBitMap)
			throws InterruptedException, ExecutionException {
		if (allRedisHosts == null || allRedisHosts.isEmpty()) {
			return new BitSet(redisBitMap.size());
		}
		if (redisBitMap.size() < allRedisHosts.size()) {
			logger.log(Level.SEVERE, "BitMap(size=" + redisBitMap.size() + ") not in sync with redis conf file(size="
					+ allRedisHosts.size() + "). Skipping operation");
			return new BitSet(redisBitMap.size());
		}

		int i = 0;
		ExecutorService executorService = Executors.newFixedThreadPool(allRedisHosts.size());
		Collection<Callable<Boolean>> tasks = new ArrayList<>();
		for (final Entry<Integer, RedisHost> redis : allRedisHosts.entrySet()) {
			i = redis.getKey();
			if (redisBitMap.get(i) && redis != null) {
				tasks.add(new Callable<Boolean>() {
					@Override
					public Boolean call() throws Exception {
						// TODO Auto-generated method stub
						return redis.getValue().removeFromRedis(redisKeys);
					}
				});
			}
		}

		List<Future<Boolean>> answers = executorService.invokeAll(tasks, 600, TimeUnit.SECONDS);
		BitSet result = new BitSet(redisBitMap.size());
		i = 0;
		for (Future<Boolean> answer : answers) {
			if (answer != null && answer.get() != null) {
				if (answer.get()) {
					result.set(i);
				} else {
					result.clear(i);
				}

			}
			i++;
		}
		executorService.shutdownNow();
		
		return result;
	}

	/**
	 * Insert the redisData in all the Redis Servers in the conf file marked
	 * true by the redisBitMap
	 * 
	 * @param redisData
	 * @param redisBitMap
	 * @return
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public BitSet insertIntoRedis(final Map<String, String> redisData, BitSet redisBitMap, final Integer expirySeconds)
			throws InterruptedException, ExecutionException {
		if (allRedisHosts == null || allRedisHosts.isEmpty()) {
			return new BitSet(redisBitMap.size());
		}
		if (redisBitMap.size() < allRedisHosts.size()) {
			logger.log(Level.SEVERE, "BitMap(size=" + redisBitMap.size() + ") not in sync with redis conf file(size="
					+ allRedisHosts.size() + "). Skipping operation");
			return new BitSet(redisBitMap.size());
		}

		int i = 0;
		ExecutorService executorService = Executors.newFixedThreadPool(allRedisHosts.size());
		Collection<Callable<Boolean>> tasks = new ArrayList<>();
		for (final Entry<Integer, RedisHost> redis : allRedisHosts.entrySet()) {
			i = redis.getKey();
			if (redisBitMap.get(i) && redis != null) {
				tasks.add(new Callable<Boolean>() {
					@Override
					public Boolean call() throws Exception {
						// TODO Auto-generated method stub
						return redis.getValue().insertIntoRedis(redisData, expirySeconds);
					}
				});
			}
		}

		List<Future<Boolean>> answers = executorService.invokeAll(tasks, 600, TimeUnit.SECONDS);
		BitSet result = new BitSet(redisBitMap.size());
		i = 0;
		for (Future<Boolean> answer : answers) {
			if (answer != null && answer.get() != null) {
				if (answer.get()) {
					result.set(i);
				} else {
					result.clear(i);
				}

			}
			i++;
		}
		executorService.shutdownNow();
		return result;
	}

	/**
	 * Insert the comma separated redisData in all the Redis Servers in the conf
	 * file marked true by the redisBitMap
	 * 
	 * @param redisData
	 * @param redisBitMap
	 * @return
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public <T> BitSet insertCommaSeparatedIntoRedis(final Map<String, Set<T>> redisData, BitSet redisBitMap,
			final Integer expirySeconds) throws InterruptedException, ExecutionException {
		if (allRedisHosts == null || allRedisHosts.isEmpty()) {
			return new BitSet(redisBitMap.size());
		}
		if (redisBitMap.size() < allRedisHosts.size()) {
			logger.log(Level.SEVERE, "BitMap(size=" + redisBitMap.size() + ") not in sync with redis conf file(size="
					+ allRedisHosts.size() + "). Skipping operation");
			return new BitSet(redisBitMap.size());
		}

		int i = 0;
		try {
			ExecutorService executorService = Executors.newFixedThreadPool(allRedisHosts.size());
			Collection<Callable<Boolean>> tasks = new ArrayList<>();
			for (final Entry<Integer, RedisHost> redis : allRedisHosts.entrySet()) {
				i = redis.getKey();
				if (redisBitMap.get(i) && redis != null) {
					tasks.add(new Callable<Boolean>() {
						@Override
						public Boolean call() throws Exception {
							// TODO Auto-generated method stub
							return redis.getValue().insertCommaSeparatedIntoRedis(redisData, expirySeconds);
						}
					});
				}
			}

			List<Future<Boolean>> answers = executorService.invokeAll(tasks, 900, TimeUnit.SECONDS);
			BitSet result = new BitSet(redisBitMap.size());
			i = 0;
			for (Future<Boolean> answer : answers) {
				if (answer != null && answer.get() != null) {
					if (answer.get()) {
						result.set(i);
					} else {
						result.clear(i);
					}

				}
				i++;
			}
			executorService.shutdownNow();
			return result;
		} catch (Exception e) {
			// TODO: handle exception
			return new BitSet();
		}
	}

	public HashSet<String> getKeysFromAllRedisHosts(final String pattern, BitSet redisBitMap)
			throws InterruptedException, ExecutionException {
		if (allRedisHosts == null || allRedisHosts.isEmpty()) {
			return null;
		}
		if (redisBitMap.size() < allRedisHosts.size()) {
			logger.log(Level.SEVERE, "BitMap(size=" + redisBitMap.size() + ") not in sync with redis conf file(size="
					+ allRedisHosts.size() + "). Skipping operation");
			return null;
		}

		int i = 0;
		ExecutorService executorService = Executors.newFixedThreadPool(allRedisHosts.size());

		Collection<Callable<HashSet<String>>> tasks = new ArrayList<>();
		for (final Entry<Integer, RedisHost> redis : allRedisHosts.entrySet()) {
			if (redisBitMap.get(i)) {
				i = redis.getKey();
				tasks.add(new Callable<HashSet<String>>() {
					@Override
					public HashSet<String> call() throws Exception {
						// TODO Auto-generated method stub
						return redis.getValue().getKeysFromRedis(pattern);
					}
				});
			}
		}

		List<Future<HashSet<String>>> answers = executorService.invokeAll(tasks, 600, TimeUnit.SECONDS);
		HashSet<String> result = new HashSet<>();
		for (Future<HashSet<String>> answer : answers) {
			if (answer != null && answer.get() != null && answer.isDone())
				result.addAll((HashSet<String>) answer.get());
		}
		executorService.shutdownNow();
		
		return result;
	}

	/**
	 * Execute Lua script already loaded into the server. Arraylist holds the
	 * keys and args to be given to Lua
	 * 
	 * @param luaHash
	 * @param redisBitMap
	 * @return
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public BitSet evalLua(final String luaHash, final int nKeys, final BitSet redisBitMap, final Integer expirySeconds,
			final ArrayList<ArrayList<String>> args) throws InterruptedException, ExecutionException {
		if (allRedisHosts == null || allRedisHosts.isEmpty()) {
			return new BitSet(redisBitMap.size());
		}
		if (redisBitMap.size() < allRedisHosts.size()) {
			logger.log(Level.SEVERE, "BitMap(size=" + redisBitMap.size() + ") not in sync with redis conf file(size="
					+ allRedisHosts.size() + "). Skipping operation");
			return new BitSet(redisBitMap.size());
		}

		int i = 0;
		ExecutorService executorService = Executors.newFixedThreadPool(allRedisHosts.size());
		Collection<Callable<Boolean>> tasks = new ArrayList<>();
		for (final Entry<Integer, RedisHost> redis : allRedisHosts.entrySet()) {
			i = redis.getKey();
			if (redisBitMap.get(i) && redis != null) {
				tasks.add(new Callable<Boolean>() {
					@Override
					public Boolean call() throws Exception {
						// TODO Auto-generated method stub
						String hash = redis.getValue().scriptCheckAndUpload(luaHash);
						return redis.getValue().evalLua(hash, 1, expirySeconds, args);
					}
				});
			}
		}

		List<Future<Boolean>> answers = executorService.invokeAll(tasks, 600, TimeUnit.SECONDS);
		BitSet result = new BitSet(redisBitMap.size());
		i = 0;
		for (Future<Boolean> answer : answers) {
			if (answer != null && answer.get() != null) {
				if (answer.get()) {
					result.set(i);
				} else {
					result.clear(i);
				}

			}
			i++;
		}
		executorService.shutdownNow();

		return result;
	}

//	public void close(final BitSet redisBitMap) {
//		ExecutorService executorService = Executors.newFixedThreadPool(allRedisHosts.size());
//		Collection<Callable<Void>> tasks = new ArrayList<>();
//		int i = 0;
//		for (final Entry<Integer, RedisHost> redis : allRedisHosts.entrySet()) {
//			i = redis.getKey();
//			if (redisBitMap.get(i) && redis != null) {
//				tasks.add(new Callable<Void>() {
//					@Override
//					public Void call() throws Exception {
//						redis.getValue().close();
//						return null;
//					}
//				});
//			}
//		}
//		executorService.shutdownNow();
//
//	}

	public HashMap<Integer, RedisHost> getAllRedisHosts() {
		return allRedisHosts;
	}

	@Deprecated
	public Jedis getJedis() {
		return new Jedis(redisHost, redisPort);
	}

	@Deprecated
	public String getRedisHost() {
		return this.redisHost;
	}

	@Deprecated
	public int getRedisPort() {
		return this.redisPort;
	}

	@Deprecated
	public boolean removeFromRedis(Set<String> redisKeys) {
		Jedis jedis = getJedis();
		final Pipeline pipeline = jedis.pipelined();
		for (String redisKey : redisKeys) {
			if (redisKey != null) {
				pipeline.del(redisKey);
			}
		}
		List<Object> results = pipeline.syncAndReturnAll();
		jedis.close();
		// TODO(guna): Process results.
		return true;
	}

	@Deprecated
	public boolean insertIntoRedis(Map<String, String> redisData) {
		Jedis jedis = getJedis();
		Pipeline pipeline = jedis.pipelined();
		for (Map.Entry<String, String> entry : redisData.entrySet()) {
			pipeline.set(entry.getKey(), entry.getValue());
		}
		List<Object> results = pipeline.syncAndReturnAll();
		jedis.close();
		return Utils.analyseRedisSetResults(results);
	}

	@Deprecated
	public List<String> getFromRedis(String... keys) {
		if (keys == null) {
			return null;
		}

		List<Response<String>> responses = new ArrayList<Response<String>>();
		Jedis jedis = getJedis();
		Pipeline pipeline = jedis.pipelined();
		for (String key : keys) {
			Response<String> response = pipeline.get(key);
			responses.add(response);
		}
		pipeline.syncAndReturnAll();
		jedis.close();

		List<String> redisResults = new ArrayList<String>();
		for (Response<String> response : responses) {
			redisResults.add(response.get());
		}
		return redisResults;
	}

	@Deprecated
	public ArrayList<String> getFromRedis(ArrayList<String> keys) {
		if (keys == null || keys.isEmpty()) {
			return null;
		}

		ArrayList<Response<String>> responses = new ArrayList<Response<String>>();
		Jedis jedis = getJedis();
		Pipeline pipeline = jedis.pipelined();
		for (String key : keys) {
			Response<String> response = pipeline.get(key);
			responses.add(response);
		}
		System.out.println("pipeline" + pipeline.toString());
		pipeline.syncAndReturnAll();
		jedis.close();

		ArrayList<String> redisResults = new ArrayList<String>();
		for (Response<String> response : responses) {
			redisResults.add(response.get());
		}
		return redisResults;
	}

	@Deprecated
	public <T> boolean insertCommaSeparatedIntoRedis(Map<String, Set<T>> redisData) {
		if (redisData == null || redisData.size() == 0) {
			return true;
		}

		Jedis jedis = getJedis();
		Joiner commaJoiner = Joiner.on(",").skipNulls();

		Pipeline pipeline = jedis.pipelined();
		for (Map.Entry<String, Set<T>> entry : redisData.entrySet()) {
			String redisKey = entry.getKey();
			String redisValue = commaJoiner.join(entry.getValue());
			pipeline.set(redisKey, redisValue);
		}
		List<Object> results = pipeline.syncAndReturnAll();
		jedis.close();
		return Utils.analyseRedisSetResults(results);
	}

	public static void main(String args[]) throws Exception {
		RedisManager manager = new RedisManager("/path/to/redisManager.conf");
		final ArrayList<String> arg = new ArrayList<>(3);
		arg.add("Hello");
		arg.add("");
		arg.add("Bye,World");
		manager.evalLua(Constants.luaEditFunctionHash, 1,

		new BitSet(2) {
			/**
			 * 
			 */
			{
				set(1);
				set(2);
			}
		}, 7200, new ArrayList<ArrayList<String>>() {
			{
				add(arg);
			}
		});

	}
}
