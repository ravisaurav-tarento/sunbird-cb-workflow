package org.sunbird.workflow.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

@Configuration
@EnableCaching
public class RedisConfig {

	@Autowired
	RedisConfiguration redisConfiguration;

	@Bean
	public JedisPool jedisPool() {
		final JedisPoolConfig poolConfig = buildPoolConfig();
		JedisPool jedisPool = new JedisPool(poolConfig, redisConfiguration.getGetRedisHostName(),
				Integer.parseInt(redisConfiguration.getRedisPort()));
		return jedisPool;
	}

	@Bean
	public JedisPool jedisDataPopulationPool() {
		final JedisPoolConfig poolConfig = buildPoolConfig();
		JedisPool jedisPool = new JedisPool(poolConfig, redisConfiguration.getRedisDataHostName(),
				Integer.parseInt(redisConfiguration.getRedisDataPort()));
		return jedisPool;
	}
	private JedisPoolConfig buildPoolConfig() {
		final JedisPoolConfig poolConfig = new JedisPoolConfig();
		poolConfig.setMaxIdle(128);
		poolConfig.setMaxTotal(3000);
		poolConfig.setMinIdle(100);
		poolConfig.setTestOnBorrow(true);
		poolConfig.setTestOnReturn(true);
		poolConfig.setTestWhileIdle(true);
		poolConfig.setMinEvictableIdleTimeMillis(120000);
		poolConfig.setTimeBetweenEvictionRunsMillis(30000);
		poolConfig.setNumTestsPerEvictionRun(3);
		poolConfig.setBlockWhenExhausted(true);
		return poolConfig;
	}
}
