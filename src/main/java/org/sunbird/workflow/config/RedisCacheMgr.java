package org.sunbird.workflow.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;


@Component
public class RedisCacheMgr {

    private static Integer cache_ttl = 84600;

    @Autowired
    private JedisPool jedisPool;

    private RedisConfiguration redisConfiguration;

    private final Logger logger = LoggerFactory.getLogger(RedisCacheMgr.class);

    public void putStringInCache(String key, String value, Integer ttl) {
        if(null == ttl)
            ttl = cache_ttl;
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.set(Constants.REDIS_COMMON_KEY + key, value);
            jedis.expire(Constants.REDIS_COMMON_KEY + key, ttl);
            logger.debug("Cache_key_value " + Constants.REDIS_COMMON_KEY + key + " is saved in redis");
        } catch (Exception e) {
            logger.error("An Error Occurred while saving in Redis", e);
        }
    }

    public String getCache(String key) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.get(Constants.REDIS_COMMON_KEY + key);
        } catch (Exception e) {
            logger.error("An Error Occurred while fetching value from Redis", e);
            return null;
        }
    }
}
