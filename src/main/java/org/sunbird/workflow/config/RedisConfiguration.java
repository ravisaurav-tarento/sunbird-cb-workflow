package org.sunbird.workflow.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class RedisConfiguration {

    @Value("${redis.host.name}")
    private String getRedisHostName;

    @Value("${redis.port}")
    private String redisPort;

    @Value("${redis.data.host.name}")
    private String redisDataHostName;

    @Value("${redis.data.port}")
    private String redisDataPort;

    public String getGetRedisHostName() {
        return getRedisHostName;
    }

    public void setGetRedisHostName(String getRedisHostName) {
        this.getRedisHostName = getRedisHostName;
    }

    public String getRedisPort() {
        return redisPort;
    }

    public void setRedisPort(String redisPort) {
        this.redisPort = redisPort;
    }

    public String getRedisDataHostName() {
        return redisDataHostName;
    }

    public void setRedisDataHostName(String redisDataHostName) {
        this.redisDataHostName = redisDataHostName;
    }

    public String getRedisDataPort() {
        return redisDataPort;
    }

    public void setRedisDataPort(String redisDataPort) {
        this.redisDataPort = redisDataPort;
    }
}
