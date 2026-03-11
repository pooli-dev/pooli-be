package com.pooli.common.config;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisPassword;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.util.StringUtils;

@Configuration
@Profile({"local", "traffic"})
@EnableConfigurationProperties(CacheRedisProperties.class)
public class CacheRedisConfig {

    /**
     * Create a RedisConnectionFactory configured for a standalone Redis instance using cache properties.
     *
     * @param properties configuration values (host, port, and optional password) used to configure the connection
     * @return a Lettuce-based RedisConnectionFactory initialized with the provided host, port, and password (if present)
     */
    @Bean("cacheRedisConnectionFactory")
    public RedisConnectionFactory cacheRedisConnectionFactory(CacheRedisProperties properties) {
        RedisStandaloneConfiguration configuration =
                new RedisStandaloneConfiguration(properties.getHost(), properties.getPort());

        if (StringUtils.hasText(properties.getPassword())) {
            configuration.setPassword(RedisPassword.of(properties.getPassword()));
        }

        return new LettuceConnectionFactory(configuration);
    }

    /**
     * Create a StringRedisTemplate configured to use the cache Redis connection factory.
     *
     * @param connectionFactory the RedisConnectionFactory provided by the `cacheRedisConnectionFactory` bean
     * @return a StringRedisTemplate wired to the specified connection factory
     */
    @Bean("cacheStringRedisTemplate")
    public StringRedisTemplate cacheStringRedisTemplate(
            @Qualifier("cacheRedisConnectionFactory") RedisConnectionFactory connectionFactory
    ) {
        return new StringRedisTemplate(connectionFactory);
    }
}

