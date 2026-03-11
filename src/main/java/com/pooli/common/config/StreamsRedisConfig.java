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
@Profile({"default", "local", "api", "traffic"})
@EnableConfigurationProperties({StreamsRedisProperties.class, AppStreamsProperties.class, AppRedisProperties.class})
public class StreamsRedisConfig {

    /**
     * Creates a Redis connection factory configured for a standalone Redis instance using the provided properties.
     *
     * @param properties configuration containing Redis host, port, and optional password
     * @return a Lettuce-based RedisConnectionFactory configured with the specified host, port, and password (if provided)
     */
    @Bean("streamsRedisConnectionFactory")
    public RedisConnectionFactory streamsRedisConnectionFactory(StreamsRedisProperties properties) {
        RedisStandaloneConfiguration configuration =
                new RedisStandaloneConfiguration(properties.getHost(), properties.getPort());

        if (StringUtils.hasText(properties.getPassword())) {
            configuration.setPassword(RedisPassword.of(properties.getPassword()));
        }

        return new LettuceConnectionFactory(configuration);
    }

    /**
     * Creates a StringRedisTemplate bound to the streams Redis connection factory.
     *
     * @param connectionFactory the RedisConnectionFactory for the streams Redis instance (qualified as "streamsRedisConnectionFactory")
     * @return a StringRedisTemplate that uses the provided connection factory
     */
    @Bean("streamsStringRedisTemplate")
    public StringRedisTemplate streamsStringRedisTemplate(
            @Qualifier("streamsRedisConnectionFactory") RedisConnectionFactory connectionFactory
    ) {
        return new StringRedisTemplate(connectionFactory);
    }
}
