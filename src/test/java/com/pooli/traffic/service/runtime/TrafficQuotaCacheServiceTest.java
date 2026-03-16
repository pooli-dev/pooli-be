package com.pooli.traffic.service.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.StringRedisTemplate;

@ExtendWith(MockitoExtension.class)
class TrafficQuotaCacheServiceTest {

    @Mock
    private StringRedisTemplate cacheStringRedisTemplate;

    @Mock
    private HashOperations<String, Object, Object> hashOperations;

    @InjectMocks
    private TrafficQuotaCacheService trafficQuotaCacheService;

    @Nested
    class ReadAmountOrDefaultTest {

        @Test
        void returnsParsedAmount() {
            when(cacheStringRedisTemplate.opsForHash()).thenReturn(hashOperations);
            when(hashOperations.get("pooli:remaining_indiv_amount:11:202603", "amount")).thenReturn("300");

            long amount = trafficQuotaCacheService.readAmountOrDefault(
                    "pooli:remaining_indiv_amount:11:202603",
                    10L
            );

            assertEquals(300L, amount);
        }

        @Test
        void returnsDefaultWhenAmountMalformed() {
            when(cacheStringRedisTemplate.opsForHash()).thenReturn(hashOperations);
            when(hashOperations.get("pooli:remaining_indiv_amount:11:202603", "amount")).thenReturn("not-a-number");

            long amount = trafficQuotaCacheService.readAmountOrDefault(
                    "pooli:remaining_indiv_amount:11:202603",
                    77L
            );

            assertEquals(77L, amount);
        }
    }

    @Nested
    class HydrateBalanceTest {

        @Test
        void hydratesBalanceWithExpireAt() {
            when(cacheStringRedisTemplate.opsForHash()).thenReturn(hashOperations);

            trafficQuotaCacheService.hydrateBalance(
                    "pooli:remaining_indiv_amount:11:202603",
                    1_775_833_199L
            );

            verify(hashOperations).putIfAbsent("pooli:remaining_indiv_amount:11:202603", "amount", "0");
            verify(hashOperations).putIfAbsent("pooli:remaining_indiv_amount:11:202603", "is_empty", "0");
            verify(cacheStringRedisTemplate).expireAt(
                    "pooli:remaining_indiv_amount:11:202603",
                    Instant.ofEpochSecond(1_775_833_199L)
            );
        }

        @Test
        void normalizesNegativeInitialAmount() {
            when(cacheStringRedisTemplate.opsForHash()).thenReturn(hashOperations);

            trafficQuotaCacheService.hydrateBalance(
                    "pooli:remaining_indiv_amount:11:202603",
                    1_775_833_199L
            );

            verify(hashOperations).putIfAbsent("pooli:remaining_indiv_amount:11:202603", "amount", "0");
            verify(hashOperations).putIfAbsent("pooli:remaining_indiv_amount:11:202603", "is_empty", "0");
        }
    }

    @Nested
    class RefillBalanceTest {

        @Test
        void refillsBalanceAndUpdatesExpireAt() {
            when(cacheStringRedisTemplate.opsForHash()).thenReturn(hashOperations);

            trafficQuotaCacheService.refillBalance(
                    "pooli:remaining_indiv_amount:11:202603",
                    80L,
                    1_775_833_199L
            );

            verify(hashOperations).increment("pooli:remaining_indiv_amount:11:202603", "amount", 80L);
            verify(cacheStringRedisTemplate).expireAt(
                    "pooli:remaining_indiv_amount:11:202603",
                    Instant.ofEpochSecond(1_775_833_199L)
            );
        }

        @Test
        void noOpWhenRefillAmountNonPositive() {
            trafficQuotaCacheService.refillBalance(
                    "pooli:remaining_indiv_amount:11:202603",
                    0L,
                    1_775_833_199L
            );

            verify(cacheStringRedisTemplate, never()).opsForHash();
            verify(cacheStringRedisTemplate, never()).expireAt(
                    "pooli:remaining_indiv_amount:11:202603",
                    Instant.ofEpochSecond(1_775_833_199L)
            );
        }
    }
}
