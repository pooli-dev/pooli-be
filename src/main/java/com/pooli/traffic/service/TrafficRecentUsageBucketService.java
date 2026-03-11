package com.pooli.traffic.service;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Profile;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import com.pooli.traffic.domain.TrafficRefillPlan;
import com.pooli.traffic.domain.dto.request.TrafficPayloadReqDto;
import com.pooli.traffic.domain.enums.TrafficPoolType;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * 최근 차감량 버킷을 Redis에 기록하고 리필 계산값(delta/unit/threshold)을 제공합니다.
 */
@Slf4j
@Service
@Profile({"local", "traffic"})
@RequiredArgsConstructor
public class TrafficRecentUsageBucketService {

    private static final long RECENT_WINDOW_SECONDS = 10L;
    private static final long SPEED_BUCKET_TTL_SECONDS = 15L;
    private static final long REFILL_UNIT_MULTIPLIER = 10L;
    private static final long THRESHOLD_NUMERATOR = 3L;
    private static final long THRESHOLD_DENOMINATOR = 10L;

    @Qualifier("cacheStringRedisTemplate")
    private final StringRedisTemplate cacheStringRedisTemplate;
    private final TrafficRedisKeyFactory trafficRedisKeyFactory;

    /**
     * Record the current tick's consumed bytes into a per-second speed bucket in Redis.
     *
     * <p>Only positive `usedBytes` are recorded. The bucket is chosen by pool type and owner
     * (lineId for individual, familyId for shared). Writing refreshes the bucket TTL; failures
     * are logged and swallowed so they do not interrupt the overall consumption flow.
     *
     * @param poolType   pool type (individual or shared)
     * @param payload    request context containing traceId and owner identifiers (lineId, familyId)
     * @param usedBytes  consumed bytes for the current tick; only values greater than zero are recorded
     */
    public void recordTickUsage(TrafficPoolType poolType, TrafficPayloadReqDto payload, long usedBytes) {
        if (poolType == null || payload == null || usedBytes <= 0) {
            return;
        }

        Long ownerId = resolveOwnerId(poolType, payload);
        if (ownerId == null || ownerId <= 0) {
            return;
        }

        String bucketKey = resolveBucketKey(poolType, ownerId, Instant.now().getEpochSecond());
        if (bucketKey == null || bucketKey.isBlank()) {
            return;
        }

        try {
            Long updatedValue = cacheStringRedisTemplate.opsForValue().increment(bucketKey, usedBytes);
            if (updatedValue != null) {
                cacheStringRedisTemplate.expire(bucketKey, Duration.ofSeconds(SPEED_BUCKET_TTL_SECONDS));
            }
        } catch (Exception e) {
            log.warn(
                    "traffic_speed_bucket_record_failed traceId={} poolType={} ownerId={} usedBytes={}",
                    payload.getTraceId(),
                    poolType,
                    ownerId,
                    usedBytes,
                    e
            );
        }
    }

    /**
     * Compute a refill plan (delta, refillUnit, threshold) based on recent speed-bucket data.
     *
     * <p>Priority:
     * 1) Aggregate recent 10-second buckets (RECENT_10S)
     * 2) If none, aggregate all TTL-present buckets (ALL_BUCKETS)
     * 3) If none, build a fallback plan from payload API total data (API_TOTAL_DATA)
     *
     * <p>Formulas:
     * - delta = ceil(bucketSum / bucketCount)
     * - refillUnit = delta * 10
     * - threshold = ceil(refillUnit * 3 / 10), corrected to at least 1
     *
     * @param poolType the traffic pool type (individual or shared)
     * @param payload request context; may contain apiTotalData used for fallback
     * @return a TrafficRefillPlan containing delta, bucketCount, bucketSum, refillUnit, threshold, and a source tag
     */
    public TrafficRefillPlan resolveRefillPlan(TrafficPoolType poolType, TrafficPayloadReqDto payload) {
        long apiTotalData = normalizeNonNegative(payload == null ? null : payload.getApiTotalData());
        Long ownerId = resolveOwnerId(poolType, payload);
        if (poolType == null || ownerId == null || ownerId <= 0) {
            return buildFallbackPlan(apiTotalData);
        }

        BucketAggregate recentAggregate = aggregateRecentWindow(poolType, ownerId);
        if (recentAggregate.bucketCount > 0) {
            long delta = divideCeil(recentAggregate.bucketSum, recentAggregate.bucketCount);
            long refillUnit = safeMultiply(delta, REFILL_UNIT_MULTIPLIER);
            long threshold = divideCeil(
                    safeMultiply(refillUnit, THRESHOLD_NUMERATOR),
                    THRESHOLD_DENOMINATOR
            );
            return TrafficRefillPlan.builder()
                    .delta(delta)
                    .bucketCount((int) recentAggregate.bucketCount)
                    .bucketSum(recentAggregate.bucketSum)
                    .refillUnit(refillUnit)
                    .threshold(Math.max(1L, threshold))
                    .source("RECENT_10S")
                    .build();
        }

        BucketAggregate allAggregate = aggregateAllBuckets(poolType, ownerId);
        if (allAggregate.bucketCount > 0) {
            long delta = divideCeil(allAggregate.bucketSum, allAggregate.bucketCount);
            long refillUnit = safeMultiply(delta, REFILL_UNIT_MULTIPLIER);
            long threshold = divideCeil(
                    safeMultiply(refillUnit, THRESHOLD_NUMERATOR),
                    THRESHOLD_DENOMINATOR
            );
            return TrafficRefillPlan.builder()
                    .delta(delta)
                    .bucketCount((int) allAggregate.bucketCount)
                    .bucketSum(allAggregate.bucketSum)
                    .refillUnit(refillUnit)
                    .threshold(Math.max(1L, threshold))
                    .source("ALL_BUCKETS")
                    .build();
        }

        return buildFallbackPlan(apiTotalData);
    }

    /**
     * Create a fallback TrafficRefillPlan to use when no bucket data is available.
     *
     * <p>The plan uses the request's total API data as the refill unit and derives the threshold
     * as ceil(refillUnit * 3 / 10) with a minimum value of 1.
     *
     * @param apiTotalData the request's total data in bytes; values less than or equal to zero are treated as zero
     * @return a TrafficRefillPlan with source "API_TOTAL_DATA", where `refillUnit` equals the non-negative
     *         `apiTotalData`, `delta` equals `refillUnit`, `bucketCount` and `bucketSum` are zero,
     *         and `threshold` is ceil(refillUnit * 3 / 10) with a minimum of 1
     */
    private TrafficRefillPlan buildFallbackPlan(long apiTotalData) {
        long refillUnit = Math.max(0L, apiTotalData);
        long threshold = divideCeil(
                safeMultiply(refillUnit, THRESHOLD_NUMERATOR),
                THRESHOLD_DENOMINATOR
        );
        return TrafficRefillPlan.builder()
                .delta(refillUnit)
                .bucketCount(0)
                .bucketSum(0L)
                .refillUnit(refillUnit)
                .threshold(Math.max(1L, threshold))
                .source("API_TOTAL_DATA")
                .build();
    }

    /**
     * Aggregates sum and count from per-second speed buckets for the recent 10-second window ending at the current time.
     *
     * @param poolType the traffic pool type (individual or shared)
     * @param ownerId  the owner identifier (lineId for individual, familyId for shared)
     * @return a BucketAggregate containing `bucketSum` and `bucketCount`; returns an empty aggregate (zeros) if no positive bucket values are found
     */
    private BucketAggregate aggregateRecentWindow(TrafficPoolType poolType, long ownerId) {
        long nowSec = Instant.now().getEpochSecond();
        List<String> keys = new ArrayList<>();
        for (long i = 0; i < RECENT_WINDOW_SECONDS; i++) {
            keys.add(resolveBucketKey(poolType, ownerId, nowSec - i));
        }
        return aggregateKeys(keys);
    }

    /**
     * Aggregate the sum and count of all existing speed-bucket keys for the given pool and owner.
     *
     * <p>Used as a secondary fallback when recent 10-second buckets contain no data.
     *
     * @param poolType the pool type indicating individual or shared buckets
     * @param ownerId  the owner identifier (lineId for individual, familyId for shared)
     * @return a BucketAggregate with the total sum of positive bucket values and the number of buckets aggregated
     */
    private BucketAggregate aggregateAllBuckets(TrafficPoolType poolType, long ownerId) {
        String pattern = resolveBucketPattern(poolType, ownerId);
        if (pattern == null || pattern.isBlank()) {
            return BucketAggregate.empty();
        }

        Set<String> keys = cacheStringRedisTemplate.keys(pattern);
        if (keys == null || keys.isEmpty()) {
            return BucketAggregate.empty();
        }
        return aggregateKeys(new ArrayList<>(keys));
    }

    /**
     * Aggregates positive numeric values stored at the given Redis bucket keys.
     *
     * <p>Only values that parse to a long greater than zero are included; null, non-parsable,
     * zero, or negative values are ignored. If no positive values are found, an empty
     * aggregate (sum=0, bucketCount=0) is returned.
     *
     * @param keys list of Redis bucket keys to read and aggregate
     * @return a BucketAggregate containing the sum of positive values and the count of buckets included
     */
    private BucketAggregate aggregateKeys(List<String> keys) {
        if (keys == null || keys.isEmpty()) {
            return BucketAggregate.empty();
        }

        List<String> values = cacheStringRedisTemplate.opsForValue().multiGet(keys);
        if (values == null || values.isEmpty()) {
            return BucketAggregate.empty();
        }

        long sum = 0L;
        long count = 0L;
        for (String value : values) {
            long parsedValue = parsePositiveLong(value);
            if (parsedValue <= 0) {
                continue;
            }
            sum += parsedValue;
            count++;
        }

        if (count <= 0) {
            return BucketAggregate.empty();
        }
        return new BucketAggregate(sum, count);
    }

    /**
     * Resolve the bucket owner identifier for the given pool type and payload.
     *
     * @param poolType the traffic pool type (INDIVIDUAL or SHARED)
     * @param payload  the request payload containing potential owner identifiers
     * @return the ownerId (lineId for INDIVIDUAL, familyId for SHARED), or `null` if unavailable
     */
    private Long resolveOwnerId(TrafficPoolType poolType, TrafficPayloadReqDto payload) {
        if (poolType == null || payload == null) {
            return null;
        }

        return switch (poolType) {
            case INDIVIDUAL -> payload.getLineId();
            case SHARED -> payload.getFamilyId();
        };
    }

    /**
     * Constructs the per-second speed-bucket Redis key for the specified pool type and owner.
     *
     * @param poolType    the pool type (INDIVIDUAL or SHARED)
     * @param ownerId     the owner identifier (lineId for INDIVIDUAL, familyId for SHARED)
     * @param epochSecond the epoch second representing the bucket's second
     * @return            the Redis key string for the specified bucket
     */
    private String resolveBucketKey(TrafficPoolType poolType, long ownerId, long epochSecond) {
        return switch (poolType) {
            case INDIVIDUAL -> trafficRedisKeyFactory.speedBucketIndividualKey(ownerId, epochSecond);
            case SHARED -> trafficRedisKeyFactory.speedBucketSharedKey(ownerId, epochSecond);
        };
    }

    /**
     * Constructs the Redis key pattern for speed buckets for the given pool type and owner.
     *
     * @param poolType the traffic pool type (INDIVIDUAL or SHARED)
     * @param ownerId  owner identifier: lineId when poolType is INDIVIDUAL, familyId when poolType is SHARED
     * @return         the Redis key pattern string that matches the owner's speed bucket keys (e.g., "...:*")
     */
    private String resolveBucketPattern(TrafficPoolType poolType, long ownerId) {
        return switch (poolType) {
            case INDIVIDUAL -> trafficRedisKeyFactory.speedBucketIndividualPattern(ownerId);
            case SHARED -> trafficRedisKeyFactory.speedBucketSharedPattern(ownerId);
        };
    }

    /**
     * Compute the ceiling of the division of two positive integers.
     *
     * <p>If either `numerator` or `denominator` is less than or equal to zero, returns 0 to keep
     * downstream calculations safe.
     *
     * @param numerator the dividend; expected positive
     * @param denominator the divisor; expected positive
     * @return the smallest integer greater than or equal to `numerator / denominator`, or `0` if
     * either input is less than or equal to zero
     */
    private long divideCeil(long numerator, long denominator) {
        if (numerator <= 0 || denominator <= 0) {
            return 0L;
        }
        long quotient = numerator / denominator;
        long remainder = numerator % denominator;
        if (remainder == 0) {
            return quotient;
        }
        return quotient + 1L;
    }

    /**
     * Multiply two long values while preventing overflow by saturating the result.
     *
     * <p>If either operand is less than or equal to zero, the method returns 0. If the exact
     * product would exceed Long.MAX_VALUE, the method returns Long.MAX_VALUE to avoid overflow.
     *
     * @param left  the left operand
     * @param right the right operand
     * @return `0` if either operand is less than or equal to zero, `Long.MAX_VALUE` if the
     *         true product would overflow, otherwise the exact product of the two operands
     */
    private long safeMultiply(long left, long right) {
        if (left <= 0 || right <= 0) {
            return 0L;
        }
        if (left > Long.MAX_VALUE / right) {
            return Long.MAX_VALUE;
        }
        return left * right;
    }

    /**
     * Normalize a possibly-null Long to a long greater than or equal to zero.
     *
     * @param value the input value; treated as 0 if null or less than or equal to 0
     * @return the original value when greater than 0, otherwise 0
     */
    private long normalizeNonNegative(Long value) {
        if (value == null || value <= 0) {
            return 0L;
        }
        return value;
    }

    /**
     * Parse a Redis string value into a positive long.
     *
     * <p>Blank, null, non-numeric, zero, or negative inputs produce 0.
     *
     * @param value the string to parse (may be null or blank)
     * @return the parsed long if greater than 0, or 0 otherwise
     */
    private long parsePositiveLong(String value) {
        if (value == null || value.isBlank()) {
            return 0L;
        }

        try {
            long parsed = Long.parseLong(value);
            return parsed > 0 ? parsed : 0L;
        } catch (NumberFormatException e) {
            return 0L;
        }
    }

    /**
     * 버킷 집계(sum/count)를 함께 전달하기 위한 경량 값 객체입니다.
     */
    private record BucketAggregate(long bucketSum, long bucketCount) {
        /**
         * Create an empty BucketAggregate representing no recorded buckets.
         *
         * @return a BucketAggregate with both bucketSum and bucketCount set to 0
         */
        private static BucketAggregate empty() {
            return new BucketAggregate(0L, 0L);
        }
    }
}
