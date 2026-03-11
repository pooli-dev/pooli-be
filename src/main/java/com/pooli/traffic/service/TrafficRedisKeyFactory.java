package com.pooli.traffic.service;

import java.time.LocalDate;
import java.time.YearMonth;
import java.util.Objects;

import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import com.pooli.common.config.AppRedisProperties;

import lombok.RequiredArgsConstructor;

/**
 * 트래픽 처리에서 사용하는 Redis 키를 명세 규칙대로 생성하는 팩토리입니다.
 * app.redis.namespace를 앞에 붙여 환경별 키 충돌을 방지합니다.
 */
@Component
@Profile({"local", "traffic"})
@RequiredArgsConstructor
public class TrafficRedisKeyFactory {

    private final AppRedisProperties appRedisProperties;
    private final TrafficRedisRuntimePolicy trafficRedisRuntimePolicy;

    /**
     * Builds the Redis key used to store or look up a policy, applying the configured namespace if present.
     *
     * @param policyId the policy identifier
     * @return the namespaced Redis key for the policy (e.g. "namespace:policy:<policyId>" or "policy:<policyId>")
     */
    public String policyKey(long policyId) {
        return namespaced("policy:" + policyId);
    }

    /**
     * Redis key for the distributed lock used to ensure a single global policy bootstrap execution during application boot.
     *
     * @return the Redis key string for that distributed lock; prefixed with the configured namespace if present.
     */
    public String policyBootstrapLockKey() {
        return namespaced("policy:bootstrap:lock");
    }

    /**
     * Key name for storing the epoch seconds of the last successful policy bootstrap or reconciliation.
     *
     * @return the Redis key string used to store the bootstrap/reconciliation timestamp
     */
    public String policyBootstrapVersionKey() {
        return namespaced("policy_bootstrap_version");
    }

    /**
     * Builds the Redis readiness key that marks a line's on-demand policy hydration as complete.
     *
     * @param lineId the line identifier
     * @return the Redis key indicating completion of on-demand policy hydration for the specified line
     */
    public String linePolicyReadyKey(long lineId) {
        return namespaced("line_policy_ready:" + lineId);
    }

    /**
     * Builds the distributed lock key used to ensure a single execution of line policy hydration for a specific line.
     *
     * @param lineId the identifier of the line
     * @return the namespaced Redis key for the line policy hydrate lock
     */
    public String linePolicyHydrateLockKey(long lineId) {
        return namespaced("line_policy_hydrate_lock:" + lineId);
    }

    /**
     * Builds the Redis key for a line's daily total limit.
     *
     * @param lineId the identifier of the line
     * @return the namespaced Redis key for the line's daily total limit
     */
    public String dailyTotalLimitKey(long lineId) {
        return namespaced("daily_total_limit:" + lineId);
    }

    /**
     * Builds the Redis key for an application's daily data limit for a specific line.
     *
     * @param lineId the line identifier
     * @return the Redis key for the app daily data limit for the given line, prefixed with the configured namespace if present
     */
    public String appDataDailyLimitKey(long lineId) {
        return namespaced("app_data_daily_limit:" + lineId);
    }

    /**
     * Builds the Redis key for an application's speed limit for a given line.
     *
     * @param lineId the line identifier
     * @return the namespaced Redis key for the application's speed limit for the specified line
     */
    public String appSpeedLimitKey(long lineId) {
        return namespaced("app_speed_limit:" + lineId);
    }

    /**
     * Builds a namespaced Redis key for the app whitelist of the specified line.
     *
     * @param lineId the line identifier
     * @return the Redis key string for the app whitelist for the given line
     */
    public String appWhitelistKey(long lineId) {
        return namespaced("app_whitelist:" + lineId);
    }

    /**
     * Builds the namespaced Redis key for the monthly shared limit of a specific line.
     *
     * @param lineId the line identifier
     * @return the Redis key string for the monthly shared limit for the given line, prefixed with the configured namespace if present
     */
    public String monthlySharedLimitKey(long lineId) {
        return namespaced("monthly_shared_limit:" + lineId);
    }

    /**
     * Builds a namespaced Redis key for a line's daily total usage.
     *
     * @param lineId the line identifier
     * @param targetDate the date used to produce the yyyymmdd suffix
     * @return the Redis key in the form `daily_total_usage:{lineId}:{yyyymmdd}`, prefixed with the configured namespace when present
     */
    public String dailyTotalUsageKey(long lineId, LocalDate targetDate) {
        // 일별 집계 키는 yyyymmdd suffix를 사용한다.
        String yyyymmdd = trafficRedisRuntimePolicy.formatYyyyMmDd(targetDate);
        return namespaced("daily_total_usage:" + lineId + ":" + yyyymmdd);
    }

    /**
     * Builds a namespaced Redis key for daily app usage for the given line and date.
     *
     * @param lineId the line identifier
     * @param targetDate the date used to derive a yyyymmdd suffix
     * @return the namespaced Redis key in the form "daily_app_usage:{lineId}:{yyyymmdd}"
     */
    public String dailyAppUsageKey(long lineId, LocalDate targetDate) {
        // 일별 집계 키는 yyyymmdd suffix를 사용한다.
        String yyyymmdd = trafficRedisRuntimePolicy.formatYyyyMmDd(targetDate);
        return namespaced("daily_app_usage:" + lineId + ":" + yyyymmdd);
    }

    /**
     * Builds a namespaced Redis key for monthly shared usage for a specific line and month.
     *
     * @param lineId the identifier of the line
     * @param targetMonth the YearMonth used to derive the YYYYMM suffix
     * @return the Redis key in the form `monthly_shared_usage:{lineId}:{yyyymm}`, prefixed with the configured namespace when present
     */
    public String monthlySharedUsageKey(long lineId, YearMonth targetMonth) {
        // 월별 집계 키는 yyyymm suffix를 사용한다.
        String yyyymm = trafficRedisRuntimePolicy.formatYyyyMm(targetMonth);
        return namespaced("monthly_shared_usage:" + lineId + ":" + yyyymm);
    }

    /**
     * Builds the Redis key that marks the end of an immediate block for a given line.
     *
     * @param lineId the identifier of the line
     * @return the namespaced Redis key for the immediate-block end of the specified line
     */
    public String immediatelyBlockEndKey(long lineId) {
        return namespaced("immediately_block_end:" + lineId);
    }

    /**
     * Builds the Redis key for a repeat block for the specified line.
     *
     * @param lineId the line identifier
     * @return the namespaced Redis key for the repeat block
     */
    public String repeatBlockKey(long lineId) {
        return namespaced("repeat_block:" + lineId);
    }

    /**
     * Builds the namespaced Redis hash key for an individual line's remaining amount for a given month.
     *
     * @param lineId the identifier of the line
     * @param targetMonth the target month used to format the `yyyymm` suffix
     * @return the Redis key string for the remaining individual amount for the specified line and month
     */
    public String remainingIndivAmountKey(long lineId, YearMonth targetMonth) {
        // 잔량 해시 키는 월 단위(yyyymm)로 관리한다.
        String yyyymm = trafficRedisRuntimePolicy.formatYyyyMm(targetMonth);
        return namespaced("remaining_indiv_amount:" + lineId + ":" + yyyymm);
    }

    /**
     * Builds the Redis hash key for a family's remaining shared amount for a specific month.
     *
     * @param familyId    the family identifier
     * @param targetMonth the month used to produce the `yyyymm` suffix for the key
     * @return            the namespaced Redis key in the form `remaining_shared_amount:{familyId}:{yyyymm}`
     */
    public String remainingSharedAmountKey(long familyId, YearMonth targetMonth) {
        // 잔량 해시 키는 월 단위(yyyymm)로 관리한다.
        String yyyymm = trafficRedisRuntimePolicy.formatYyyyMm(targetMonth);
        return namespaced("remaining_shared_amount:" + familyId + ":" + yyyymm);
    }

    /**
     * Builds the Redis lock key for performing an individual refill for a specific line.
     *
     * @param lineId the identifier of the line
     * @return the Redis key string for the individual refill lock, prefixed with the configured namespace if present
     */
    public String indivRefillLockKey(long lineId) {
        return namespaced("indiv_refill_lock:" + lineId);
    }

    /**
     * Builds the Redis key for a distributed lock used when performing shared refill operations for a family.
     *
     * @param familyId the family identifier
     * @return the namespaced Redis key string for the family's shared refill lock
     */
    public String sharedRefillLockKey(long familyId) {
        return namespaced("shared_refill_lock:" + familyId);
    }

    /**
     * Builds the Redis key for QoS data for a given line.
     *
     * @param lineId the line identifier
     * @return the namespaced Redis key for the line's QoS (format: `qos:{lineId}` with optional namespace prefix)
     */
    public String qosKey(long lineId) {
        return namespaced("qos:" + lineId);
    }

    /**
     * Builds a namespaced Redis key for an individual speed bucket.
     *
     * The key is partitioned by second-granularity epoch time to isolate per-second buckets.
     *
     * @param lineId the line identifier
     * @param epochSecond the epoch second used to partition the speed bucket (seconds since Unix epoch)
     * @return the Redis key string for the individual speed bucket for the given line at the specified epoch second
     */
    public String speedBucketIndividualKey(long lineId, long epochSecond) {
        // 속도 버킷은 초 단위로 키를 분리한다.
        return namespaced("speed_bucket:individual:" + lineId + ":" + epochSecond);
    }

    /**
     * Builds the Redis key for a shared speed bucket for a family at a specific epoch second.
     *
     * @param familyId the family identifier the bucket belongs to
     * @param epochSecond the epoch second partition for the bucket
     * @return the namespaced Redis key string for the shared speed bucket
     */
    public String speedBucketSharedKey(long familyId, long epochSecond) {
        // 공유풀 속도 버킷은 초 단위로 키를 분리한다.
        return namespaced("speed_bucket:shared:" + familyId + ":" + epochSecond);
    }

    /**
     * Builds a namespaced Redis key pattern that matches individual speed-bucket keys for the given line.
     *
     * @param lineId the identifier of the line
     * @return a Redis key pattern string for individual speed buckets for the line (namespace applied if configured)
     */
    public String speedBucketIndividualPattern(long lineId) {
        return namespaced("speed_bucket:individual:" + lineId + ":*");
    }

    /**
     * Builds a namespaced Redis key pattern that matches shared speed-bucket entries for a family.
     *
     * @param familyId the family identifier
     * @return the namespaced Redis key pattern for shared speed buckets (suffix `:*` to match buckets)
     */
    public String speedBucketSharedPattern(long familyId) {
        return namespaced("speed_bucket:shared:" + familyId + ":*");
    }

    /**
     * Builds a namespaced Redis key for a dedupe run using the given trace identifier.
     *
     * @param traceId the trace identifier; must not be null and must contain non-whitespace characters after trimming
     * @return the namespaced Redis key for the dedupe run (format: "dedupe:run:{traceId}" with the configured namespace prefixed when present)
     * @throws NullPointerException if {@code traceId} is null
     * @throws IllegalArgumentException if {@code traceId} is blank after trimming
     */
    public String dedupeRunKey(String traceId) {
        // traceId는 필수이므로 빈 문자열은 허용하지 않는다.
        String normalizedTraceId = Objects.requireNonNull(traceId, "traceId must not be null").trim();
        if (normalizedTraceId.isEmpty()) {
            throw new IllegalArgumentException("traceId must not be blank");
        }
        return namespaced("dedupe:run:" + normalizedTraceId);
    }

    /**
     * Apply the configured Redis namespace to a key body.
     *
     * @param keyBody the unprefixed key string to namespace
     * @return the namespaced key (prefixed with "namespace:") when a non-blank namespace is configured, otherwise the original `keyBody`
     */
    private String namespaced(String keyBody) {
        // namespace가 비어 있으면 원본 키를 그대로 사용한다.
        String namespace = appRedisProperties.getNamespace();
        if (namespace == null || namespace.isBlank()) {
            return keyBody;
        }
        return namespace + ":" + keyBody;
    }
}
