package com.pooli.traffic.service.runtime;

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
@Profile({"local", "api", "traffic"})
@RequiredArgsConstructor
public class TrafficRedisKeyFactory {

    private final AppRedisProperties appRedisProperties;
    private final TrafficRedisRuntimePolicy trafficRedisRuntimePolicy;

    /**
      * 입력 식별자와 정책 규칙을 기준으로 Redis 키 문자열을 생성합니다.
     */
    public String policyKey(long policyId) {
        return namespaced("policy:" + policyId);
    }

    /**
     * 부팅 시 policy 전역 활성화 bootstrap 단일 실행을 위한 분산락 키입니다.
     */
    public String policyBootstrapLockKey() {
        return namespaced("policy:bootstrap:lock");
    }

    /**
     * 마지막 policy bootstrap/reconciliation 성공 시각(epoch seconds)을 저장하는 키입니다.
     */
    public String policyBootstrapVersionKey() {
        return namespaced("policy_bootstrap_version");
    }

    /**
     * 회선 정책(on-demand hydrate) 완료 여부를 나타내는 준비 키입니다.
     */
    public String linePolicyReadyKey(long lineId) {
        return namespaced("line_policy_ready:" + lineId);
    }

    /**
     * 회선 정책 hydrate의 단일 실행을 위한 분산락 키입니다.
     */
    public String linePolicyHydrateLockKey(long lineId) {
        return namespaced("line_policy_hydrate_lock:" + lineId);
    }

    /**
      * 입력 식별자와 정책 규칙을 기준으로 Redis 키 문자열을 생성합니다.
     */
    public String dailyTotalLimitKey(long lineId) {
        return namespaced("daily_total_limit:" + lineId);
    }

    /**
      * 입력 식별자와 정책 규칙을 기준으로 Redis 키 문자열을 생성합니다.
     */
    public String appDataDailyLimitKey(long lineId) {
        return namespaced("app_data_daily_limit:" + lineId);
    }

    /**
      * 입력 식별자와 정책 규칙을 기준으로 Redis 키 문자열을 생성합니다.
     */
    public String appSpeedLimitKey(long lineId) {
        return namespaced("app_speed_limit:" + lineId);
    }

    /**
      * 입력 식별자와 정책 규칙을 기준으로 Redis 키 문자열을 생성합니다.
     */
    public String appWhitelistKey(long lineId) {
        return namespaced("app_whitelist:" + lineId);
    }

    /**
      * 입력 식별자와 정책 규칙을 기준으로 Redis 키 문자열을 생성합니다.
     */
    public String monthlySharedLimitKey(long lineId) {
        return namespaced("monthly_shared_limit:" + lineId);
    }

    /**
      * 입력 식별자와 정책 규칙을 기준으로 Redis 키 문자열을 생성합니다.
     */
    public String dailyTotalUsageKey(long lineId, LocalDate targetDate) {
        // 일별 집계 키는 yyyymmdd suffix를 사용한다.
        String yyyymmdd = trafficRedisRuntimePolicy.formatYyyyMmDd(targetDate);
        return namespaced("daily_total_usage:" + lineId + ":" + yyyymmdd);
    }

    /**
      * 입력 식별자와 정책 규칙을 기준으로 Redis 키 문자열을 생성합니다.
     */
    public String dailyAppUsageKey(long lineId, LocalDate targetDate) {
        // 일별 집계 키는 yyyymmdd suffix를 사용한다.
        String yyyymmdd = trafficRedisRuntimePolicy.formatYyyyMmDd(targetDate);
        return namespaced("daily_app_usage:" + lineId + ":" + yyyymmdd);
    }

    /**
      * 입력 식별자와 정책 규칙을 기준으로 Redis 키 문자열을 생성합니다.
     */
    public String monthlySharedUsageKey(long lineId, YearMonth targetMonth) {
        // 월별 집계 키는 yyyymm suffix를 사용한다.
        String yyyymm = trafficRedisRuntimePolicy.formatYyyyMm(targetMonth);
        return namespaced("monthly_shared_usage:" + lineId + ":" + yyyymm);
    }

    /**
      * 입력 식별자와 정책 규칙을 기준으로 Redis 키 문자열을 생성합니다.
     */
    public String immediatelyBlockEndKey(long lineId) {
        return namespaced("immediately_block_end:" + lineId);
    }

    /**
      * 입력 식별자와 정책 규칙을 기준으로 Redis 키 문자열을 생성합니다.
     */
    public String repeatBlockKey(long lineId) {
        return namespaced("repeat_block:" + lineId);
    }

    /**
      * 입력 식별자와 정책 규칙을 기준으로 Redis 키 문자열을 생성합니다.
     */
    public String remainingIndivAmountKey(long lineId, YearMonth targetMonth) {
        // 잔량 해시 키는 월 단위(yyyymm)로 관리한다.
        String yyyymm = trafficRedisRuntimePolicy.formatYyyyMm(targetMonth);
        return namespaced("remaining_indiv_amount:" + lineId + ":" + yyyymm);
    }

    /**
      * 입력 식별자와 정책 규칙을 기준으로 Redis 키 문자열을 생성합니다.
     */
    public String remainingSharedAmountKey(long familyId, YearMonth targetMonth) {
        // 잔량 해시 키는 월 단위(yyyymm)로 관리한다.
        String yyyymm = trafficRedisRuntimePolicy.formatYyyyMm(targetMonth);
        return namespaced("remaining_shared_amount:" + familyId + ":" + yyyymm);
    }

    /**
      * 입력 식별자와 정책 규칙을 기준으로 Redis 키 문자열을 생성합니다.
     */
    public String indivRefillLockKey(long lineId) {
        return namespaced("indiv_refill_lock:" + lineId);
    }

    /**
      * 입력 식별자와 정책 규칙을 기준으로 Redis 키 문자열을 생성합니다.
     */
    public String sharedRefillLockKey(long familyId) {
        return namespaced("shared_refill_lock:" + familyId);
    }

    /**
      * 입력 식별자와 정책 규칙을 기준으로 Redis 키 문자열을 생성합니다.
     */
    public String indivHydrateLockKey(long lineId) {
        return namespaced("indiv_hydrate_lock:" + lineId);
    }

    /**
      * 입력 식별자와 정책 규칙을 기준으로 Redis 키 문자열을 생성합니다.
     */
    public String sharedHydrateLockKey(long familyId) {
        return namespaced("shared_hydrate_lock:" + familyId);
    }

    /**
      * 입력 식별자와 정책 규칙을 기준으로 Redis 키 문자열을 생성합니다.
     */
    public String qosKey(long lineId) {
        return namespaced("qos:" + lineId);
    }

    /**
      * 입력 식별자와 정책 규칙을 기준으로 Redis 키 문자열을 생성합니다.
     */
    public String speedBucketIndividualKey(long lineId, long epochSecond) {
        // 속도 버킷은 초 단위로 키를 분리한다.
        return namespaced("speed_bucket:individual:" + lineId + ":" + epochSecond);
    }

    /**
      * 입력 식별자와 정책 규칙을 기준으로 Redis 키 문자열을 생성합니다.
     */
    public String speedBucketSharedKey(long familyId, long epochSecond) {
        // 공유풀 속도 버킷은 초 단위로 키를 분리한다.
        return namespaced("speed_bucket:shared:" + familyId + ":" + epochSecond);
    }

    /**
      * 입력 식별자와 정책 규칙을 기준으로 Redis 키 문자열을 생성합니다.
     */
    public String speedBucketIndividualPattern(long lineId) {
        return namespaced("speed_bucket:individual:" + lineId + ":*");
    }

    /**
      * 입력 식별자와 정책 규칙을 기준으로 Redis 키 문자열을 생성합니다.
     */
    public String speedBucketSharedPattern(long familyId) {
        return namespaced("speed_bucket:shared:" + familyId + ":*");
    }

    /**
      * 입력 식별자와 정책 규칙을 기준으로 Redis 키 문자열을 생성합니다.
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
     * 리필 요청 멱등키를 생성합니다.
     */
    public String refillIdempotencyKey(String uuid) {
        String normalizedUuid = Objects.requireNonNull(uuid, "uuid must not be null").trim();
        if (normalizedUuid.isEmpty()) {
            throw new IllegalArgumentException("uuid must not be blank");
        }
        return namespaced("refill:idempotency:" + normalizedUuid);
    }

    /**
      * `namespaced` 처리 목적에 맞는 핵심 로직을 수행합니다.
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
