package com.pooli.traffic.service.decision;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.YearMonth;
import java.util.List;
import java.util.UUID;

import com.pooli.traffic.service.outbox.RedisOutboxRecordService;
import com.pooli.traffic.service.outbox.TrafficRefillOutboxSupportService;
import com.pooli.traffic.service.policy.TrafficLinePolicyHydrationService;
import com.pooli.traffic.service.policy.TrafficPolicyBootstrapService;
import com.pooli.traffic.service.runtime.TrafficRedisRuntimePolicy;
import com.pooli.traffic.service.runtime.TrafficLuaScriptInfraService;
import com.pooli.traffic.service.runtime.TrafficQuotaCacheService;
import com.pooli.traffic.service.runtime.TrafficRedisKeyFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import com.pooli.monitoring.metrics.TrafficHydrateMetrics;
import com.pooli.monitoring.metrics.TrafficRefillMetrics;
import com.pooli.traffic.domain.TrafficDbRefillClaimResult;
import com.pooli.traffic.domain.TrafficLuaExecutionResult;
import com.pooli.traffic.domain.TrafficRefillPlan;
import com.pooli.traffic.domain.dto.request.TrafficPayloadReqDto;
import com.pooli.traffic.domain.enums.TrafficLuaStatus;
import com.pooli.traffic.domain.enums.TrafficPoolType;
import com.pooli.traffic.domain.enums.TrafficRefillGateStatus;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * 차감 결과에 따라 hydrate 및 refill 복구 흐름을 연결하는 어댑터 서비스입니다.
 */
@Slf4j
@Service
@Profile({"local", "traffic"})
@RequiredArgsConstructor
public class TrafficHydrateRefillAdapterService {

    private static final int HYDRATE_RETRY_MAX = 10;
    private static final int REFILL_RETRY_MAX = 1;
    private static final int DB_RETRY_MAX = 2;
    private static final long DB_RETRY_BACKOFF_MS = 50L;
    private static final long POLICY_REPEAT_BLOCK_ID = 1L;
    private static final long POLICY_IMMEDIATE_BLOCK_ID = 2L;
    private static final long POLICY_LINE_LIMIT_SHARED_ID = 3L;
    private static final long POLICY_LINE_LIMIT_DAILY_ID = 4L;
    private static final long POLICY_APP_DATA_ID = 5L;
    private static final long POLICY_APP_SPEED_ID = 6L;
    private static final long POLICY_APP_WHITELIST_ID = 7L;

    @Value("${app.traffic.hydrate-lock.enabled:true}")
    private boolean hydrateLockEnabled;

    @Value("${app.traffic.hydrate-lock.wait-ms:100}")
    private long hydrateLockWaitMs;

    @Qualifier("cacheStringRedisTemplate")
    private final StringRedisTemplate cacheStringRedisTemplate;
    private final TrafficLuaScriptInfraService trafficLuaScriptInfraService;
    private final TrafficRedisKeyFactory trafficRedisKeyFactory;
    private final TrafficRedisRuntimePolicy trafficRedisRuntimePolicy;
    private final TrafficQuotaSourcePort trafficQuotaSourcePort;
    private final TrafficQuotaCacheService trafficQuotaCacheService;
    private final TrafficLinePolicyHydrationService trafficLinePolicyHydrationService;
    private final TrafficPolicyBootstrapService trafficPolicyBootstrapService;
    private final TrafficHydrateMetrics trafficHydrateMetrics;
    private final TrafficRefillMetrics trafficRefillMetrics;
    private final RedisOutboxRecordService redisOutboxRecordService;
    private final TrafficRefillOutboxSupportService trafficRefillOutboxSupportService;

    /**
     * 개인풀 차감과 복구 흐름을 실행합니다.
     */
    public TrafficLuaExecutionResult executeIndividualWithRecovery(TrafficPayloadReqDto payload, long requestedDataBytes) {
        return executeWithRecovery(TrafficPoolType.INDIVIDUAL, payload, requestedDataBytes);
    }

    /**
     * 공유풀 차감과 복구 흐름을 실행합니다.
     */
    public TrafficLuaExecutionResult executeSharedWithRecovery(TrafficPayloadReqDto payload, long requestedDataBytes) {
        return executeWithRecovery(TrafficPoolType.SHARED, payload, requestedDataBytes);
    }

    /**
     * 풀 유형에 맞는 차감, hydrate, refill 복구 흐름을 순차 실행합니다.
     */
    private TrafficLuaExecutionResult executeWithRecovery(
            TrafficPoolType poolType,
            TrafficPayloadReqDto payload,
            long requestedDataBytes
    ) {
        if (!isPayloadValidForPool(poolType, payload)) {
            return errorResult();
        }

        try {
            trafficLinePolicyHydrationService.ensureLoaded(payload.getLineId());
        } catch (RuntimeException e) {
            log.error(
                    "traffic_line_policy_hydration_failed lineId={}",
                    payload.getLineId(),
                    e
            );
            return errorResult();
        }

        YearMonth targetMonth = resolveTargetMonth(payload);
        String balanceKey = resolveBalanceKey(poolType, payload, targetMonth);

        TrafficLuaExecutionResult initialResult = executeDeduct(poolType, payload, balanceKey, requestedDataBytes);

        TrafficLuaExecutionResult afterGlobalPolicyHydrateResult = handleGlobalPolicyHydrateIfNeeded(
                poolType,
                payload,
                balanceKey,
                requestedDataBytes,
                initialResult
        );

        TrafficLuaExecutionResult afterHydrateResult = handleHydrateIfNeeded(
                poolType,
                payload,
                targetMonth,
                balanceKey,
                requestedDataBytes,
                afterGlobalPolicyHydrateResult
        );

        return handleRefillIfNeeded(
                poolType,
                payload,
                targetMonth,
                balanceKey,
                requestedDataBytes,
                afterHydrateResult
        );
    }

    /**
     * GLOBAL_POLICY_HYDRATE 상태일 때 전역 정책 스냅샷을 다시 적재한 뒤 동일 Lua를 재시도합니다.
     */
    private TrafficLuaExecutionResult handleGlobalPolicyHydrateIfNeeded(
            TrafficPoolType poolType,
            TrafficPayloadReqDto payload,
            String balanceKey,
            long requestedDataBytes,
            TrafficLuaExecutionResult currentResult
    ) {
        if (currentResult.getStatus() != TrafficLuaStatus.GLOBAL_POLICY_HYDRATE) {
            return currentResult;
        }

        TrafficLuaExecutionResult retriedResult = currentResult;
        for (int retry = 0; retry < HYDRATE_RETRY_MAX; retry++) {
            try {
                // 기존 bootstrap lock 규칙을 재사용해 전역 정책 전체 스냅샷을 보정한다.
                trafficPolicyBootstrapService.hydrateOnDemand();
            } catch (RuntimeException e) {
                log.error(
                        "traffic_global_policy_hydrate_failed poolType={} traceId={} retry={}",
                        poolType,
                        payload.getTraceId(),
                        retry + 1,
                        e
                );
            }

            retriedResult = executeDeduct(poolType, payload, balanceKey, requestedDataBytes);
            if (retriedResult.getStatus() != TrafficLuaStatus.GLOBAL_POLICY_HYDRATE) {
                return retriedResult;
            }
            sleepHydrateLockWait();
        }

        log.error(
                "traffic_global_policy_hydrate_retry_exhausted poolType={} traceId={} balanceKey={}",
                poolType,
                payload.getTraceId(),
                balanceKey
        );
        return retriedResult;
    }

    /**
     * HYDRATE 상태일 때 캐시를 복구한 뒤 재차감합니다.
     */
    private TrafficLuaExecutionResult handleHydrateIfNeeded(
            TrafficPoolType poolType,
            TrafficPayloadReqDto payload,
            YearMonth targetMonth,
            String balanceKey,
            long requestedDataBytes,
            TrafficLuaExecutionResult currentResult
    ) {
        if (currentResult.getStatus() != TrafficLuaStatus.HYDRATE) {
            return currentResult;
        }

        TrafficLuaExecutionResult retriedResult = currentResult;
        String hydrateLockKey = resolveHydrateLockKey(poolType, payload);
        for (int retry = 0; retry < HYDRATE_RETRY_MAX; retry++) {
            if (!hydrateLockEnabled) {
                applyHydrate(poolType, payload, targetMonth, balanceKey);
            } else if (tryAcquireHydrateLock(hydrateLockKey, payload.getTraceId())) {
                try {
                    applyHydrate(poolType, payload, targetMonth, balanceKey);
                } finally {
                    trafficLuaScriptInfraService.executeLockRelease(hydrateLockKey, payload.getTraceId());
                }
            } else {
                sleepHydrateLockWait();
            }

            retriedResult = executeDeduct(poolType, payload, balanceKey, requestedDataBytes);
            if (retriedResult.getStatus() != TrafficLuaStatus.HYDRATE) {
                return retriedResult;
            }
        }

        log.error(
                "traffic_hydrate_retry_exhausted poolType={} traceId={} balanceKey={}",
                poolType,
                payload.getTraceId(),
                balanceKey
        );
        return retriedResult;
    }

    /**
     * DB 원본 잔량과 QoS 정보를 Redis 캐시에 반영합니다.
     */
    private void applyHydrate(
            TrafficPoolType poolType,
            TrafficPayloadReqDto payload,
            YearMonth targetMonth,
            String balanceKey
    ) {
        long monthlyExpireAt = trafficRedisRuntimePolicy.resolveMonthlyExpireAtEpochSeconds(targetMonth);
        trafficQuotaCacheService.hydrateBalance(balanceKey, monthlyExpireAt);
        if (poolType == TrafficPoolType.INDIVIDUAL) {
            long qosSpeedLimit = trafficQuotaSourcePort.loadIndividualQosSpeedLimit(payload);
            trafficQuotaCacheService.putQos(balanceKey, qosSpeedLimit);
        }
        trafficHydrateMetrics.incrementHydrate(poolType);
    }

    /**
     * NO_BALANCE 상태일 때 리필 게이트와 DB 차감을 거쳐 재차감합니다.
     */
    private TrafficLuaExecutionResult handleRefillIfNeeded(
            TrafficPoolType poolType,
            TrafficPayloadReqDto payload,
            YearMonth targetMonth,
            String balanceKey,
            long requestedDataBytes,
            TrafficLuaExecutionResult currentResult
    ) {
        if (currentResult.getStatus() != TrafficLuaStatus.NO_BALANCE) {
            return currentResult;
        }

        String lockKey = resolveLockKey(poolType, payload);
        long normalizedRequestedDataBytes = Math.max(0L, requestedDataBytes);
        long firstDeductedAmount = normalizeNonNegative(currentResult.getAnswer());
        long retryTargetData = clampRemaining(normalizedRequestedDataBytes - firstDeductedAmount);

        TrafficLuaExecutionResult retriedResult = currentResult;
        for (int retry = 0; retry < REFILL_RETRY_MAX; retry++) {
            long currentAmount = trafficQuotaCacheService.readAmountOrDefault(balanceKey, 0L);
            TrafficRefillPlan refillPlan = trafficQuotaSourcePort.resolveRefillPlan(poolType, payload);
            long delta = normalizeNonNegative(refillPlan == null ? null : refillPlan.getDelta());
            int bucketCount = normalizeNonNegativeInt(refillPlan == null ? null : refillPlan.getBucketCount());
            long requestedRefillUnit = normalizeNonNegative(refillPlan == null ? null : refillPlan.getRefillUnit());
            long threshold = Math.max(1L, normalizeNonNegative(refillPlan == null ? null : refillPlan.getThreshold()));
            String refillPlanSource = refillPlan == null || refillPlan.getSource() == null
                    ? "UNKNOWN"
                    : refillPlan.getSource();

            log.info(
                    "traffic_refill_plan_resolved poolType={} balanceKey={} currentAmount={} delta={} bucketCount={} refillUnit={} threshold={} source={}",
                    poolType,
                    balanceKey,
                    currentAmount,
                    delta,
                    bucketCount,
                    requestedRefillUnit,
                    threshold,
                    refillPlanSource
            );

            TrafficRefillGateStatus gateStatus = trafficLuaScriptInfraService.executeRefillGate(
                    lockKey,
                    balanceKey,
                    payload.getTraceId(),
                    TrafficRedisRuntimePolicy.LOCK_TTL_MS,
                    currentAmount,
                    threshold
            );

            if (gateStatus != TrafficRefillGateStatus.OK) {
                log.debug(
                        "traffic_refill_gate_not_ok poolType={} gateStatus={}",
                        poolType,
                        gateStatus
                );
                trafficRefillMetrics.increment(poolType.name(), "gate_" + gateStatus.name().toLowerCase());
                return retriedResult;
            }

            boolean lockOwned = trafficLuaScriptInfraService.executeLockHeartbeat(
                    lockKey,
                    payload.getTraceId(),
                    TrafficRedisRuntimePolicy.LOCK_TTL_MS
            );

            if (!lockOwned) {
                log.debug(
                        "traffic_refill_lock_not_owned poolType={} lockKey={}",
                        poolType,
                        lockKey
                );
                trafficRefillMetrics.increment(poolType.name(), "lock_not_owned");
                return retriedResult;
            }

            try {
                String refillUuid = UUID.randomUUID().toString();
                TrafficDbRefillClaimResult claimResult = claimRefillAmountFromDbWithRetry(
                        poolType,
                        payload,
                        targetMonth,
                        requestedRefillUnit,
                        refillUuid
                );
                long dbRemainingBefore = normalizeNonNegative(claimResult == null ? null : claimResult.getDbRemainingBefore());
                long actualRefillAmount = normalizeNonNegative(claimResult == null ? null : claimResult.getActualRefillAmount());
                long dbRemainingAfter = normalizeNonNegative(claimResult == null ? null : claimResult.getDbRemainingAfter());
                Long outboxRecordId = claimResult == null ? null : claimResult.getOutboxRecordId();
                String outboxRefillUuid = claimResult == null ? null : claimResult.getRefillUuid();
                // is_empty 플래그 갱신은 applyRefillWithIdempotency Lua 스크립트 내부에서
                // amount 증가와 원자적으로 처리된다. 여기서는 별도로 기록하지 않는다.
                if (actualRefillAmount <= 0) {
                    markOutboxSuccessIfPresent(outboxRecordId);
                    log.debug(
                            "traffic_refill_db_noop poolType={} requestedRefill={} threshold={} delta={} bucketCount={} source={} dbBefore={} actualRefill={} dbAfter={}",
                            poolType,
                            requestedRefillUnit,
                            threshold,
                            delta,
                            bucketCount,
                            refillPlanSource,
                            dbRemainingBefore,
                            actualRefillAmount,
                            dbRemainingAfter
                    );
                    trafficRefillMetrics.increment(poolType.name(), "db_noop");
                    return retriedResult;
                }

                long monthlyExpireAt = trafficRedisRuntimePolicy.resolveMonthlyExpireAtEpochSeconds(targetMonth);

                boolean lockStillOwned = trafficLuaScriptInfraService.executeLockHeartbeat(
                        lockKey,
                        payload.getTraceId(),
                        TrafficRedisRuntimePolicy.LOCK_TTL_MS
                );
                if (!lockStillOwned) {
                    log.warn(
                            "traffic_refill_lock_lost_before_redis_apply poolType={} lockKey={}",
                            poolType,
                            lockKey
                    );
                    markOutboxFailIfPresent(outboxRecordId);
                    trafficRefillOutboxSupportService.compensateRefillOnce(
                            outboxRecordId,
                            poolType,
                            payload,
                            actualRefillAmount
                    );
                    trafficRefillMetrics.increment(poolType.name(), "lock_lost");
                    return retriedResult;
                }

                try {
                    boolean applied = trafficQuotaCacheService.applyRefillWithIdempotency(
                            balanceKey,
                            trafficRefillOutboxSupportService.resolveIdempotencyKey(outboxRefillUuid),
                            outboxRefillUuid,
                            actualRefillAmount,
                            monthlyExpireAt,
                            trafficRefillOutboxSupportService.refillIdempotencyTtlSeconds(),
                            // DB 잔량 소진 여부를 Lua 내부에서 amount 증가와 함께 원자적으로 기록한다.
                            dbRemainingAfter <= 0
                    );
                    if (!applied) {
                        log.info(
                                "traffic_refill_idempotent_skip poolType={} balanceKey={} outboxId={} uuid={}",
                                poolType,
                                balanceKey,
                                outboxRecordId,
                                outboxRefillUuid
                        );
                        markOutboxSuccessIfPresent(outboxRecordId);
                        trafficRefillOutboxSupportService.clearIdempotency(outboxRefillUuid);
                        trafficRefillMetrics.increment(poolType.name(), "idempotent_skip");
                        TrafficLuaExecutionResult refillRetryResult = executeDeduct(
                                poolType,
                                payload,
                                balanceKey,
                                retryTargetData
                        );
                        retriedResult = mergeRefillRetryResult(
                                normalizedRequestedDataBytes,
                                firstDeductedAmount,
                                refillRetryResult
                        );
                        return retriedResult;
                    }

                    // refill + idempotency key 등록을 단일 Lua로 수행했으므로
                    // 여기서는 성공 상태 전이만 마무리한다.
                    markOutboxSuccessIfPresent(outboxRecordId);
                    trafficRefillOutboxSupportService.clearIdempotency(outboxRefillUuid);
                } catch (RuntimeException redisApplyException) {
                    markOutboxFailIfPresent(outboxRecordId);
                    RuntimeException unwrapped = trafficRefillOutboxSupportService.unwrapRuntimeException(redisApplyException);
                    trafficRefillMetrics.increment(
                            poolType.name(),
                            trafficRefillOutboxSupportService.isTimeoutFailure(unwrapped) ? "redis_timeout" : "redis_error"
                    );
                    return retriedResult;
                }

                log.info(
                        "traffic_refill_applied poolType={} balanceKey={} requestedRefill={} threshold={} delta={} bucketCount={} source={} dbBefore={} actualRefill={} dbAfter={}",
                        poolType,
                        balanceKey,
                        requestedRefillUnit,
                        threshold,
                        delta,
                        bucketCount,
                        refillPlanSource,
                        dbRemainingBefore,
                        actualRefillAmount,
                        dbRemainingAfter
                );
                trafficRefillMetrics.increment(poolType.name(), "refill_applied");

                TrafficLuaExecutionResult refillRetryResult = executeDeduct(
                        poolType,
                        payload,
                        balanceKey,
                        retryTargetData
                );
                retriedResult = mergeRefillRetryResult(
                        normalizedRequestedDataBytes,
                        firstDeductedAmount,
                        refillRetryResult
                );
                return retriedResult;
            } catch (RuntimeException e) {
                log.error(
                        "traffic_refill_db_claim_failed poolType={} balanceKey={} traceId={}",
                        poolType,
                        balanceKey,
                        payload.getTraceId(),
                        e
                );
                trafficRefillMetrics.increment(poolType.name(), "db_error");
                return retriedResult;
            } finally {
                trafficLuaScriptInfraService.executeLockRelease(lockKey, payload.getTraceId());
            }
        }

        return retriedResult;
    }

    /**
     * 1차 차감량과 리필 후 재차감량을 합쳐 최종 결과를 만듭니다.
     */
    private TrafficLuaExecutionResult mergeRefillRetryResult(
            long requestedDataBytes,
            long firstDeductedAmount,
            TrafficLuaExecutionResult refillRetryResult
    ) {
        long retriedDeductedAmount = normalizeNonNegative(refillRetryResult == null ? null : refillRetryResult.getAnswer());
        long mergedDeductedAmount = clampToMax(requestedDataBytes, safeAdd(firstDeductedAmount, retriedDeductedAmount));
        TrafficLuaStatus mergedStatus = refillRetryResult == null
                ? TrafficLuaStatus.ERROR
                : refillRetryResult.getStatus();

        return TrafficLuaExecutionResult.builder()
                .answer(mergedDeductedAmount)
                .status(mergedStatus)
                .build();
    }

    /**
     * 풀 유형에 맞는 Lua 차감 스크립트를 실행합니다.
     */
    private TrafficLuaExecutionResult executeDeduct(
            TrafficPoolType poolType,
            TrafficPayloadReqDto payload,
            String balanceKey,
            long requestedDataBytes
    ) {
        LocalDateTime now = LocalDateTime.now(trafficRedisRuntimePolicy.zoneId());
        LocalDate targetDate = now.toLocalDate();
        YearMonth targetUsageMonth = YearMonth.from(now);
        long nowEpochSecond = now.atZone(trafficRedisRuntimePolicy.zoneId()).toEpochSecond();
        int dayNum = now.getDayOfWeek().getValue() % 7;
        int secOfDay = now.toLocalTime().toSecondOfDay();
        long dailyExpireAt = trafficRedisRuntimePolicy.resolveDailyExpireAtEpochSeconds(targetDate);
        long monthlyExpireAt = trafficRedisRuntimePolicy.resolveMonthlyExpireAtEpochSeconds(targetUsageMonth);

        String policyRepeatKey = trafficRedisKeyFactory.policyKey(POLICY_REPEAT_BLOCK_ID);
        String policyImmediateKey = trafficRedisKeyFactory.policyKey(POLICY_IMMEDIATE_BLOCK_ID);
        String policyLineLimitSharedKey = trafficRedisKeyFactory.policyKey(POLICY_LINE_LIMIT_SHARED_ID);
        String policyLineLimitDailyKey = trafficRedisKeyFactory.policyKey(POLICY_LINE_LIMIT_DAILY_ID);
        String policyAppDataKey = trafficRedisKeyFactory.policyKey(POLICY_APP_DATA_ID);
        String policyAppSpeedKey = trafficRedisKeyFactory.policyKey(POLICY_APP_SPEED_ID);
        String policyAppWhitelistKey = trafficRedisKeyFactory.policyKey(POLICY_APP_WHITELIST_ID);

        String appWhitelistKey = trafficRedisKeyFactory.appWhitelistKey(payload.getLineId());
        String immediatelyBlockEndKey = trafficRedisKeyFactory.immediatelyBlockEndKey(payload.getLineId());
        String repeatBlockKey = trafficRedisKeyFactory.repeatBlockKey(payload.getLineId());
        String dailyTotalLimitKey = trafficRedisKeyFactory.dailyTotalLimitKey(payload.getLineId());
        String dailyTotalUsageKey = trafficRedisKeyFactory.dailyTotalUsageKey(payload.getLineId(), targetDate);
        String appDataDailyLimitKey = trafficRedisKeyFactory.appDataDailyLimitKey(payload.getLineId());
        String dailyAppUsageKey = trafficRedisKeyFactory.dailyAppUsageKey(payload.getLineId(), targetDate);
        String appSpeedLimitKey = trafficRedisKeyFactory.appSpeedLimitKey(payload.getLineId());

        return switch (poolType) {
            case INDIVIDUAL -> {
                String speedBucketKey = trafficRedisKeyFactory.speedBucketIndividualKey(payload.getLineId(), nowEpochSecond);
                List<String> keys = List.of(
                        balanceKey,
                        policyRepeatKey,
                        policyImmediateKey,
                        policyLineLimitDailyKey,
                        policyAppDataKey,
                        policyAppSpeedKey,
                        policyAppWhitelistKey,
                        appWhitelistKey,
                        immediatelyBlockEndKey,
                        repeatBlockKey,
                        dailyTotalLimitKey,
                        dailyTotalUsageKey,
                        appDataDailyLimitKey,
                        dailyAppUsageKey,
                        appSpeedLimitKey,
                        speedBucketKey
                );
                List<String> args = List.of(
                        String.valueOf(requestedDataBytes),
                        String.valueOf(payload.getAppId()),
                        String.valueOf(dayNum),
                        String.valueOf(secOfDay),
                        String.valueOf(nowEpochSecond),
                        String.valueOf(dailyExpireAt)
                );
                yield trafficLuaScriptInfraService.executeDeductIndividual(keys, args);
            }
            case SHARED -> {
                String monthlySharedLimitKey = trafficRedisKeyFactory.monthlySharedLimitKey(payload.getLineId());
                String monthlySharedUsageKey = trafficRedisKeyFactory.monthlySharedUsageKey(payload.getLineId(), targetUsageMonth);
                String speedBucketKey = trafficRedisKeyFactory.speedBucketSharedKey(payload.getFamilyId(), nowEpochSecond);
                List<String> keys = List.of(
                        balanceKey,
                        policyRepeatKey,
                        policyImmediateKey,
                        policyLineLimitSharedKey,
                        policyLineLimitDailyKey,
                        policyAppDataKey,
                        policyAppSpeedKey,
                        policyAppWhitelistKey,
                        appWhitelistKey,
                        immediatelyBlockEndKey,
                        repeatBlockKey,
                        dailyTotalLimitKey,
                        dailyTotalUsageKey,
                        monthlySharedLimitKey,
                        monthlySharedUsageKey,
                        appDataDailyLimitKey,
                        dailyAppUsageKey,
                        appSpeedLimitKey,
                        speedBucketKey
                );
                List<String> args = List.of(
                        String.valueOf(requestedDataBytes),
                        String.valueOf(payload.getAppId()),
                        String.valueOf(dayNum),
                        String.valueOf(secOfDay),
                        String.valueOf(nowEpochSecond),
                        String.valueOf(dailyExpireAt),
                        String.valueOf(monthlyExpireAt)
                );
                yield trafficLuaScriptInfraService.executeDeductShared(keys, args);
            }
        };
    }

    /**
     *
     * @return remaining_indiv/shared_amount ??
     */
    private String resolveBalanceKey(TrafficPoolType poolType, TrafficPayloadReqDto payload, YearMonth targetMonth) {
        return switch (poolType) {
            case INDIVIDUAL -> trafficRedisKeyFactory.remainingIndivAmountKey(payload.getLineId(), targetMonth);
            case SHARED -> trafficRedisKeyFactory.remainingSharedAmountKey(payload.getFamilyId(), targetMonth);
        };
    }

    /**
     *
     * @return indiv/shared refill lock ??
     */
    private String resolveLockKey(TrafficPoolType poolType, TrafficPayloadReqDto payload) {
        return switch (poolType) {
            case INDIVIDUAL -> trafficRedisKeyFactory.indivRefillLockKey(payload.getLineId());
            case SHARED -> trafficRedisKeyFactory.sharedRefillLockKey(payload.getFamilyId());
        };
    }

    /**
     *
     * @return indiv/shared hydrate lock ??
     */
    private String resolveHydrateLockKey(TrafficPoolType poolType, TrafficPayloadReqDto payload) {
        return switch (poolType) {
            case INDIVIDUAL -> trafficRedisKeyFactory.indivHydrateLockKey(payload.getLineId());
            case SHARED -> trafficRedisKeyFactory.sharedHydrateLockKey(payload.getFamilyId());
        };
    }

    /**
     * hydrate 처리를 위한 분산 락 획득을 시도합니다.
     */
    private boolean tryAcquireHydrateLock(String lockKey, String traceId) {
        Boolean acquired = cacheStringRedisTemplate.opsForValue().setIfAbsent(
                lockKey,
                traceId,
                Duration.ofMillis(TrafficRedisRuntimePolicy.LOCK_TTL_MS)
        );
        return Boolean.TRUE.equals(acquired);
    }

    /**
     * hydrate 락 재시도 전에 잠시 대기합니다.
     */
    private void sleepHydrateLockWait() {
        long waitMs = Math.max(0L, hydrateLockWaitMs);
        if (waitMs <= 0) {
            return;
        }
        try {
            Thread.sleep(waitMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * 이벤트가 속한 사용 월을 계산합니다.
     */
    private YearMonth resolveTargetMonth(TrafficPayloadReqDto payload) {
        Long enqueuedAt = payload.getEnqueuedAt();
        if (enqueuedAt == null || enqueuedAt <= 0) {
            return YearMonth.now(trafficRedisRuntimePolicy.zoneId());
        }

        return YearMonth.from(Instant.ofEpochMilli(enqueuedAt).atZone(trafficRedisRuntimePolicy.zoneId()));
    }

    /**
     * 풀 유형에 필요한 payload 필수값이 모두 존재하는지 확인합니다.
     */
    private boolean isPayloadValidForPool(TrafficPoolType poolType, TrafficPayloadReqDto payload) {
        if (payload == null) {
            return false;
        }
        if (payload.getTraceId() == null || payload.getTraceId().isBlank()) {
            return false;
        }
        if (payload.getApiTotalData() == null || payload.getApiTotalData() < 0) {
            return false;
        }
        if (payload.getLineId() == null || payload.getLineId() <= 0) {
            return false;
        }
        if (payload.getAppId() == null || payload.getAppId() < 0) {
            return false;
        }

        return switch (poolType) {
            case INDIVIDUAL -> true;
            case SHARED -> payload.getFamilyId() != null && payload.getFamilyId() > 0;
        };
    }

    /**
     *
     * @return answer=-1, status=ERROR
     */
    private TrafficLuaExecutionResult errorResult() {
        return TrafficLuaExecutionResult.builder()
                .answer(-1L)
                .status(TrafficLuaStatus.ERROR)
                .build();
    }

    /**
     * Long 값을 0 이상의 값으로 보정합니다.
     */
    private long normalizeNonNegative(Long value) {
        if (value == null || value <= 0) {
            return 0L;
        }
        return value;
    }

    /**
     * Integer 값을 0 이상의 값으로 보정합니다.
     */
    private int normalizeNonNegativeInt(Integer value) {
        if (value == null || value <= 0) {
            return 0;
        }
        return value;
    }

    /**
     * 남은 차감 대상 값을 0 이상으로 보정합니다.
     */
    private long clampRemaining(long value) {
        if (value <= 0) {
            return 0L;
        }
        return value;
    }

    /**
     * 값이 최대 허용치를 넘지 않도록 제한합니다.
     */
    private long clampToMax(long maxValue, long value) {
        long normalizedMaxValue = Math.max(0L, maxValue);
        if (value <= 0) {
            return 0L;
        }
        return Math.min(normalizedMaxValue, value);
    }

    /**
     * 오버플로 없이 두 값을 더합니다.
     */
    private long safeAdd(long left, long right) {
        if (left <= 0) {
            return Math.max(0L, right);
        }
        if (right <= 0) {
            return left;
        }
        if (left > Long.MAX_VALUE - right) {
            return Long.MAX_VALUE;
        }
        return left + right;
    }

    /**
     * outbox 레코드가 존재할 때 SUCCESS로 전이합니다.
     */
    private void markOutboxSuccessIfPresent(Long outboxRecordId) {
        if (outboxRecordId == null || outboxRecordId <= 0) {
            return;
        }
        redisOutboxRecordService.markSuccess(outboxRecordId);
    }

    /**
     * outbox 레코드가 존재할 때 FAIL로 전이합니다.
     */
    private void markOutboxFailIfPresent(Long outboxRecordId) {
        if (outboxRecordId == null || outboxRecordId <= 0) {
            return;
        }
        redisOutboxRecordService.markFail(outboxRecordId);
    }

    /**
     * DB 리필 차감을 재시도 정책과 함께 수행합니다.
     */
    private TrafficDbRefillClaimResult claimRefillAmountFromDbWithRetry(
            TrafficPoolType poolType,
            TrafficPayloadReqDto payload,
            YearMonth targetMonth,
            long requestedRefillAmount,
            String refillUuid
    ) {
        RuntimeException lastException = null;
        for (int retryCount = 0; retryCount <= DB_RETRY_MAX; retryCount++) {
            try {
                return trafficQuotaSourcePort.claimRefillAmountFromDb(
                        poolType,
                        payload,
                        targetMonth,
                        requestedRefillAmount,
                        refillUuid
                );
            } catch (RuntimeException e) {
                lastException = e;
                boolean retryable = isRetryableDbException(e);
                if (!retryable || retryCount >= DB_RETRY_MAX) {
                    throw e;
                }

                log.warn(
                        "traffic_refill_db_retry poolType={} traceId={} retry={}/{}",
                        poolType,
                        payload.getTraceId(),
                        retryCount + 1,
                        DB_RETRY_MAX
                );
                sleepDbRetryBackoff();
            }
        }

        throw lastException == null
                ? new IllegalStateException("traffic_refill_db_retry_exhausted")
                : lastException;
    }

    /**
     * DB 예외가 재시도 가능한 종류인지 판별합니다.
     */
    private boolean isRetryableDbException(RuntimeException exception) {
        Throwable current = exception;
        while (current != null) {
            String message = current.getMessage();
            if (message != null) {
                String normalized = message.toLowerCase();
                if (normalized.contains("deadlock") || normalized.contains("lock wait timeout")) {
                    return true;
                }
            }
            current = current.getCause();
        }
        return false;
    }

    /**
     * DB 재시도 전 backoff 시간만큼 대기합니다.
     */
    private void sleepDbRetryBackoff() {
        try {
            Thread.sleep(DB_RETRY_BACKOFF_MS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
