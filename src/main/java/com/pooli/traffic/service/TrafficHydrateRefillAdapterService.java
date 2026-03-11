package com.pooli.traffic.service;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.YearMonth;
import java.util.List;

import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

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
 * HYDRATE/REFILL 어댑터를 수행하는 서비스입니다.
 * 개인풀/공유풀 차감 Lua 결과를 보고 hydrate 1회 재시도, refill gate/lock 흐름을 처리합니다.
 */
@Slf4j
@Service
@Profile({"local", "traffic"})
@RequiredArgsConstructor
public class TrafficHydrateRefillAdapterService {

    private static final int HYDRATE_RETRY_MAX = 1;
    private static final int REFILL_RETRY_MAX = 1;
    private static final long POLICY_REPEAT_BLOCK_ID = 1L;
    private static final long POLICY_IMMEDIATE_BLOCK_ID = 2L;
    private static final long POLICY_LINE_LIMIT_SHARED_ID = 3L;
    private static final long POLICY_LINE_LIMIT_DAILY_ID = 4L;
    private static final long POLICY_APP_DATA_ID = 5L;
    private static final long POLICY_APP_SPEED_ID = 6L;
    private static final long POLICY_APP_WHITELIST_ID = 7L;

    private final TrafficLuaScriptInfraService trafficLuaScriptInfraService;
    private final TrafficRedisKeyFactory trafficRedisKeyFactory;
    private final TrafficRedisRuntimePolicy trafficRedisRuntimePolicy;
    private final TrafficQuotaSourcePort trafficQuotaSourcePort;
    private final TrafficQuotaCacheService trafficQuotaCacheService;
    private final TrafficLinePolicyHydrationService trafficLinePolicyHydrationService;

    /**
     * Executes the deduction flow for an individual pool, including hydrate and refill recovery when needed.
     *
     * Performs an initial individual-pool Lua deduction and, if required, attempts a hydrate recovery and/or a refill (gate, lock, DB claim, Redis refill) followed by a same-tick reattempt of deduction.
     *
     * @param payload                request context containing traceId, lineId, familyId, appId, and related data
     * @param currentTickTargetData  target number of bytes to process for the current tick
     * @return                       the final TrafficLuaExecutionResult from the individual-pool deduction path
     */
    public TrafficLuaExecutionResult executeIndividualWithRecovery(TrafficPayloadReqDto payload, long currentTickTargetData) {
        // 개인풀 분기 처리를 공통 메서드로 위임해 중복 코드를 줄인다.
        return executeWithRecovery(TrafficPoolType.INDIVIDUAL, payload, currentTickTargetData);
    }

    /**
     * Execute the shared-pool deduction flow, applying hydrate and refill recovery rules while
     * interpreting balance/lock/owner identifiers by the shared familyId.
     *
     * @param payload context for the shared-pool request (trace, line, family, app and usage details)
     * @param currentTickTargetData target number of bytes to deduct from the shared pool for this tick
     * @return the final TrafficLuaExecutionResult containing the Lua script answer and status for the shared-pool deduction
     */
    public TrafficLuaExecutionResult executeSharedWithRecovery(TrafficPayloadReqDto payload, long currentTickTargetData) {
        // 공유풀 분기 처리를 공통 메서드로 위임해 중복 코드를 줄인다.
        return executeWithRecovery(TrafficPoolType.SHARED, payload, currentTickTargetData);
    }

    /**
     * Orchestrates the common recovery flow for individual and shared traffic pools.
     *
     * <p>Performs an initial Lua deduction, optionally attempts a hydrate retry when hydration is required,
     * and optionally attempts a refill flow when no balance is available, then returns the resulting execution state.
     *
     * @param poolType the pool type to process (INDIVIDUAL or SHARED)
     * @param payload request context containing identifiers and usage information
     * @param currentTickTargetData target byte amount for the current tick
     * @return the final TrafficLuaExecutionResult reflecting the outcome after hydrate/refill recovery steps
     */
    private TrafficLuaExecutionResult executeWithRecovery(
            TrafficPoolType poolType,
            TrafficPayloadReqDto payload,
            long currentTickTargetData
    ) {
        // 필수 값이 비어 있으면 이후 키/락 계산이 불가능하므로 ERROR로 즉시 종료한다.
        if (!isPayloadValidForPool(poolType, payload)) {
            return errorResult();
        }

        try {
            trafficLinePolicyHydrationService.ensureLoaded(payload.getLineId());
        } catch (RuntimeException e) {
            log.error(
                    "traffic_line_policy_hydration_failed traceId={} lineId={}",
                    payload.getTraceId(),
                    payload.getLineId(),
                    e
            );
            return errorResult();
        }

        YearMonth targetMonth = resolveTargetMonth(payload);
        String balanceKey = resolveBalanceKey(poolType, payload, targetMonth);

        // 1차 Lua 차감 실행
        TrafficLuaExecutionResult initialResult = executeDeduct(poolType, payload, balanceKey, currentTickTargetData);

        // HYDRATE 분기: 키 미존재 시 hydrate -> 동일 tick 1회 재시도
        TrafficLuaExecutionResult afterHydrateResult = handleHydrateIfNeeded(
                poolType,
                payload,
                targetMonth,
                balanceKey,
                currentTickTargetData,
                initialResult
        );

        // NO_BALANCE 분기: refill gate/lock 성공 시 refill -> 동일 tick 1회 재시도
        return handleRefillIfNeeded(
                poolType,
                payload,
                targetMonth,
                balanceKey,
                currentTickTargetData,
                afterHydrateResult
        );
    }

    /**
         * Attempts to recover from a HYDRATE response by restoring the Redis balance from the source and retrying deduction once.
         *
         * <p>Behavior:
         * - If the provided result status is not HYDRATE, the original result is returned unchanged.
         * - If the status is HYDRATE, restores the Redis balance using the source-provided initial amount and re-executes the deduct Lua call up to one retry.
         * - If the retry yields a non-HYDRATE status, that result is returned; otherwise the final HYDRATE result is returned unchanged.
         *
         * @param poolType the pool type being processed (INDIVIDUAL or SHARED)
         * @param payload request context containing identifiers and usage data
         * @param targetMonth month used to compute balance expiry and keys
         * @param balanceKey Redis key holding the remaining balance to restore
         * @param currentTickTargetData the target byte amount for the current tick
         * @param currentResult the initial Lua execution result that triggered (or did not trigger) hydrate
         * @return the TrafficLuaExecutionResult after hydrate/retry processing, or the original result if no hydrate was performed
         */
    private TrafficLuaExecutionResult handleHydrateIfNeeded(
            TrafficPoolType poolType,
            TrafficPayloadReqDto payload,
            YearMonth targetMonth,
            String balanceKey,
            long currentTickTargetData,
            TrafficLuaExecutionResult currentResult
    ) {
        if (currentResult.getStatus() != TrafficLuaStatus.HYDRATE) {
            return currentResult;
        }

        TrafficLuaExecutionResult retriedResult = currentResult;
        for (int retry = 0; retry < HYDRATE_RETRY_MAX; retry++) {
            // DB hydrate 연동 전 단계이므로 source port가 제공하는 초기값으로 키를 복구한다.
            long initialAmount = trafficQuotaSourcePort.loadInitialAmount(poolType, payload, targetMonth);
            long monthlyExpireAt = trafficRedisRuntimePolicy.resolveMonthlyExpireAtEpochSeconds(targetMonth);
            trafficQuotaCacheService.hydrateBalance(balanceKey, initialAmount, monthlyExpireAt);

            retriedResult = executeDeduct(poolType, payload, balanceKey, currentTickTargetData);
            if (retriedResult.getStatus() != TrafficLuaStatus.HYDRATE) {
                // HYDRATE에서 벗어나면 즉시 결과를 반환한다.
                return retriedResult;
            }
        }

        // 재시도 후에도 HYDRATE면 상위 오케스트레이터가 실패 분기로 처리할 수 있도록 그대로 반환한다.
        return retriedResult;
    }

    /**
     * Attempt a refill when the current result indicates no balance, claim refill amount from the database,
     * apply the refill to Redis, and re-run the deduction once for the same tick if refill is applied.
     *
     * <p>The method performs gate and lock checks before claiming DB refill amounts, ensures the refill lock
     * is released in all cases, and returns early if gates or lock ownership prevent a refill.
     *
     * @param poolType the pool type to operate on (INDIVIDUAL or SHARED)
     * @param payload the request context containing identifiers and trace information
     * @param targetMonth the YearMonth used to compute monthly expiry for Redis entries
     * @param balanceKey the Redis key that holds the current remaining balance for the target pool
     * @param currentTickTargetData the byte amount targeted for the current tick deduction
     * @param currentResult the execution result observed before attempting refill; method only proceeds when its status is NO_BALANCE
     * @return the execution result after a potential refill and a single reattempted deduction, or the original result if no refill occurred
     */
    private TrafficLuaExecutionResult handleRefillIfNeeded(
            TrafficPoolType poolType,
            TrafficPayloadReqDto payload,
            YearMonth targetMonth,
            String balanceKey,
            long currentTickTargetData,
            TrafficLuaExecutionResult currentResult
    ) {
        if (currentResult.getStatus() != TrafficLuaStatus.NO_BALANCE) {
            return currentResult;
        }

        String lockKey = resolveLockKey(poolType, payload);

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
                    "traffic_refill_plan_resolved traceId={} poolType={} balanceKey={} currentAmount={} delta={} bucketCount={} refillUnit={} threshold={} source={}",
                    payload.getTraceId(),
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
                    payload.getTraceId(),
                    TrafficRedisRuntimePolicy.LOCK_TTL_MS,
                    currentAmount,
                    threshold
            );

            if (gateStatus != TrafficRefillGateStatus.OK) {
                // WAIT/SKIP/FAIL이면 현재 tick에서 리필을 진행하지 않고 기존 결과를 유지한다.
                log.debug(
                        "traffic_refill_gate_not_ok traceId={} poolType={} gateStatus={}",
                        payload.getTraceId(),
                        poolType,
                        gateStatus
                );
                return retriedResult;
            }

            boolean lockOwned = trafficLuaScriptInfraService.executeLockHeartbeat(
                    lockKey,
                    payload.getTraceId(),
                    TrafficRedisRuntimePolicy.LOCK_TTL_MS
            );

            if (!lockOwned) {
                // lock 소유권이 없으면 동시성 충돌 가능성이 있어 리필을 건너뛴다.
                log.debug(
                        "traffic_refill_lock_not_owned traceId={} poolType={} lockKey={}",
                        payload.getTraceId(),
                        poolType,
                        lockKey
                );
                return retriedResult;
            }

            try {
                TrafficDbRefillClaimResult claimResult = trafficQuotaSourcePort.claimRefillAmountFromDb(
                        poolType,
                        payload,
                        targetMonth,
                        requestedRefillUnit
                );
                long dbRemainingBefore = normalizeNonNegative(claimResult == null ? null : claimResult.getDbRemainingBefore());
                long actualRefillAmount = normalizeNonNegative(claimResult == null ? null : claimResult.getActualRefillAmount());
                long dbRemainingAfter = normalizeNonNegative(claimResult == null ? null : claimResult.getDbRemainingAfter());
                if (actualRefillAmount <= 0) {
                    // DB에서 실제 차감된 양이 없으면 Redis 충전 없이 현재 결과를 유지한다.
                    log.debug(
                            "traffic_refill_db_noop traceId={} poolType={} requestedRefill={} threshold={} delta={} bucketCount={} source={} dbBefore={} actualRefill={} dbAfter={}",
                            payload.getTraceId(),
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
                    return retriedResult;
                }

                long monthlyExpireAt = trafficRedisRuntimePolicy.resolveMonthlyExpireAtEpochSeconds(targetMonth);

                // 리필 작업 동안 lock TTL이 만료되지 않도록 heartbeat를 한번 더 수행한다.
                trafficLuaScriptInfraService.executeLockHeartbeat(
                        lockKey,
                        payload.getTraceId(),
                        TrafficRedisRuntimePolicy.LOCK_TTL_MS
                );
                trafficQuotaCacheService.refillBalance(balanceKey, actualRefillAmount, monthlyExpireAt);
                log.info(
                        "traffic_refill_applied traceId={} poolType={} balanceKey={} requestedRefill={} threshold={} delta={} bucketCount={} source={} dbBefore={} actualRefill={} dbAfter={}",
                        payload.getTraceId(),
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

                // 리필 후 동일 tick 차감을 1회 재시도한다.
                retriedResult = executeDeduct(poolType, payload, balanceKey, currentTickTargetData);
                return retriedResult;
            } finally {
                // 성공/실패와 무관하게 lock은 반드시 소유자 기준으로 해제한다.
                trafficLuaScriptInfraService.executeLockRelease(lockKey, payload.getTraceId());
            }
        }

        return retriedResult;
    }

    /**
         * Execute the deduction Lua script appropriate for the given pool type.
         *
         * @param poolType the pool type (INDIVIDUAL or SHARED) determining which Lua script and keys to use
         * @param payload request payload containing appId, lineId, familyId and other request metadata used to build keys/args
         * @param balanceKey Redis key holding the target balance to be deducted
         * @param currentTickTargetData number of bytes targeted for deduction in the current tick
         * @return the Lua deduction result containing the script `answer` and `status`
         */
    private TrafficLuaExecutionResult executeDeduct(
            TrafficPoolType poolType,
            TrafficPayloadReqDto payload,
            String balanceKey,
            long currentTickTargetData
    ) {
        // 정책 게이트/사용량 키를 Lua에서 함께 처리할 수 있도록 현재 시각 기반 파생 키를 구성한다.
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

        // 풀 유형에 맞는 Lua 스크립트를 선택해 차감 실행한다.
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
                        String.valueOf(currentTickTargetData),
                        String.valueOf(payload.getAppId()),
                        String.valueOf(dayNum),
                        String.valueOf(secOfDay),
                        String.valueOf(nowEpochSecond),
                        String.valueOf(dailyExpireAt)
                );
                yield trafficLuaScriptInfraService.executeDeductIndivTick(keys, args);
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
                        String.valueOf(currentTickTargetData),
                        String.valueOf(payload.getAppId()),
                        String.valueOf(dayNum),
                        String.valueOf(secOfDay),
                        String.valueOf(nowEpochSecond),
                        String.valueOf(dailyExpireAt),
                        String.valueOf(monthlyExpireAt)
                );
                yield trafficLuaScriptInfraService.executeDeductSharedTick(keys, args);
            }
        };
    }

    /**
     * Resolve the Redis balance key for the given pool type and target month.
     *
     * @param poolType    the pool type (INDIVIDUAL or SHARED)
     * @param payload     request context containing identifiers used to build the key:
     *                    for INDIVIDUAL uses payload.getLineId(), for SHARED uses payload.getFamilyId()
     * @param targetMonth the month used to compute the key suffix
     * @return the Redis key for the remaining amount for the specified pool and month
     */
    private String resolveBalanceKey(TrafficPoolType poolType, TrafficPayloadReqDto payload, YearMonth targetMonth) {
        // 풀 유형마다 잔량 키 구조가 다르므로 분기해 생성한다.
        return switch (poolType) {
            case INDIVIDUAL -> trafficRedisKeyFactory.remainingIndivAmountKey(payload.getLineId(), targetMonth);
            case SHARED -> trafficRedisKeyFactory.remainingSharedAmountKey(payload.getFamilyId(), targetMonth);
        };
    }

    /**
     * Resolve the Redis refill-lock key for the specified pool type and request payload.
     *
     * @param poolType the pool type (INDIVIDUAL or SHARED) used to select which key to build
     * @param payload  request context containing the identifier used to construct the key (lineId for individual, familyId for shared)
     * @return the Redis key string used as the refill lock for the selected pool
     */
    private String resolveLockKey(TrafficPoolType poolType, TrafficPayloadReqDto payload) {
        // 리필 lock 키도 풀 유형마다 다르므로 분기해 생성한다.
        return switch (poolType) {
            case INDIVIDUAL -> trafficRedisKeyFactory.indivRefillLockKey(payload.getLineId());
            case SHARED -> trafficRedisKeyFactory.sharedRefillLockKey(payload.getFamilyId());
        };
    }

    /**
     * Determine the YearMonth to use for DB/Redis monthly keys based on the request.
     *
     * @param payload request context whose `enqueuedAt` timestamp will be used when present and valid
     * @return the YearMonth derived from `payload.enqueuedAt` if it is a positive timestamp; otherwise the current YearMonth in the configured runtime zone
     */
    private YearMonth resolveTargetMonth(TrafficPayloadReqDto payload) {
        Long enqueuedAt = payload.getEnqueuedAt();
        if (enqueuedAt == null || enqueuedAt <= 0) {
            // enqueue 시각이 없으면 현재 시각(Asia/Seoul) 기준 월을 사용한다.
            return YearMonth.now(trafficRedisRuntimePolicy.zoneId());
        }

        // payload에 담긴 enqueue 시각을 기준으로 월 키(yyyymm)를 계산한다.
        return YearMonth.from(Instant.ofEpochMilli(enqueuedAt).atZone(trafficRedisRuntimePolicy.zoneId()));
    }

    /**
     * Validates that the request payload contains all required fields for the specified pool type.
     *
     * @param poolType the pool type to validate for
     * @param payload the request payload to validate
     * @return `true` if the payload contains all required fields for the given pool type, `false` otherwise
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

        // 풀별 키 생성에 필요한 식별자가 없으면 처리할 수 없다.
        return switch (poolType) {
            case INDIVIDUAL -> true;
            case SHARED -> payload.getFamilyId() != null && payload.getFamilyId() > 0;
        };
    }

    /**
     * Create a standard ERROR TrafficLuaExecutionResult used for immediate termination scenarios (e.g., validation failure).
     *
     * @return a TrafficLuaExecutionResult with answer = -1 and status = ERROR
     */
    private TrafficLuaExecutionResult errorResult() {
        return TrafficLuaExecutionResult.builder()
                .answer(-1L)
                .status(TrafficLuaStatus.ERROR)
                .build();
    }

    /**
     * Normalize a Long to a value greater than or equal to zero.
     *
     * @param value the input value to normalize; may be null or negative
     * @return the original value when greater than zero, or 0 if the input is null or less than or equal to zero
     */
    private long normalizeNonNegative(Long value) {
        if (value == null || value <= 0) {
            return 0L;
        }
        return value;
    }

    /**
     * Normalize an Integer to an int value that is greater than or equal to zero.
     *
     * @param value the Integer to normalize; may be null
     * @return an int greater than or equal to zero — returns 0 if {@code value} is null or less than or equal to 0, otherwise returns {@code value}
     */
    private int normalizeNonNegativeInt(Integer value) {
        if (value == null || value <= 0) {
            return 0;
        }
        return value;
    }
}
