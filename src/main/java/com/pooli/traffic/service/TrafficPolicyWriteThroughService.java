package com.pooli.traffic.service;

import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Profile;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.SetOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import com.pooli.common.exception.ApplicationException;
import com.pooli.common.exception.CommonErrorCode;
import com.pooli.policy.domain.dto.response.RepeatBlockDayResDto;
import com.pooli.policy.domain.dto.response.RepeatBlockPolicyResDto;
import com.pooli.policy.domain.entity.AppPolicy;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * 정책 변경 시 Redis 정책 키를 즉시 동기화(write-through)하는 서비스입니다.
 * 트랜잭션이 존재하면 커밋 후 반영하고, 실패 시 재시도 후 예외를 발생시켜 정합성을 지킵니다.
 */
@Slf4j
@Service
@Profile({"local", "traffic"})
@RequiredArgsConstructor
public class TrafficPolicyWriteThroughService {

    private static final int WRITE_THROUGH_RETRY_MAX = 3;
    private static final long WRITE_THROUGH_RETRY_BACKOFF_MS = 50L;

    @Qualifier("cacheStringRedisTemplate")
    private final StringRedisTemplate cacheStringRedisTemplate;
    private final TrafficRedisKeyFactory trafficRedisKeyFactory;
    private final TrafficRedisRuntimePolicy trafficRedisRuntimePolicy;

    /**
     * Synchronizes a policy's activation state to Redis.
     *
     * When `isActive` is true, the policy key is set to "1"; when `isActive` is false, the key is deleted.
     * The Redis write is scheduled to run after the current transaction commits if a transaction exists,
     * otherwise it executes immediately and will be retried on transient failures according to the service's retry policy.
     *
     * @param policyId the identifier of the policy to synchronize
     * @param isActive true to mark the policy active (set key to "1"), false to mark it inactive (delete key)
     */
    public void syncPolicyActivation(long policyId, boolean isActive) {
        executeAfterCommit(
                "policy_activation_sync policyId=" + policyId + " isActive=" + isActive,
                () -> {
                    String key = trafficRedisKeyFactory.policyKey(policyId);
                    if (isActive) {
                        cacheStringRedisTemplate.opsForValue().set(key, "1");
                    } else {
                        cacheStringRedisTemplate.delete(key);
                    }
                }
        );
    }

    /**
     * Synchronizes line-level daily and monthly shared limits to Redis.
     *
     * If a limit is not active, stores -1 to indicate "unlimited" so downstream Redis/Lua logic does not enforce a limit.
     *
     * @param lineId        identifier of the line whose limits are being synchronized
     * @param dailyLimit    configured daily limit; may be null
     * @param isDailyActive true if the daily limit should be enforced, false or null to mark as unlimited
     * @param sharedLimit   configured monthly shared limit; may be null
     * @param isSharedActive true if the monthly shared limit should be enforced, false or null to mark as unlimited
     */
    public void syncLineLimit(
            long lineId,
            Long dailyLimit,
            Boolean isDailyActive,
            Long sharedLimit,
            Boolean isSharedActive
    ) {
        executeAfterCommit(
                "line_limit_sync lineId=" + lineId,
                () -> {
                    String dailyLimitKey = trafficRedisKeyFactory.dailyTotalLimitKey(lineId);
                    String monthlySharedLimitKey = trafficRedisKeyFactory.monthlySharedLimitKey(lineId);

                    long dailyLimitValue = resolveLimitValue(dailyLimit, isDailyActive);
                    long sharedLimitValue = resolveLimitValue(sharedLimit, isSharedActive);

                    cacheStringRedisTemplate.opsForValue().set(dailyLimitKey, String.valueOf(dailyLimitValue));
                    cacheStringRedisTemplate.opsForValue().set(monthlySharedLimitKey, String.valueOf(sharedLimitValue));
                }
        );
    }

    /**
     * Synchronizes the immediate block end time for a line to Redis.
     *
     * @param lineId     the identifier of the line whose block end time is being synchronized
     * @param blockEndAt the block end time to store; if `null`, the Redis key is deleted;
     *                   otherwise the time is stored as epoch seconds in the Asia/Seoul time zone
     */
    public void syncImmediateBlockEnd(long lineId, LocalDateTime blockEndAt) {
        executeAfterCommit(
                "immediate_block_sync lineId=" + lineId,
                () -> {
                    String key = trafficRedisKeyFactory.immediatelyBlockEndKey(lineId);
                    if (blockEndAt == null) {
                        cacheStringRedisTemplate.delete(key);
                        return;
                    }

                    long epochSecond = blockEndAt.atZone(trafficRedisRuntimePolicy.zoneId()).toEpochSecond();
                    cacheStringRedisTemplate.opsForValue().set(key, String.valueOf(epochSecond));
                }
        );
    }

    /**
     * Synchronizes repeat-block policies for a line into the repeat_block Redis hash as a snapshot.
     *
     * Deletes the existing hash and writes the provided repeat-block entries so that removals or
     * deactivations are reflected immediately.
     *
     * @param lineId       identifier of the line whose repeat-block policies are being synchronized
     * @param repeatBlocks list of repeat-block policy DTOs to persist as the snapshot; if null or empty,
     *                     the existing repeat_block hash will be cleared
     */
    public void syncRepeatBlock(long lineId, List<RepeatBlockPolicyResDto> repeatBlocks) {
        executeAfterCommit(
                "repeat_block_sync lineId=" + lineId,
                () -> {
                    String repeatBlockKey = trafficRedisKeyFactory.repeatBlockKey(lineId);

                    // 기존 hash를 먼저 비워 soft-delete/비활성화 변경이 즉시 반영되도록 한다.
                    cacheStringRedisTemplate.delete(repeatBlockKey);

                    Map<String, String> hashToWrite = buildRepeatBlockHash(repeatBlocks);
                    if (!hashToWrite.isEmpty()) {
                        HashOperations<String, String, String> hashOps = cacheStringRedisTemplate.opsForHash();
                        hashOps.putAll(repeatBlockKey, hashToWrite);
                    }
                }
        );
    }

    /**
     * Synchronizes an app's data daily limit, speed limit, and whitelist membership to Redis for a given line.
     *
     * If `isActive` is false, removes all stored entries for the app so no limits or whitelist membership remain.
     * If `isActive` is true, updates the data and speed limit fields (using `-1` to represent an unset limit) and
     * updates membership in the whitelist set according to `isWhitelist`.
     *
     * @param lineId      the identifier of the line whose Redis keys will be updated
     * @param appId       the application identifier used to build hash fields and whitelist member
     * @param isActive    when false, all Redis entries for this app are removed; when true, entries are created/updated
     * @param dataLimit   the daily data limit to store; treated as "unset" (`-1`) if null
     * @param speedLimit  the speed limit to store; treated as "unset" (`-1`) if null
     * @param isWhitelist whether the app should be present in the line's whitelist set
     */
    public void syncAppPolicy(
            long lineId,
            int appId,
            boolean isActive,
            Long dataLimit,
            Integer speedLimit,
            boolean isWhitelist
    ) {
        executeAfterCommit(
                "app_policy_sync lineId=" + lineId + " appId=" + appId + " isActive=" + isActive,
                () -> {
                    String appDataDailyLimitKey = trafficRedisKeyFactory.appDataDailyLimitKey(lineId);
                    String appSpeedLimitKey = trafficRedisKeyFactory.appSpeedLimitKey(lineId);
                    String appWhitelistKey = trafficRedisKeyFactory.appWhitelistKey(lineId);

                    String dataField = appDataLimitField(appId);
                    String speedField = appSpeedLimitField(appId);
                    String appMember = String.valueOf(appId);

                    if (!isActive) {
                        // 비활성 정책은 즉시 제거해 차감 Lua가 제한을 보지 않도록 한다.
                        cacheStringRedisTemplate.opsForHash().delete(appDataDailyLimitKey, dataField);
                        cacheStringRedisTemplate.opsForHash().delete(appSpeedLimitKey, speedField);
                        cacheStringRedisTemplate.opsForSet().remove(appWhitelistKey, appMember);
                        return;
                    }

                    long normalizedDataLimit = dataLimit == null ? -1L : dataLimit;
                    int normalizedSpeedLimit = speedLimit == null ? -1 : speedLimit;

                    cacheStringRedisTemplate.opsForHash().put(
                            appDataDailyLimitKey,
                            dataField,
                            String.valueOf(normalizedDataLimit)
                    );
                    cacheStringRedisTemplate.opsForHash().put(
                            appSpeedLimitKey,
                            speedField,
                            String.valueOf(normalizedSpeedLimit)
                    );

                    SetOperations<String, String> setOps = cacheStringRedisTemplate.opsForSet();
                    if (isWhitelist) {
                        setOps.add(appWhitelistKey, appMember);
                    } else {
                        setOps.remove(appWhitelistKey, appMember);
                    }
                }
        );
    }

    /**
     * Remove all Redis entries related to an app's policy for the given line.
     *
     * Removes the app's field from the line's data-daily-limit and speed-limit hashes and removes the app from the line's whitelist set.
     *
     * @param lineId the identifier of the line whose app-related Redis entries should be removed
     * @param appId  the application identifier whose Redis entries should be evicted
     */
    public void evictAppPolicy(long lineId, int appId) {
        executeAfterCommit(
                "app_policy_evict lineId=" + lineId + " appId=" + appId,
                () -> {
                    String appDataDailyLimitKey = trafficRedisKeyFactory.appDataDailyLimitKey(lineId);
                    String appSpeedLimitKey = trafficRedisKeyFactory.appSpeedLimitKey(lineId);
                    String appWhitelistKey = trafficRedisKeyFactory.appWhitelistKey(lineId);
                    String appMember = String.valueOf(appId);

                    cacheStringRedisTemplate.opsForHash().delete(appDataDailyLimitKey, appDataLimitField(appId));
                    cacheStringRedisTemplate.opsForHash().delete(appSpeedLimitKey, appSpeedLimitField(appId));
                    cacheStringRedisTemplate.opsForSet().remove(appWhitelistKey, appMember);
                }
        );
    }

    /**
     * Applies a complete snapshot of app policies for a given line to Redis by clearing related keys and reloading active policies.
     *
     * <p>Deletes the three per-line app policy keys (data daily limit hash, speed limit hash, whitelist set) and then writes back entries only for policies that are active and have a valid application id.</p>
     *
     * @param lineId the identifier of the line whose app policy snapshot will be applied
     * @param appPolicies the list of app policies to load; null or empty list results in cleared keys with no further writes
     */
    public void syncAppPolicySnapshot(long lineId, List<AppPolicy> appPolicies) {
        executeAfterCommit(
                "app_policy_snapshot_sync lineId=" + lineId,
                () -> {
                    String appDataDailyLimitKey = trafficRedisKeyFactory.appDataDailyLimitKey(lineId);
                    String appSpeedLimitKey = trafficRedisKeyFactory.appSpeedLimitKey(lineId);
                    String appWhitelistKey = trafficRedisKeyFactory.appWhitelistKey(lineId);

                    cacheStringRedisTemplate.delete(List.of(
                            appDataDailyLimitKey,
                            appSpeedLimitKey,
                            appWhitelistKey
                    ));

                    if (appPolicies == null || appPolicies.isEmpty()) {
                        return;
                    }

                    Map<String, String> dataLimitHash = new HashMap<>();
                    Map<String, String> speedLimitHash = new HashMap<>();
                    Set<String> whitelistMembers = new HashSet<>();

                    for (AppPolicy appPolicy : appPolicies) {
                        if (appPolicy == null || appPolicy.getApplicationId() == null) {
                            continue;
                        }
                        if (!Boolean.TRUE.equals(appPolicy.getIsActive())) {
                            continue;
                        }

                        int appId = appPolicy.getApplicationId();
                        long normalizedDataLimit = appPolicy.getDataLimit() == null ? -1L : appPolicy.getDataLimit();
                        int normalizedSpeedLimit = appPolicy.getSpeedLimit() == null ? -1 : appPolicy.getSpeedLimit();

                        dataLimitHash.put(appDataLimitField(appId), String.valueOf(normalizedDataLimit));
                        speedLimitHash.put(appSpeedLimitField(appId), String.valueOf(normalizedSpeedLimit));

                        if (Boolean.TRUE.equals(appPolicy.getIsWhitelist())) {
                            whitelistMembers.add(String.valueOf(appId));
                        }
                    }

                    HashOperations<String, String, String> hashOps = cacheStringRedisTemplate.opsForHash();
                    if (!dataLimitHash.isEmpty()) {
                        hashOps.putAll(appDataDailyLimitKey, dataLimitHash);
                    }
                    if (!speedLimitHash.isEmpty()) {
                        hashOps.putAll(appSpeedLimitKey, speedLimitHash);
                    }
                    if (!whitelistMembers.isEmpty()) {
                        cacheStringRedisTemplate.opsForSet().add(
                                appWhitelistKey,
                                whitelistMembers.toArray(new String[0])
                        );
                    }
                }
        );
    }

    /**
     * Ensures the given Redis write operation is executed only after a successful database commit when a transaction is active; otherwise executes it immediately.
     *
     * @param operationName        a descriptive name for the operation used for logging and tracing
     * @param redisWriteOperation  the Redis write logic to be executed (will be run after commit if inside a transaction)
     */
    private void executeAfterCommit(String operationName, Runnable redisWriteOperation) {
        Runnable wrappedOperation = () -> executeWithRetry(operationName, redisWriteOperation);

        // 트랜잭션 내에서는 DB 커밋 성공 이후에만 Redis 반영을 수행한다.
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                /**
                 * Executes the wrapped operation after the surrounding transaction has successfully committed.
                 *
                 * <p>This method is invoked by Spring's transaction synchronization to perform the
                 * write-through Redis update once the database transaction is committed.</p>
                 */
                @Override
                /**
                  * `afterCommit` 처리 목적에 맞는 핵심 로직을 수행합니다.
                 */
                public void afterCommit() {
                    wrappedOperation.run();
                }
            });
            return;
        }

        wrappedOperation.run();
    }

    /**
     * Executes a Redis write operation with retries and backoff, and fails the surrounding transaction if retries are exhausted.
     *
     * @param operationName a descriptive name used for logging and tracing the operation
     * @param redisWriteOperation the Redis write action to execute
     * @throws ApplicationException with CommonErrorCode.EXTERNAL_SYSTEM_ERROR when all retry attempts fail
     */
    private void executeWithRetry(String operationName, Runnable redisWriteOperation) {
        RuntimeException lastException = null;
        for (int attempt = 1; attempt <= WRITE_THROUGH_RETRY_MAX; attempt++) {
            try {
                redisWriteOperation.run();
                return;
            } catch (RuntimeException e) {
                lastException = e;
                if (attempt >= WRITE_THROUGH_RETRY_MAX) {
                    log.error(
                            "traffic_policy_write_through_failed operation={} attempts={}",
                            operationName,
                            attempt,
                            e
                    );
                    throw new ApplicationException(CommonErrorCode.EXTERNAL_SYSTEM_ERROR, "정책 Redis 즉시 갱신에 실패했습니다.");
                }

                log.warn(
                        "traffic_policy_write_through_retry operation={} attempt={}/{}",
                        operationName,
                        attempt,
                        WRITE_THROUGH_RETRY_MAX
                );
                sleepBackoff();
            }
        }

        throw lastException == null
                ? new ApplicationException(CommonErrorCode.EXTERNAL_SYSTEM_ERROR, "정책 Redis 즉시 갱신에 실패했습니다.")
                : lastException;
    }

    /**
     * Sleeps for a short backoff period used between retry attempts.
     *
     * If the sleep is interrupted, restores the thread's interrupt status and throws an
     * ApplicationException with CommonErrorCode.EXTERNAL_SYSTEM_ERROR.
     *
     * @throws ApplicationException when the sleep is interrupted
     */
    private void sleepBackoff() {
        try {
            Thread.sleep(WRITE_THROUGH_RETRY_BACKOFF_MS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ApplicationException(CommonErrorCode.EXTERNAL_SYSTEM_ERROR, "정책 Redis 재시도 대기 중 인터럽트가 발생했습니다.");
        }
    }

    /**
     * Builds a Redis hash representation of active repeat block policies.
     *
     * <p>Each map entry's key is formatted as {@code day:{dayNum}:{repeatBlockId}} and the value as
     * {@code {startSec}:{endSec}}, where {@code dayNum} is the day's ordinal (0–6) and {@code startSec}
     * / {@code endSec} are seconds from midnight.</p>
     *
     * @param repeatBlocks list of repeat block DTOs to convert; null, inactive items, or entries with
     *                     missing required fields are ignored
     * @return a map ready to be written to a Redis hash, mapping field names to their corresponding
     *         start/end second ranges; empty if there are no valid entries
     */
    private Map<String, String> buildRepeatBlockHash(List<RepeatBlockPolicyResDto> repeatBlocks) {
        Map<String, String> hashToWrite = new HashMap<>();
        if (repeatBlocks == null || repeatBlocks.isEmpty()) {
            return hashToWrite;
        }

        for (RepeatBlockPolicyResDto repeatBlock : repeatBlocks) {
            if (repeatBlock == null || !Boolean.TRUE.equals(repeatBlock.getIsActive())) {
                continue;
            }
            if (repeatBlock.getRepeatBlockId() == null || repeatBlock.getDays() == null) {
                continue;
            }

            for (RepeatBlockDayResDto day : repeatBlock.getDays()) {
                if (day == null || day.getDayOfWeek() == null || day.getStartAt() == null || day.getEndAt() == null) {
                    continue;
                }

                int dayNum = day.getDayOfWeek().ordinal();
                int startAtSec = day.getStartAt().toSecondOfDay();
                int endAtSec = day.getEndAt().toSecondOfDay();

                String field = "day:" + dayNum + ":" + repeatBlock.getRepeatBlockId();
                String value = startAtSec + ":" + endAtSec;
                hashToWrite.put(field, value);
            }
        }

        return hashToWrite;
    }

    /**
     * Determine the final stored numeric limit for a policy based on its active flag.
     *
     * @param limit    the configured limit value, or `null` if unspecified
     * @param isActive `true` if the policy is active; otherwise not active
     * @return         `-1` if the policy is not active or `limit` is `null`, otherwise the provided `limit`
     */
    private long resolveLimitValue(Long limit, Boolean isActive) {
        if (!Boolean.TRUE.equals(isActive)) {
            return -1L;
        }
        return limit == null ? -1L : limit;
    }

    /**
     * Constructs the hash field name for an app's daily data limit.
     *
     * @param appId the application identifier
     * @return the hash field name formatted as "limit:{appId}"
     */
    private String appDataLimitField(int appId) {
        return "limit:" + appId;
    }

    /**
     * Builds the Redis hash field name for an app's speed limit.
     *
     * @param appId the application identifier
     * @return the field name formatted as "speed:{appId}"
     */
    private String appSpeedLimitField(int appId) {
        return "speed:" + appId;
    }
}
