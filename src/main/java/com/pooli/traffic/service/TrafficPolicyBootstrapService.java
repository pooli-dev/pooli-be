package com.pooli.traffic.service;

import static java.util.Map.entry;

import java.time.Instant;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Profile;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import com.pooli.common.exception.ApplicationException;
import com.pooli.common.exception.CommonErrorCode;
import com.pooli.policy.domain.dto.response.PolicyActivationSnapshotResDto;
import com.pooli.policy.mapper.PolicyBackOfficeMapper;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * POLICY 전역 활성화 상태를 Redis policy:{policyId} 키에 동기화하는 bootstrap/reconciliation 서비스입니다.
 *
 * <p>동작 규칙:
 * 1) 부팅 시 POLICY 스냅샷을 읽어 필수 정책 ID(1~7) 존재를 검증(fail-fast)
 * 2) 분산락(NX PX) 획득 인스턴스만 pipeline으로 Redis 반영
 * 3) 주기적 reconciliation으로 DB->Redis 불일치를 보정
 * 4) lock 해제는 소유자 비교 Lua로 수행
 */
@Slf4j
@Service
@Profile({"local", "traffic"})
@RequiredArgsConstructor
public class TrafficPolicyBootstrapService {

    private static final long POLICY_BOOTSTRAP_LOCK_TTL_MS = 30_000L;

    private static final Map<Integer, String> REQUIRED_POLICY_MAPPING = Map.ofEntries(
            entry(1, "REPEAT_BLOCK_POLICY"),
            entry(2, "IMMEDIATELY_BLOCK_POLICY"),
            entry(3, "LINE_LIMIT_SHARED_POLICY"),
            entry(4, "LINE_LIMIT_DAILY_POLICY"),
            entry(5, "APP_POLICY_DATA_POLICY"),
            entry(6, "APP_POLICY_SPEED_POLICY"),
            entry(7, "APP_POLICY_WHITELIST_POLICY")
    );

    private static final RedisScript<Long> LOCK_RELEASE_SCRIPT = createLockReleaseScript();

    @Qualifier("cacheStringRedisTemplate")
    private final StringRedisTemplate cacheStringRedisTemplate;
    private final PolicyBackOfficeMapper policyBackOfficeMapper;
    private final TrafficRedisKeyFactory trafficRedisKeyFactory;
    private final TrafficRedisRuntimePolicy trafficRedisRuntimePolicy;

    /**
     * Perform a single policy activation bootstrap at application startup to synchronize the database snapshot of policy activation states to Redis.
     *
     * @throws ApplicationException if required policy IDs are missing in the database snapshot (fail-fast behavior)
     */
    @PostConstruct
    /**
      * 애플리케이션 부팅 시 정책 활성화 키 bootstrap을 1회 수행합니다.
     */
    public void bootstrapOnStartup() {
        synchronizePolicyActivationSnapshot("startup", true);
    }

    /**
     * Periodically reconciles policy activation keys in Redis to reflect the database state.
     *
     * Does not propagate exceptions — failures are logged and the scheduler thread continues to the next cycle.
     */
    @Scheduled(
            fixedDelayString = "${app.policy.bootstrap.reconcile-interval-ms:300000}",
            initialDelayString = "${app.policy.bootstrap.reconcile-initial-delay-ms:60000}"
    )
    /**
      * 주기적으로 정책 활성화 키를 재동기화해 DB/Redis 불일치를 보정합니다.
     */
    public void reconcilePolicyActivationSnapshot() {
        try {
            synchronizePolicyActivationSnapshot("reconcile", false);
        } catch (Exception e) {
            // reconciliation 실패가 런타임 스레드를 죽이지 않도록 로그만 남기고 다음 주기를 기다린다.
            log.error("traffic_policy_bootstrap_reconcile_failed", e);
        }
    }

    /**
     * Synchronizes policy activation snapshots from the database to Redis under a distributed lock.
     *
     * Validates required policy IDs, attempts to acquire a lock to serialize Redis writes, applies the snapshot
     * state to Redis if the lock is obtained, and always releases the lock when finished.
     *
     * @param executionType a label describing the invocation context (e.g., "startup" or "reconcile") used in logs
     * @param failFastOnMissingRequiredIds if `true`, throw an ApplicationException when required policy IDs are missing;
     *                                     if `false`, log the condition and skip synchronization
     * @throws com.pooli.common.exception.ApplicationException when required policy IDs are missing and
     *                                                         {@code failFastOnMissingRequiredIds} is {@code true}
     */
    private void synchronizePolicyActivationSnapshot(String executionType, boolean failFastOnMissingRequiredIds) {
        List<PolicyActivationSnapshotResDto> snapshots = policyBackOfficeMapper.selectPolicyActivationSnapshot();
        if (!validateRequiredPolicyIds(snapshots, failFastOnMissingRequiredIds)) {
            return;
        }

        String lockKey = trafficRedisKeyFactory.policyBootstrapLockKey();
        String lockOwner = buildLockOwner(executionType);
        boolean lockAcquired = tryAcquireLock(lockKey, lockOwner);
        if (!lockAcquired) {
            log.info(
                    "traffic_policy_bootstrap_lock_skipped executionType={} lockKey={}",
                    executionType,
                    lockKey
            );
            return;
        }

        try {
            syncSnapshotToRedis(snapshots);
            log.info(
                    "traffic_policy_bootstrap_completed executionType={} policyCount={}",
                    executionType,
                    snapshots.size()
            );
        } finally {
            releaseLock(lockKey, lockOwner);
        }
    }

    /**
     * Verify that all required policy IDs (1 through 7) are present in the provided DB snapshots.
     *
     * If any required IDs are missing and `failFastOnMissingRequiredIds` is true, an ApplicationException is thrown.
     * Otherwise the method logs an error and returns false.
     *
     * @param snapshots                       list of policy activation snapshots to inspect; null is treated as empty
     * @param failFastOnMissingRequiredIds    if true, throw ApplicationException when required IDs are missing; if false, log and return false
     * @return                                `true` if all required policy IDs are present, `false` otherwise
     * @throws ApplicationException           when required policy IDs are missing and `failFastOnMissingRequiredIds` is true
     */
    private boolean validateRequiredPolicyIds(
            List<PolicyActivationSnapshotResDto> snapshots,
            boolean failFastOnMissingRequiredIds
    ) {
        Set<Integer> existingPolicyIds = snapshots == null
                ? Set.of()
                : snapshots.stream()
                        .map(PolicyActivationSnapshotResDto::getPolicyId)
                        .filter(id -> id != null && id > 0)
                        .collect(Collectors.toSet());

        Set<Integer> missingPolicyIds = new HashSet<>(REQUIRED_POLICY_MAPPING.keySet());
        missingPolicyIds.removeAll(existingPolicyIds);
        if (missingPolicyIds.isEmpty()) {
            return true;
        }

        String missingPolicyDescription = missingPolicyIds.stream()
                .sorted()
                .map(policyId -> policyId + ":" + REQUIRED_POLICY_MAPPING.get(policyId))
                .collect(Collectors.joining(", "));
        String message = "필수 POLICY ID가 누락되어 Redis bootstrap을 진행할 수 없습니다. missing=[" + missingPolicyDescription + "]";
        if (failFastOnMissingRequiredIds) {
            throw new ApplicationException(CommonErrorCode.INTERNAL_SERVER_ERROR, message);
        }
        log.error("traffic_policy_bootstrap_validation_failed message={}", message);
        return false;
    }

    /**
     * Apply database policy activation snapshots to Redis in a single pipeline operation.
     *
     * For each snapshot, sets the corresponding policy key to "1" when active or removes the key when inactive,
     * then updates the bootstrap version key to the computed epoch-second version.
     *
     * @param snapshots list of policy activation snapshots; entries that are null or have a null policyId are ignored
     */
    private void syncSnapshotToRedis(List<PolicyActivationSnapshotResDto> snapshots) {
        long bootstrapVersionEpochSeconds = resolveBootstrapVersionEpochSeconds(snapshots);
        String versionKey = trafficRedisKeyFactory.policyBootstrapVersionKey();

        cacheStringRedisTemplate.executePipelined(new SessionCallback<>() {
            /**
             * Applies the provided policy activation snapshots to Redis and updates the bootstrap version key.
             *
             * For each non-null snapshot with a non-null policyId, sets the corresponding policy key to `"1"`
             * when the snapshot is active, or deletes the policy key when it is not active. After processing
             * all snapshots, writes the computed bootstrap version epoch seconds to the specified version key.
             *
             * @param operations the Redis operations context used to perform value set/delete operations
             * @return null
             */
            @Override
            @SuppressWarnings("unchecked")
            public Object execute(RedisOperations operations) {
                RedisOperations<String, String> stringOperations = (RedisOperations<String, String>) operations;
                ValueOperations<String, String> valueOperations = stringOperations.opsForValue();

                for (PolicyActivationSnapshotResDto snapshot : snapshots) {
                    if (snapshot == null || snapshot.getPolicyId() == null) {
                        continue;
                    }
                    long policyId = snapshot.getPolicyId();
                    String policyKey = trafficRedisKeyFactory.policyKey(policyId);

                    if (Boolean.TRUE.equals(snapshot.getIsActive())) {
                        valueOperations.set(policyKey, "1");
                    } else {
                        stringOperations.delete(policyKey);
                    }
                }

                valueOperations.set(versionKey, String.valueOf(bootstrapVersionEpochSeconds));
                return null;
            }
        });
    }

    /**
     * Compute the bootstrap version as the most recent snapshot timestamp expressed in epoch seconds using the runtime zone.
     *
     * For each snapshot, the latest available timestamp is taken (prefer `updatedAt` over `createdAt`); if no valid timestamps exist, the current instant's epoch seconds are returned.
     *
     * @param snapshots the list of policy activation snapshots to inspect
     * @return the epoch-second value of the most recent timestamp across the provided snapshots in the configured zone, or the current epoch seconds if none exist
     */
    private long resolveBootstrapVersionEpochSeconds(List<PolicyActivationSnapshotResDto> snapshots) {
        ZoneId zoneId = trafficRedisRuntimePolicy.zoneId();
        return snapshots.stream()
                .map(this::resolveLatestTimestamp)
                .filter(timestamp -> timestamp != null)
                .mapToLong(timestamp -> timestamp.atZone(zoneId).toEpochSecond())
                .max()
                .orElseGet(() -> Instant.now().getEpochSecond());
    }

    /**
     * Return the latest timestamp from the given snapshot, preferring `updatedAt` when present.
     *
     * @param snapshot the policy activation snapshot to inspect
     * @return the `updatedAt` value if non-null, otherwise the `createdAt` value; returns `null` if `snapshot` is null or neither timestamp is present
     */
    private LocalDateTime resolveLatestTimestamp(PolicyActivationSnapshotResDto snapshot) {
        if (snapshot == null) {
            return null;
        }
        if (snapshot.getUpdatedAt() != null) {
            return snapshot.getUpdatedAt();
        }
        return snapshot.getCreatedAt();
    }

    /**
     * Attempts to acquire a distributed bootstrap lock in Redis.
     *
     * @param lockKey the Redis key used for the lock
     * @param lockOwner unique identifier for the lock owner
     * @return true if the lock was acquired, false otherwise
     */
    private boolean tryAcquireLock(String lockKey, String lockOwner) {
        Boolean acquired = cacheStringRedisTemplate.opsForValue().setIfAbsent(
                lockKey,
                lockOwner,
                Duration.ofMillis(POLICY_BOOTSTRAP_LOCK_TTL_MS)
        );
        return Boolean.TRUE.equals(acquired);
    }

    /**
     * Create a unique lock owner token for distributed lock operations.
     *
     * @param executionType a label identifying the execution context (e.g., "startup" or "reconcile") used as the token prefix
     * @return a string composed of the executionType, a colon, and a random UUID identifying the lock owner
     */
    private String buildLockOwner(String executionType) {
        return executionType + ":" + UUID.randomUUID();
    }

    /**
     * Releases the distributed bootstrap lock if the stored owner matches the provided owner.
     *
     * @param lockKey   the Redis key of the lock to release
     * @param lockOwner the expected owner value; the lock is deleted only when this matches the key's current value
     */
    private void releaseLock(String lockKey, String lockOwner) {
        try {
            cacheStringRedisTemplate.execute(LOCK_RELEASE_SCRIPT, List.of(lockKey), lockOwner);
        } catch (Exception e) {
            log.warn("traffic_policy_bootstrap_lock_release_failed lockKey={}", lockKey, e);
        }
    }

    /**
     * Create a Redis script that releases a lock only when the caller owns it.
     *
     * The returned script atomically checks that the key's value equals the provided owner argument
     * and deletes the key only if the check passes.
     *
     * @return the `RedisScript<Long>` which returns `1` if the key was deleted, `0` otherwise
     */
    private static RedisScript<Long> createLockReleaseScript() {
        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        script.setScriptText("""
                if redis.call('GET', KEYS[1]) == ARGV[1] then
                  return redis.call('DEL', KEYS[1])
                end
                return 0
                """);
        script.setResultType(Long.class);
        return script;
    }
}
