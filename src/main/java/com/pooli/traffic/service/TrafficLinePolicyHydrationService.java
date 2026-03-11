package com.pooli.traffic.service;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.pooli.policy.domain.dto.response.ImmediateBlockResDto;
import com.pooli.policy.domain.dto.response.RepeatBlockPolicyResDto;
import com.pooli.policy.domain.entity.AppPolicy;
import com.pooli.policy.domain.entity.LineLimit;
import com.pooli.policy.mapper.AppPolicyMapper;
import com.pooli.policy.mapper.ImmediateBlockMapper;
import com.pooli.policy.mapper.LineLimitMapper;
import com.pooli.policy.mapper.RepeatBlockMapper;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * 트래픽 차감 직전에 회선 정책 스냅샷을 Redis로 보장하는 on-demand hydration 서비스입니다.
 * 부팅 preload 없이 line 단위로 필요 시 로드하며, 다중 서버 환경에서 분산락으로 중복 작업을 줄입니다.
 */
@Slf4j
@Service
@Profile({"local", "traffic"})
@RequiredArgsConstructor
public class TrafficLinePolicyHydrationService {

    private static final int READY_RECHECK_MAX = 3;
    private static final long READY_RECHECK_SLEEP_MS = 30L;

    @Value("${app.policy.line-hydration.ready-ttl-sec:60}")
    private long linePolicyReadyTtlSeconds = 60L;

    @Qualifier("cacheStringRedisTemplate")
    private final StringRedisTemplate cacheStringRedisTemplate;
    private final TrafficRedisKeyFactory trafficRedisKeyFactory;
    private final TrafficPolicyWriteThroughService trafficPolicyWriteThroughService;
    private final LineLimitMapper lineLimitMapper;
    private final ImmediateBlockMapper immediateBlockMapper;
    private final RepeatBlockMapper repeatBlockMapper;
    private final AppPolicyMapper appPolicyMapper;
    private final TrafficLuaScriptInfraService trafficLuaScriptInfraService;

    /**
     * Ensures a line's policy snapshot exists in Redis so the line is ready for use.
     *
     * If the snapshot is absent, this method attempts to acquire a distributed lock to perform hydration;
     * if the lock is held by another process it will wait briefly for readiness and may fall back to
     * a single self-hydration attempt if the lock is not obtained.
     *
     * @param lineId the line identifier; must be greater than zero
     * @throws IllegalArgumentException if {@code lineId} is less than or equal to zero
     */
    public void ensureLoaded(long lineId) {
        if (lineId <= 0) {
            throw new IllegalArgumentException("lineId must be positive");
        }

        String readyKey = trafficRedisKeyFactory.linePolicyReadyKey(lineId);
        if (isReady(readyKey)) {
            return;
        }

        String lockKey = trafficRedisKeyFactory.linePolicyHydrateLockKey(lineId);
        String lockOwner = "line-policy:" + UUID.randomUUID();
        boolean lockAcquired = tryAcquireLock(lockKey, lockOwner);
        if (!lockAcquired) {
            if (waitUntilReady(readyKey)) {
                return;
            }

            // 락을 얻지 못했고 ready도 생성되지 않았다면 self-hydrate 1회로 복구를 시도한다.
            log.info(
                    "traffic_line_policy_hydrate_lock_not_acquired_self_hydrate lineId={} lockKey={}",
                    lineId,
                    lockKey
            );
            hydrateSnapshot(lineId, readyKey);
            return;
        }

        try {
            hydrateSnapshot(lineId, readyKey);
        } finally {
            releaseLock(lockKey, lockOwner);
        }
    }

    /**
     * Load a line's policy snapshot from the database and publish it to the policy store and Redis readiness key.
     *
     * Reads line limit, immediate block, repeat block, and app policy data for the given lineId, writes those
     * snapshots to the target policy store via the write-through service, and sets the provided Redis readyKey
     * with a TTL to indicate the snapshot is ready.
     *
     * @param lineId  the identifier of the line whose policy snapshot will be hydrated
     * @param readyKey the Redis key used to mark the line's policy snapshot as ready (set with a configured TTL)
     */
    private void hydrateSnapshot(long lineId, String readyKey) {
        long startedNano = System.nanoTime();

        Optional<LineLimit> lineLimitOptional = lineLimitMapper.getExistLineLimitByLineId(lineId);
        LineLimit lineLimit = lineLimitOptional.orElse(null);
        long dailyLimit = lineLimit == null || lineLimit.getDailyDataLimit() == null
                ? -1L
                : lineLimit.getDailyDataLimit();
        boolean isDailyActive = lineLimit != null && Boolean.TRUE.equals(lineLimit.getIsDailyLimitActive());
        long sharedLimit = lineLimit == null || lineLimit.getSharedDataLimit() == null
                ? -1L
                : lineLimit.getSharedDataLimit();
        boolean isSharedActive = lineLimit != null && Boolean.TRUE.equals(lineLimit.getIsSharedLimitActive());

        ImmediateBlockResDto immediateBlock = immediateBlockMapper.selectImmediateBlockPolicy(lineId);
        LocalDateTime blockEndAt = immediateBlock == null ? null : immediateBlock.getBlockEndAt();

        List<RepeatBlockPolicyResDto> repeatBlocks = repeatBlockMapper.selectRepeatBlocksByLineId(lineId);
        List<AppPolicy> appPolicies = appPolicyMapper.findAllEntityByLineId(lineId);

        trafficPolicyWriteThroughService.syncLineLimit(
                lineId,
                dailyLimit,
                isDailyActive,
                sharedLimit,
                isSharedActive
        );
        trafficPolicyWriteThroughService.syncImmediateBlockEnd(lineId, blockEndAt);
        trafficPolicyWriteThroughService.syncRepeatBlock(lineId, repeatBlocks);
        trafficPolicyWriteThroughService.syncAppPolicySnapshot(lineId, appPolicies);

        long readyTtlSeconds = Math.max(1L, linePolicyReadyTtlSeconds);
        cacheStringRedisTemplate.opsForValue().set(readyKey, "1", Duration.ofSeconds(readyTtlSeconds));

        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedNano);
        log.info(
                "traffic_line_policy_hydrate_success lineId={} lineLimitExists={} repeatCount={} appCount={} elapsedMs={}",
                lineId,
                lineLimit != null,
                repeatBlocks == null ? 0 : repeatBlocks.size(),
                appPolicies == null ? 0 : appPolicies.size(),
                elapsedMs
        );
    }

    /**
     * Attempts to acquire a distributed lock that designates a single instance to perform hydration.
     *
     * @param lockKey   Redis key used for the distributed lock
     * @param lockOwner Unique identifier for the lock owner
     * @return `true` if the lock was acquired, `false` otherwise
     */
    private boolean tryAcquireLock(String lockKey, String lockOwner) {
        Boolean acquired = cacheStringRedisTemplate.opsForValue().setIfAbsent(
                lockKey,
                lockOwner,
                Duration.ofMillis(TrafficRedisRuntimePolicy.LOCK_TTL_MS)
        );
        return Boolean.TRUE.equals(acquired);
    }

    /**
     * Waits briefly, rechecking the provided readiness key up to a small number of times.
     *
     * Repeatedly checks whether the given ready key exists, sleeping between attempts.
     *
     * @param readyKey the Redis key that indicates readiness
     * @return `true` if the ready key is present during the retries or on the final check, `false` otherwise
     */
    private boolean waitUntilReady(String readyKey) {
        for (int attempt = 0; attempt < READY_RECHECK_MAX; attempt++) {
            if (isReady(readyKey)) {
                return true;
            }
            sleepBriefly();
        }
        return isReady(readyKey);
    }

    /**
     * Check whether the readiness key exists in Redis.
     *
     * @param readyKey the Redis key that represents readiness
     * @return `true` if the key exists in Redis, `false` otherwise
     */
    private boolean isReady(String readyKey) {
        return Boolean.TRUE.equals(cacheStringRedisTemplate.hasKey(readyKey));
    }

    /**
     * Releases a distributed lock identified by the given key only if the lock is currently owned by the provided owner.
     *
     * @param lockKey   the Redis key representing the distributed lock
     * @param lockOwner the owner identifier used to validate ownership before releasing the lock
     * @implNote Logs a warning if the lock release operation fails. 
     */
    private void releaseLock(String lockKey, String lockOwner) {
        try {
            trafficLuaScriptInfraService.executeLockRelease(lockKey, lockOwner);
        } catch (RuntimeException e) {
            log.warn("traffic_line_policy_hydrate_lock_release_failed lineKey={}", lockKey, e);
        }
    }

    /**
     * Pauses the current thread briefly to reduce busy polling.
     *
     * Sleeps for READY_RECHECK_SLEEP_MS milliseconds; if interrupted, restores the thread's interrupted status.
     */
    private void sleepBriefly() {
        try {
            Thread.sleep(READY_RECHECK_SLEEP_MS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
