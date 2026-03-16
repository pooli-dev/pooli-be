package com.pooli.traffic.service.policy;

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
import com.pooli.traffic.service.runtime.TrafficRedisRuntimePolicy;
import com.pooli.traffic.service.runtime.TrafficLuaScriptInfraService;
import com.pooli.traffic.service.runtime.TrafficRedisKeyFactory;
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
     * 대상 lineId의 정책 스냅샷이 Redis에 존재하도록 보장합니다.
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
     * DB 스냅샷을 읽어 회선 정책 키를 Redis에 반영합니다.
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

        long version = System.currentTimeMillis();
        trafficPolicyWriteThroughService.syncLineLimitUntracked(
                lineId,
                dailyLimit,
                isDailyActive,
                sharedLimit,
                isSharedActive,
                version
        );
        trafficPolicyWriteThroughService.syncImmediateBlockEndUntracked(lineId, blockEndAt, version);
        trafficPolicyWriteThroughService.syncRepeatBlockUntracked(lineId, repeatBlocks, version);
        trafficPolicyWriteThroughService.syncAppPolicySnapshotUntracked(lineId, appPolicies, version);

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
     * hydrate 선행 수행 인스턴스를 하나로 제한하기 위해 분산락 획득을 시도합니다.
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
     * lock 미획득 인스턴스에서 짧게 ready 키를 재확인합니다.
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
     * ready 키 존재 여부를 확인합니다.
     */
    private boolean isReady(String readyKey) {
        return Boolean.TRUE.equals(cacheStringRedisTemplate.hasKey(readyKey));
    }

    /**
     * 소유자 검증 기반으로 분산락을 해제합니다.
     */
    private void releaseLock(String lockKey, String lockOwner) {
        try {
            trafficLuaScriptInfraService.executeLockRelease(lockKey, lockOwner);
        } catch (RuntimeException e) {
            log.warn("traffic_line_policy_hydrate_lock_release_failed lineKey={}", lockKey, e);
        }
    }

    /**
     * 짧은 대기로 busy polling을 완화합니다.
     */
    private void sleepBriefly() {
        try {
            Thread.sleep(READY_RECHECK_SLEEP_MS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
