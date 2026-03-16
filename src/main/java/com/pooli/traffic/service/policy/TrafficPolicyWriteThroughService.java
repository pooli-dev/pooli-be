package com.pooli.traffic.service.policy;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import com.pooli.policy.domain.dto.response.RepeatBlockDayResDto;
import com.pooli.policy.domain.dto.response.RepeatBlockPolicyResDto;
import com.pooli.policy.domain.entity.AppPolicy;
import com.pooli.traffic.domain.outbox.OutboxEventType;
import com.pooli.traffic.domain.outbox.payload.AppPolicyOutboxPayload;
import com.pooli.traffic.domain.outbox.payload.ImmediateBlockOutboxPayload;
import com.pooli.traffic.domain.outbox.payload.LineLimitOutboxPayload;
import com.pooli.traffic.domain.outbox.payload.LineScopedOutboxPayload;
import com.pooli.traffic.domain.outbox.payload.PolicyActivationOutboxPayload;
import com.pooli.traffic.service.outbox.PolicySyncResult;
import com.pooli.traffic.service.outbox.RedisOutboxRecordService;
import com.pooli.traffic.service.outbox.TrafficPolicyVersionedRedisService;
import com.pooli.traffic.service.runtime.TrafficRedisKeyFactory;
import com.pooli.traffic.service.runtime.TrafficRedisRuntimePolicy;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * 정책 변경 시 Redis 정책 키를 즉시 동기화(write-through)하는 서비스입니다.
 * 방안 B 규칙에 맞춰 모든 정책 키를 version CAS로 반영합니다.
 */
@Slf4j
@Service
@Profile({"local", "api", "traffic"})
@RequiredArgsConstructor
public class TrafficPolicyWriteThroughService {

    private static final int WRITE_THROUGH_RETRY_MAX = 3;
    private static final long WRITE_THROUGH_RETRY_BACKOFF_MS = 50L;
    private static final int APP_SPEED_LIMIT_UPLOAD_MULTIPLIER = 125;

    private final TrafficRedisKeyFactory trafficRedisKeyFactory;
    private final TrafficRedisRuntimePolicy trafficRedisRuntimePolicy;
    private final TrafficPolicyVersionedRedisService trafficPolicyVersionedRedisService;
    private final RedisOutboxRecordService redisOutboxRecordService;

    /**
     * 정책 활성화 상태를 Redis policy 키에 동기화합니다.
     */
    public void syncPolicyActivation(long policyId, boolean isActive) {
        long version = nowEpochMillis();
        PolicyActivationOutboxPayload payload = PolicyActivationOutboxPayload.builder()
                .policyId(policyId)
                .active(isActive)
                .version(version)
                .build();

        executeTrackedAfterCommit(
                OutboxEventType.SYNC_POLICY_ACTIVATION,
                payload,
                null,
                "policy_activation_sync policyId=" + policyId + " isActive=" + isActive,
                () -> syncPolicyActivationUntracked(policyId, isActive, version)
        );
    }

    /**
     * line 단위 한도 정책(daily/shared)을 Redis에 즉시 반영합니다.
     */
    public void syncLineLimit(
            long lineId,
            Long dailyLimit,
            Boolean isDailyActive,
            Long sharedLimit,
            Boolean isSharedActive
    ) {
        long version = nowEpochMillis();
        LineLimitOutboxPayload payload = LineLimitOutboxPayload.builder()
                .lineId(lineId)
                .dailyLimit(dailyLimit)
                .isDailyActive(isDailyActive)
                .sharedLimit(sharedLimit)
                .isSharedActive(isSharedActive)
                .version(version)
                .build();

        executeTrackedAfterCommit(
                OutboxEventType.SYNC_LINE_LIMIT,
                payload,
                null,
                "line_limit_sync lineId=" + lineId,
                () -> syncLineLimitUntracked(lineId, dailyLimit, isDailyActive, sharedLimit, isSharedActive, version)
        );
    }

    /**
     * 즉시 차단 종료 시각을 Redis에 동기화합니다.
     */
    public void syncImmediateBlockEnd(long lineId, LocalDateTime blockEndAt) {
        long version = nowEpochMillis();
        long blockEndEpochSecond = resolveBlockEndEpochSecond(blockEndAt);
        ImmediateBlockOutboxPayload payload = ImmediateBlockOutboxPayload.builder()
                .lineId(lineId)
                .blockEndEpochSecond(blockEndEpochSecond)
                .version(version)
                .build();

        executeTrackedAfterCommit(
                OutboxEventType.SYNC_IMMEDIATE_BLOCK,
                payload,
                null,
                "immediate_block_sync lineId=" + lineId,
                () -> syncImmediateBlockEndUntracked(lineId, blockEndAt, version)
        );
    }

    /**
     * 반복 차단 정책 목록을 repeat_block hash 스냅샷으로 동기화합니다.
     */
    public void syncRepeatBlock(long lineId, List<RepeatBlockPolicyResDto> repeatBlocks) {
        long version = nowEpochMillis();
        LineScopedOutboxPayload payload = LineScopedOutboxPayload.builder()
                .lineId(lineId)
                .version(version)
                .build();

        executeTrackedAfterCommit(
                OutboxEventType.SYNC_REPEAT_BLOCK,
                payload,
                null,
                "repeat_block_sync lineId=" + lineId,
                () -> syncRepeatBlockUntracked(lineId, repeatBlocks, version)
        );
    }

    /**
     * 앱 정책(일 제한/속도 제한/화이트리스트)을 Redis 정책 키에 동기화합니다.
     */
    public void syncAppPolicy(
            long lineId,
            int appId,
            boolean isActive,
            Long dataLimit,
            Integer speedLimit,
            boolean isWhitelist
    ) {
        long version = nowEpochMillis();
        AppPolicyOutboxPayload payload = AppPolicyOutboxPayload.builder()
                .lineId(lineId)
                .appId(appId)
                .version(version)
                .build();

        executeTrackedAfterCommit(
                OutboxEventType.SYNC_APP_POLICY,
                payload,
                null,
                "app_policy_sync lineId=" + lineId + " appId=" + appId + " isActive=" + isActive,
                () -> syncAppPolicyUntracked(lineId, appId, isActive, dataLimit, speedLimit, isWhitelist, version)
        );
    }

    /**
     * 앱 정책 삭제 시 Redis의 관련 키 조각(limit/speed/whitelist)을 제거합니다.
     */
    public void evictAppPolicy(long lineId, int appId) {
        long version = nowEpochMillis();
        AppPolicyOutboxPayload payload = AppPolicyOutboxPayload.builder()
                .lineId(lineId)
                .appId(appId)
                .version(version)
                .build();

        executeTrackedAfterCommit(
                OutboxEventType.SYNC_APP_POLICY,
                payload,
                null,
                "app_policy_evict lineId=" + lineId + " appId=" + appId,
                () -> evictAppPolicyUntracked(lineId, appId, version)
        );
    }

    /**
     * 앱 정책 스냅샷을 Redis에 일괄 반영합니다.
     */
    public void syncAppPolicySnapshot(long lineId, List<AppPolicy> appPolicies) {
        long version = nowEpochMillis();
        LineScopedOutboxPayload payload = LineScopedOutboxPayload.builder()
                .lineId(lineId)
                .version(version)
                .build();

        executeTrackedAfterCommit(
                OutboxEventType.SYNC_APP_POLICY_SNAPSHOT,
                payload,
                null,
                "app_policy_snapshot_sync lineId=" + lineId,
                () -> syncAppPolicySnapshotUntracked(lineId, appPolicies, version)
        );
    }

    /**
     * Hydration/Outbox 재시도 경로에서 사용하는 비추적 동기화 메서드입니다.
     */
    public PolicySyncResult syncPolicyActivationUntracked(long policyId, boolean isActive, long version) {
        String key = trafficRedisKeyFactory.policyKey(policyId);
        String value = isActive ? "1" : "0";
        return executeWithRetry(
                "policy_activation_sync_untracked policyId=" + policyId + " isActive=" + isActive,
                () -> trafficPolicyVersionedRedisService.syncVersionedValue(key, value, version)
        );
    }

    /**
     * Hydration/Outbox 재시도 경로에서 사용하는 비추적 동기화 메서드입니다.
     */
    public PolicySyncResult syncLineLimitUntracked(
            long lineId,
            Long dailyLimit,
            Boolean isDailyActive,
            Long sharedLimit,
            Boolean isSharedActive,
            long version
    ) {
        String dailyLimitKey = trafficRedisKeyFactory.dailyTotalLimitKey(lineId);
        String monthlySharedLimitKey = trafficRedisKeyFactory.monthlySharedLimitKey(lineId);

        long dailyLimitValue = resolveLimitValue(dailyLimit, isDailyActive);
        long sharedLimitValue = resolveLimitValue(sharedLimit, isSharedActive);

        return executeWithRetry(
                "line_limit_sync_untracked lineId=" + lineId,
                () -> mergeSyncResult(
                        trafficPolicyVersionedRedisService.syncVersionedValue(dailyLimitKey, String.valueOf(dailyLimitValue), version),
                        trafficPolicyVersionedRedisService.syncVersionedValue(monthlySharedLimitKey, String.valueOf(sharedLimitValue), version)
                )
        );
    }

    /**
     * Hydration/Outbox 재시도 경로에서 사용하는 비추적 동기화 메서드입니다.
     */
    public PolicySyncResult syncImmediateBlockEndUntracked(long lineId, LocalDateTime blockEndAt, long version) {
        String key = trafficRedisKeyFactory.immediatelyBlockEndKey(lineId);
        String value = String.valueOf(resolveBlockEndEpochSecond(blockEndAt));
        return executeWithRetry(
                "immediate_block_sync_untracked lineId=" + lineId,
                () -> trafficPolicyVersionedRedisService.syncVersionedValue(key, value, version)
        );
    }

    /**
     * Hydration/Outbox 재시도 경로에서 사용하는 비추적 동기화 메서드입니다.
     */
    public PolicySyncResult syncRepeatBlockUntracked(long lineId, List<RepeatBlockPolicyResDto> repeatBlocks, long version) {
        String repeatBlockKey = trafficRedisKeyFactory.repeatBlockKey(lineId);
        Map<String, String> hashToWrite = buildRepeatBlockHash(repeatBlocks);
        return executeWithRetry(
                "repeat_block_sync_untracked lineId=" + lineId,
                () -> trafficPolicyVersionedRedisService.syncRepeatBlockSnapshot(repeatBlockKey, hashToWrite, version)
        );
    }

    /**
     * Hydration/Outbox 재시도 경로에서 사용하는 비추적 동기화 메서드입니다.
     */
    public PolicySyncResult syncAppPolicyUntracked(
            long lineId,
            int appId,
            boolean isActive,
            Long dataLimit,
            Integer speedLimit,
            boolean isWhitelist,
            long version
    ) {
        String appDataDailyLimitKey = trafficRedisKeyFactory.appDataDailyLimitKey(lineId);
        String appSpeedLimitKey = trafficRedisKeyFactory.appSpeedLimitKey(lineId);
        String appWhitelistKey = trafficRedisKeyFactory.appWhitelistKey(lineId);

        long normalizedDataLimit = dataLimit == null ? -1L : dataLimit;
        int normalizedSpeedLimit = normalizeAppSpeedLimitForRedis(speedLimit);

        return executeWithRetry(
                "app_policy_sync_untracked lineId=" + lineId + " appId=" + appId,
                () -> trafficPolicyVersionedRedisService.syncAppPolicySingle(
                        appDataDailyLimitKey,
                        appSpeedLimitKey,
                        appWhitelistKey,
                        appId,
                        isActive,
                        normalizedDataLimit,
                        normalizedSpeedLimit,
                        isWhitelist,
                        version
                )
        );
    }

    /**
     * Hydration/Outbox 재시도 경로에서 사용하는 비추적 동기화 메서드입니다.
     */
    public PolicySyncResult evictAppPolicyUntracked(long lineId, int appId, long version) {
        return syncAppPolicyUntracked(lineId, appId, false, -1L, -1, false, version);
    }

    /**
     * Hydration/Outbox 재시도 경로에서 사용하는 비추적 동기화 메서드입니다.
     */
    public PolicySyncResult syncAppPolicySnapshotUntracked(long lineId, List<AppPolicy> appPolicies, long version) {
        String appDataDailyLimitKey = trafficRedisKeyFactory.appDataDailyLimitKey(lineId);
        String appSpeedLimitKey = trafficRedisKeyFactory.appSpeedLimitKey(lineId);
        String appWhitelistKey = trafficRedisKeyFactory.appWhitelistKey(lineId);

        Map<String, String> dataLimitHash = new HashMap<>();
        Map<String, String> speedLimitHash = new HashMap<>();
        Set<String> whitelistMembers = new HashSet<>();

        if (appPolicies != null) {
            for (AppPolicy appPolicy : appPolicies) {
                if (appPolicy == null || appPolicy.getApplicationId() == null) {
                    continue;
                }
                if (!Boolean.TRUE.equals(appPolicy.getIsActive())) {
                    continue;
                }

                int appId = appPolicy.getApplicationId();
                long normalizedDataLimit = appPolicy.getDataLimit() == null ? -1L : appPolicy.getDataLimit();
                int normalizedSpeedLimit = normalizeAppSpeedLimitForRedis(appPolicy.getSpeedLimit());

                dataLimitHash.put(appDataLimitField(appId), String.valueOf(normalizedDataLimit));
                speedLimitHash.put(appSpeedLimitField(appId), String.valueOf(normalizedSpeedLimit));

                if (Boolean.TRUE.equals(appPolicy.getIsWhitelist())) {
                    whitelistMembers.add(String.valueOf(appId));
                }
            }
        }

        return executeWithRetry(
                "app_policy_snapshot_sync_untracked lineId=" + lineId,
                () -> trafficPolicyVersionedRedisService.syncAppPolicySnapshot(
                        appDataDailyLimitKey,
                        appSpeedLimitKey,
                        appWhitelistKey,
                        dataLimitHash,
                        speedLimitHash,
                        whitelistMembers,
                        version
                )
        );
    }

    /**
     * Outbox를 기록한 뒤, DB 커밋 이후에 Redis 동기화를 수행합니다.
     */
    private void executeTrackedAfterCommit(
            OutboxEventType eventType,
            Object payload,
            String uuid,
            String operationName,
            Supplier<PolicySyncResult> redisWriteOperation
    ) {
        long outboxId = redisOutboxRecordService.createPending(eventType, payload, uuid);
        executeAfterCommit(() -> {
            PolicySyncResult syncResult = redisWriteOperation.get();
            if (isSuccessEquivalent(syncResult)) {
                redisOutboxRecordService.markSuccess(outboxId);
                return;
            }

            redisOutboxRecordService.markFail(outboxId);
            log.warn("traffic_policy_write_through_outbox_marked_fail outboxId={} operation={} result={}", outboxId, operationName, syncResult);
        });
    }

    /**
     * 트랜잭션 경계에 맞춰 Redis write 실행 시점을 제어합니다.
     */
    private void executeAfterCommit(Runnable redisWriteOperation) {
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    redisWriteOperation.run();
                }
            });
            return;
        }

        redisWriteOperation.run();
    }

    /**
     * Redis write-through를 재시도 정책과 함께 실행합니다.
     */
    private PolicySyncResult executeWithRetry(String operationName, Supplier<PolicySyncResult> redisWriteOperation) {
        PolicySyncResult lastFailure = PolicySyncResult.RETRYABLE_FAILURE;
        for (int attempt = 1; attempt <= WRITE_THROUGH_RETRY_MAX; attempt++) {
            PolicySyncResult syncResult;
            try {
                syncResult = redisWriteOperation.get();
            } catch (RuntimeException e) {
                syncResult = PolicySyncResult.RETRYABLE_FAILURE;
                log.warn("traffic_policy_write_through_exception operation={} attempt={}/{}", operationName, attempt, WRITE_THROUGH_RETRY_MAX, e);
            }

            if (syncResult != PolicySyncResult.RETRYABLE_FAILURE
                    && syncResult != PolicySyncResult.CONNECTION_FAILURE) {
                return syncResult;
            }
            lastFailure = syncResult;

            if (attempt < WRITE_THROUGH_RETRY_MAX) {
                log.warn(
                        "traffic_policy_write_through_retry operation={} attempt={}/{}",
                        operationName,
                        attempt,
                        WRITE_THROUGH_RETRY_MAX
                );
                sleepBackoff();
            }
        }

        log.error(
                "traffic_policy_write_through_retry_exhausted operation={} attempts={}",
                operationName,
                WRITE_THROUGH_RETRY_MAX
        );
        return lastFailure;
    }

    /**
     * 두 개의 동기화 결과를 병합합니다.
     */
    private PolicySyncResult mergeSyncResult(PolicySyncResult first, PolicySyncResult second) {
        if (first == PolicySyncResult.RETRYABLE_FAILURE || second == PolicySyncResult.RETRYABLE_FAILURE) {
            return PolicySyncResult.RETRYABLE_FAILURE;
        }
        if (first == PolicySyncResult.CONNECTION_FAILURE || second == PolicySyncResult.CONNECTION_FAILURE) {
            return PolicySyncResult.CONNECTION_FAILURE;
        }
        if (first == PolicySyncResult.SUCCESS || second == PolicySyncResult.SUCCESS) {
            return PolicySyncResult.SUCCESS;
        }
        return PolicySyncResult.STALE_REJECTED;
    }

    /**
     * 반복 차단 DTO 목록을 Redis hash 포맷으로 변환합니다.
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
     * 한도 정책의 최종 저장값을 계산합니다.
     */
    private long resolveLimitValue(Long limit, Boolean isActive) {
        if (!Boolean.TRUE.equals(isActive)) {
            return -1L;
        }
        return limit == null ? -1L : limit;
    }

    /**
     * app 속도 제한값을 Redis 저장 규격으로 정규화합니다.
     */
    private int normalizeAppSpeedLimitForRedis(Integer speedLimit) {
        if (speedLimit == null) {
            return -1;
        } else if (speedLimit <= 0) {
            return speedLimit;
        }
        return speedLimit * APP_SPEED_LIMIT_UPLOAD_MULTIPLIER;
    }

    /**
     * app_data_daily_limit hash의 field 이름 규칙입니다.
     */
    private String appDataLimitField(int appId) {
        return "limit:" + appId;
    }

    /**
     * app_speed_limit hash의 field 이름 규칙입니다.
     */
    private String appSpeedLimitField(int appId) {
        return "speed:" + appId;
    }

    /**
     * 즉시 차단 종료시각을 epoch second로 변환합니다.
     * null은 tombstone 값(0)으로 저장합니다.
     */
    private long resolveBlockEndEpochSecond(LocalDateTime blockEndAt) {
        if (blockEndAt == null) {
            return 0L;
        }
        return blockEndAt.atZone(trafficRedisRuntimePolicy.zoneId()).toEpochSecond();
    }

    /**
     * 성공으로 간주할 동기화 결과인지 판별합니다.
     */
    private boolean isSuccessEquivalent(PolicySyncResult syncResult) {
        return syncResult == PolicySyncResult.SUCCESS
                || syncResult == PolicySyncResult.STALE_REJECTED;
    }

    /**
     * 정책 버전 비교에 사용하는 Epoch Millis를 생성합니다.
     */
    private long nowEpochMillis() {
        return System.currentTimeMillis();
    }

    /**
     * 재시도 간 짧은 백오프를 수행합니다.
     */
    private void sleepBackoff() {
        try {
            Thread.sleep(WRITE_THROUGH_RETRY_BACKOFF_MS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
