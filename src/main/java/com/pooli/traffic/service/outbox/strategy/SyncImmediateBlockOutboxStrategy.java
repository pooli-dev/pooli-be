package com.pooli.traffic.service.outbox.strategy;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import com.pooli.traffic.domain.outbox.OutboxEventType;
import com.pooli.traffic.domain.outbox.OutboxRetryResult;
import com.pooli.traffic.domain.outbox.RedisOutboxRecord;
import com.pooli.traffic.domain.outbox.payload.ImmediateBlockOutboxPayload;
import com.pooli.traffic.service.outbox.RedisOutboxRecordService;
import com.pooli.traffic.service.runtime.TrafficRedisRuntimePolicy;
import com.pooli.traffic.service.policy.TrafficPolicyWriteThroughService;

import lombok.RequiredArgsConstructor;

/**
 * SYNC_IMMEDIATE_BLOCK 재시도 전략입니다.
 */
@Component
@Profile({"local", "api", "traffic"})
@RequiredArgsConstructor
public class SyncImmediateBlockOutboxStrategy extends AbstractPolicyOutboxRetryStrategy {

    private final RedisOutboxRecordService redisOutboxRecordService;
    private final TrafficPolicyWriteThroughService trafficPolicyWriteThroughService;
    private final TrafficRedisRuntimePolicy trafficRedisRuntimePolicy;

    @Override
    public OutboxEventType supports() {
        return OutboxEventType.SYNC_IMMEDIATE_BLOCK;
    }

    @Override
    public OutboxRetryResult execute(RedisOutboxRecord record) {
        ImmediateBlockOutboxPayload payload = redisOutboxRecordService.readPayload(record, ImmediateBlockOutboxPayload.class);
        LocalDateTime blockEndAt = toLocalDateTime(payload.getBlockEndEpochSecond(), trafficRedisRuntimePolicy.zoneId());
        return mapPolicySyncResult(
                trafficPolicyWriteThroughService.syncImmediateBlockEndUntracked(
                        payload.getLineId(),
                        blockEndAt,
                        payload.getVersion()
                )
        );
    }

    private LocalDateTime toLocalDateTime(Long epochSecond, ZoneId zoneId) {
        if (epochSecond == null || epochSecond <= 0) {
            return null;
        }
        return LocalDateTime.ofInstant(Instant.ofEpochSecond(epochSecond), zoneId);
    }
}
