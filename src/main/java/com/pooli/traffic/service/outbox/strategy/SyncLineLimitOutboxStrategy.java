package com.pooli.traffic.service.outbox.strategy;

import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import com.pooli.traffic.domain.outbox.OutboxEventType;
import com.pooli.traffic.domain.outbox.OutboxRetryResult;
import com.pooli.traffic.domain.outbox.RedisOutboxRecord;
import com.pooli.traffic.domain.outbox.payload.LineLimitOutboxPayload;
import com.pooli.traffic.service.outbox.RedisOutboxRecordService;
import com.pooli.traffic.service.policy.TrafficPolicyWriteThroughService;

import lombok.RequiredArgsConstructor;

/**
 * SYNC_LINE_LIMIT 재시도 전략입니다.
 */
@Component
@Profile({"local", "api", "traffic"})
@RequiredArgsConstructor
public class SyncLineLimitOutboxStrategy extends AbstractPolicyOutboxRetryStrategy {

    private final RedisOutboxRecordService redisOutboxRecordService;
    private final TrafficPolicyWriteThroughService trafficPolicyWriteThroughService;

    @Override
    public OutboxEventType supports() {
        return OutboxEventType.SYNC_LINE_LIMIT;
    }

    @Override
    public OutboxRetryResult execute(RedisOutboxRecord record) {
        LineLimitOutboxPayload payload = redisOutboxRecordService.readPayload(record, LineLimitOutboxPayload.class);
        return mapPolicySyncResult(
                trafficPolicyWriteThroughService.syncLineLimitUntracked(
                        payload.getLineId(),
                        payload.getDailyLimit(),
                        payload.getIsDailyActive(),
                        payload.getSharedLimit(),
                        payload.getIsSharedActive(),
                        payload.getVersion()
                )
        );
    }
}
