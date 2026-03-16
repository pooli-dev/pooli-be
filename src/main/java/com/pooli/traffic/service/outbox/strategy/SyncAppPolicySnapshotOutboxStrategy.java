package com.pooli.traffic.service.outbox.strategy;

import java.util.List;

import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import com.pooli.policy.domain.entity.AppPolicy;
import com.pooli.policy.mapper.AppPolicyMapper;
import com.pooli.traffic.domain.outbox.OutboxEventType;
import com.pooli.traffic.domain.outbox.OutboxRetryResult;
import com.pooli.traffic.domain.outbox.RedisOutboxRecord;
import com.pooli.traffic.domain.outbox.payload.LineScopedOutboxPayload;
import com.pooli.traffic.service.outbox.RedisOutboxRecordService;
import com.pooli.traffic.service.policy.TrafficPolicyWriteThroughService;

import lombok.RequiredArgsConstructor;

/**
 * SYNC_APP_POLICY_SNAPSHOT 재시도 전략입니다.
 */
@Component
@Profile({"local", "api", "traffic"})
@RequiredArgsConstructor
public class SyncAppPolicySnapshotOutboxStrategy extends AbstractPolicyOutboxRetryStrategy {

    private final RedisOutboxRecordService redisOutboxRecordService;
    private final AppPolicyMapper appPolicyMapper;
    private final TrafficPolicyWriteThroughService trafficPolicyWriteThroughService;

    @Override
    public OutboxEventType supports() {
        return OutboxEventType.SYNC_APP_POLICY_SNAPSHOT;
    }

    @Override
    public OutboxRetryResult execute(RedisOutboxRecord record) {
        LineScopedOutboxPayload payload = redisOutboxRecordService.readPayload(record, LineScopedOutboxPayload.class);
        List<AppPolicy> appPolicies = appPolicyMapper.findAllEntityByLineId(payload.getLineId());
        return mapPolicySyncResult(
                trafficPolicyWriteThroughService.syncAppPolicySnapshotUntracked(
                        payload.getLineId(),
                        appPolicies,
                        payload.getVersion()
                )
        );
    }
}
