package com.pooli.traffic.service.outbox.strategy;

import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import com.pooli.traffic.domain.outbox.OutboxEventType;
import com.pooli.traffic.domain.outbox.OutboxRetryResult;
import com.pooli.traffic.domain.outbox.RedisOutboxRecord;
import com.pooli.traffic.domain.outbox.payload.PolicyActivationOutboxPayload;
import com.pooli.traffic.service.outbox.RedisOutboxRecordService;
import com.pooli.traffic.service.policy.TrafficPolicyWriteThroughService;

import lombok.RequiredArgsConstructor;

/**
 * SYNC_POLICY_ACTIVATION 재시도 전략입니다.
 */
@Component
@Profile({"local", "api", "traffic"})
@RequiredArgsConstructor
public class SyncPolicyActivationOutboxStrategy extends AbstractPolicyOutboxRetryStrategy {

    private final RedisOutboxRecordService redisOutboxRecordService;
    private final TrafficPolicyWriteThroughService trafficPolicyWriteThroughService;

    @Override
    public OutboxEventType supports() {
        return OutboxEventType.SYNC_POLICY_ACTIVATION;
    }

    @Override
    public OutboxRetryResult execute(RedisOutboxRecord record) {
        PolicyActivationOutboxPayload payload = redisOutboxRecordService.readPayload(record, PolicyActivationOutboxPayload.class);
        return mapPolicySyncResult(
                trafficPolicyWriteThroughService.syncPolicyActivationUntracked(
                        payload.getPolicyId(),
                        Boolean.TRUE.equals(payload.getActive()),
                        payload.getVersion()
                )
        );
    }
}
