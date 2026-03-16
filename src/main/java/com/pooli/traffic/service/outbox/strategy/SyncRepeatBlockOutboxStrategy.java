package com.pooli.traffic.service.outbox.strategy;

import java.util.List;

import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import com.pooli.policy.domain.dto.response.RepeatBlockPolicyResDto;
import com.pooli.policy.mapper.RepeatBlockMapper;
import com.pooli.traffic.domain.outbox.OutboxEventType;
import com.pooli.traffic.domain.outbox.OutboxRetryResult;
import com.pooli.traffic.domain.outbox.RedisOutboxRecord;
import com.pooli.traffic.domain.outbox.payload.LineScopedOutboxPayload;
import com.pooli.traffic.service.outbox.RedisOutboxRecordService;
import com.pooli.traffic.service.policy.TrafficPolicyWriteThroughService;

import lombok.RequiredArgsConstructor;

/**
 * SYNC_REPEAT_BLOCK 재시도 전략입니다.
 */
@Component
@Profile({"local", "api", "traffic"})
@RequiredArgsConstructor
public class SyncRepeatBlockOutboxStrategy extends AbstractPolicyOutboxRetryStrategy {

    private final RedisOutboxRecordService redisOutboxRecordService;
    private final RepeatBlockMapper repeatBlockMapper;
    private final TrafficPolicyWriteThroughService trafficPolicyWriteThroughService;

    @Override
    public OutboxEventType supports() {
        return OutboxEventType.SYNC_REPEAT_BLOCK;
    }

    @Override
    public OutboxRetryResult execute(RedisOutboxRecord record) {
        LineScopedOutboxPayload payload = redisOutboxRecordService.readPayload(record, LineScopedOutboxPayload.class);
        List<RepeatBlockPolicyResDto> repeatBlocks = repeatBlockMapper.selectRepeatBlocksByLineId(payload.getLineId());
        return mapPolicySyncResult(
                trafficPolicyWriteThroughService.syncRepeatBlockUntracked(
                        payload.getLineId(),
                        repeatBlocks,
                        payload.getVersion()
                )
        );
    }
}
