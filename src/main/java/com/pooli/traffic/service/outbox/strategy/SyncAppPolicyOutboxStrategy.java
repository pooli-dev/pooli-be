package com.pooli.traffic.service.outbox.strategy;

import java.util.Optional;

import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import com.pooli.policy.domain.dto.response.AppPolicyResDto;
import com.pooli.policy.mapper.AppPolicyMapper;
import com.pooli.traffic.domain.outbox.OutboxEventType;
import com.pooli.traffic.domain.outbox.OutboxRetryResult;
import com.pooli.traffic.domain.outbox.RedisOutboxRecord;
import com.pooli.traffic.domain.outbox.payload.AppPolicyOutboxPayload;
import com.pooli.traffic.service.outbox.RedisOutboxRecordService;
import com.pooli.traffic.service.policy.TrafficPolicyWriteThroughService;

import lombok.RequiredArgsConstructor;

/**
 * SYNC_APP_POLICY 재시도 전략입니다.
 */
@Component
@Profile({"local", "api", "traffic"})
@RequiredArgsConstructor
public class SyncAppPolicyOutboxStrategy extends AbstractPolicyOutboxRetryStrategy {

    private final RedisOutboxRecordService redisOutboxRecordService;
    private final AppPolicyMapper appPolicyMapper;
    private final TrafficPolicyWriteThroughService trafficPolicyWriteThroughService;

    @Override
    public OutboxEventType supports() {
        return OutboxEventType.SYNC_APP_POLICY;
    }

    @Override
    public OutboxRetryResult execute(RedisOutboxRecord record) {
        AppPolicyOutboxPayload payload = redisOutboxRecordService.readPayload(record, AppPolicyOutboxPayload.class);
        Optional<AppPolicyResDto> appPolicyOptional =
                appPolicyMapper.findDtoExistByLineIdAndAppId(payload.getLineId(), payload.getAppId());

        if (appPolicyOptional.isEmpty() || appPolicyOptional.get().getAppPolicyId() == null) {
            return mapPolicySyncResult(
                    trafficPolicyWriteThroughService.evictAppPolicyUntracked(
                            payload.getLineId(),
                            payload.getAppId(),
                            payload.getVersion()
                    )
            );
        }

        AppPolicyResDto appPolicy = appPolicyOptional.get();
        return mapPolicySyncResult(
                trafficPolicyWriteThroughService.syncAppPolicyUntracked(
                        payload.getLineId(),
                        payload.getAppId(),
                        Boolean.TRUE.equals(appPolicy.getIsActive()),
                        appPolicy.getDailyLimitData(),
                        appPolicy.getDailyLimitSpeed(),
                        Boolean.TRUE.equals(appPolicy.getIsWhiteList()),
                        payload.getVersion()
                )
        );
    }
}
