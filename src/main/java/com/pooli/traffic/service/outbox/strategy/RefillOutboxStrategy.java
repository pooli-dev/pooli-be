package com.pooli.traffic.service.outbox.strategy;

import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import com.pooli.traffic.domain.outbox.OutboxEventType;
import com.pooli.traffic.domain.outbox.OutboxRetryResult;
import com.pooli.traffic.domain.outbox.RedisOutboxRecord;
import com.pooli.traffic.domain.outbox.payload.RefillOutboxPayload;
import com.pooli.traffic.service.outbox.RedisOutboxRecordService;
import com.pooli.traffic.service.outbox.TrafficRefillOutboxSupportService;
import com.pooli.traffic.service.runtime.TrafficQuotaCacheService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * REFILL 재시도 전략입니다.
 */
@Slf4j
@Component
@Profile({"local", "api", "traffic"})
@RequiredArgsConstructor
public class RefillOutboxStrategy implements OutboxEventRetryStrategy {

    private final RedisOutboxRecordService redisOutboxRecordService;
    private final TrafficRefillOutboxSupportService trafficRefillOutboxSupportService;
    private final TrafficQuotaCacheService trafficQuotaCacheService;

    @Override
    public OutboxEventType supports() {
        return OutboxEventType.REFILL;
    }

    @Override
    public OutboxRetryResult execute(RedisOutboxRecord record) {
        RefillOutboxPayload payload = redisOutboxRecordService.readPayload(record, RefillOutboxPayload.class);
        if (payload.getUuid() == null || payload.getUuid().isBlank()) {
            log.error("traffic_outbox_refill_payload_invalid outboxId={} reason=missing_uuid", record.getId());
            return OutboxRetryResult.FAIL;
        }

        long refillAmount = payload.getActualRefillAmount() == null ? 0L : payload.getActualRefillAmount();
        if (refillAmount <= 0) {
            return OutboxRetryResult.SUCCESS;
        }

        try {
            String balanceKey = trafficRefillOutboxSupportService.resolveBalanceKey(payload);
            String idempotencyKey = trafficRefillOutboxSupportService.resolveIdempotencyKey(payload.getUuid());
            long expireAtEpochSeconds = trafficRefillOutboxSupportService.resolveMonthlyExpireAt(payload);
            trafficQuotaCacheService.applyRefillWithIdempotency(
                    balanceKey,
                    idempotencyKey,
                    payload.getUuid(),
                    refillAmount,
                    expireAtEpochSeconds,
                    trafficRefillOutboxSupportService.refillIdempotencyTtlSeconds(),
                    // [Option A] Outbox 재처리 시점에는 DB 잔량 상태를 정확히 알 수 없으므로
                    // is_empty를 false로 전달한다. 잔량 복구가 목적이며,
                    // is_empty 상태는 다음 정상 리필 사이클에서 재기록된다.
                    false
            );
            return OutboxRetryResult.SUCCESS;
        } catch (RuntimeException e) {
            return OutboxRetryResult.FAIL;
        }
    }
}
