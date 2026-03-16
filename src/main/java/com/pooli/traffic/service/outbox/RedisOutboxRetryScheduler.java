package com.pooli.traffic.service.outbox;

import java.util.List;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.pooli.traffic.domain.outbox.OutboxEventType;
import com.pooli.traffic.domain.outbox.OutboxRetryResult;
import com.pooli.traffic.domain.outbox.RedisOutboxRecord;
import com.pooli.traffic.domain.outbox.payload.RefillOutboxPayload;
import com.pooli.traffic.service.outbox.strategy.OutboxEventRetryStrategy;
import com.pooli.traffic.service.outbox.strategy.OutboxRetryStrategyRegistry;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Redis Outbox FAIL/PENDING/PROCESSING 레코드를 주기적으로 재시도하는 스케줄러입니다.
 */
@Slf4j
@Component
@Profile({"local", "api", "traffic"})
@RequiredArgsConstructor
public class RedisOutboxRetryScheduler {

    private static final int TERMINAL_RETRY_MARKER = 22;

    @Value("${app.traffic.outbox.retry.batch-size:100}")
    private int batchSize;

    @Value("${app.traffic.outbox.retry.pending-delay-seconds:60}")
    private int pendingDelaySeconds;

    @Value("${app.traffic.outbox.retry.processing-stuck-seconds:150}")
    private int processingStuckSeconds;

    @Value("${app.traffic.outbox.retry.max-retry-count:10}")
    private int maxRetryCount;

    private final RedisOutboxRecordService redisOutboxRecordService;
    private final OutboxRetryStrategyRegistry outboxRetryStrategyRegistry;
    private final TrafficRefillOutboxSupportService trafficRefillOutboxSupportService;

    @Scheduled(fixedDelayString = "${app.traffic.outbox.retry.fixed-delay-ms:5000}")
    public void runRetryCycle() {
        int normalizedBatchSize = Math.max(1, batchSize);
        int normalizedPendingDelaySeconds = Math.max(1, pendingDelaySeconds);
        int normalizedProcessingStuckSeconds = Math.max(1, processingStuckSeconds);

        List<RedisOutboxRecord> candidates = redisOutboxRecordService.lockRetryCandidatesAndMarkProcessing(
                normalizedBatchSize,
                normalizedPendingDelaySeconds,
                normalizedProcessingStuckSeconds
        );

        for (RedisOutboxRecord candidate : candidates) {
            if (candidate.getId() == null || candidate.getEventType() == null) {
                continue;
            }

            try {
                processOne(candidate);
            } catch (RuntimeException e) {
                log.error(
                        "traffic_outbox_retry_cycle_failed outboxId={} eventType={}",
                        candidate.getId(),
                        candidate.getEventType(),
                        e
                );
                redisOutboxRecordService.markFailWithRetryIncrement(candidate.getId());
            }
        }
    }

    private void processOne(RedisOutboxRecord record) {
        int retryCount = record.getRetryCount() == null ? 0 : record.getRetryCount();
        if (retryCount >= maxRetryCount) {
            handleRetryCapExceeded(record, retryCount);
            return;
        }

        OutboxEventRetryStrategy strategy = outboxRetryStrategyRegistry.get(record.getEventType());
        OutboxRetryResult retryResult = strategy.execute(record);
        if (retryResult == OutboxRetryResult.SUCCESS) {
            redisOutboxRecordService.markSuccess(record.getId());
            if (record.getEventType() == OutboxEventType.REFILL) {
                RefillOutboxPayload payload = redisOutboxRecordService.readPayload(record, RefillOutboxPayload.class);
                trafficRefillOutboxSupportService.clearIdempotency(payload.getUuid());
            }
            return;
        }

        redisOutboxRecordService.markFailWithRetryIncrement(record.getId());
    }

    /**
     * retry_count 상한 초과 레코드를 처리합니다.
     * - 재시도는 수행하지 않습니다.
     * - REFILL은 DB 반납을 1회 수행하고 REVERT 터미널 상태로 종료합니다.
     * - 비-REFILL은 기존처럼 retry_count를 터미널 마커(22)로 고정합니다.
     */
    private void handleRetryCapExceeded(RedisOutboxRecord record, int retryCount) {
        if (retryCount >= TERMINAL_RETRY_MARKER) {
            // 이미 cap 처리를 마친 레코드는 무해하게 FAIL 유지한다.
            redisOutboxRecordService.markFail(record.getId());
            return;
        }

        log.error(
                "traffic_outbox_retry_cap_exceeded outboxId={} eventType={} retryCount={} reason=max_retry_exceeded",
                record.getId(),
                record.getEventType(),
                retryCount
        );

        if (record.getEventType() == OutboxEventType.REFILL) {
            RefillOutboxPayload payload = redisOutboxRecordService.readPayload(record, RefillOutboxPayload.class);
            trafficRefillOutboxSupportService.compensateRefillOnce(record.getId(), payload);
            return;
        }

        redisOutboxRecordService.markFailWithRetryCount(record.getId(), TERMINAL_RETRY_MARKER);
    }
}
