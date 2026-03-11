package com.pooli.traffic.service;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.springframework.context.annotation.Profile;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.PendingMessage;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.stereotype.Service;

import com.pooli.common.config.AppStreamsProperties;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Streams pending 메시지의 reclaim/retry/DLQ 분기를 담당하는 서비스입니다.
 * min-idle 기준으로 재처리 대상을 선별하고, retry 초과 메시지는 DLQ로 이동합니다.
 */
@Slf4j
@Service
@Profile({"local", "traffic"})
@RequiredArgsConstructor
public class TrafficStreamReclaimService {

    private final TrafficStreamInfraService trafficStreamInfraService;
    private final AppStreamsProperties appStreamsProperties;

    /**
     * Reclaims eligible pending Redis Stream messages and routes messages that exceeded retry limits to the dead-letter queue.
     *
     * Reclaimed records are returned for downstream processing; messages that exceed the configured retry limit are moved to DLQ and acknowledged so they are not reprocessed.
     *
     * @return a list of reclaimed stream records, or an empty list if no records were reclaimed
     */
    public List<MapRecord<String, String, String>> reclaimAndRouteExceededRetries() {
        // pending 스캔 건수는 readCount를 재사용하되, 최소 1건을 보장한다.
        long pendingScanCount = Math.max(1L, appStreamsProperties.getReadCount());
        List<PendingMessage> pendingMessages = trafficStreamInfraService.readPendingMessages(pendingScanCount);
        if (pendingMessages.isEmpty()) {
            return List.of();
        }

        long reclaimMinIdleMs = Math.max(0L, appStreamsProperties.getReclaimMinIdleMs());
        int maxRetry = Math.max(0, appStreamsProperties.getMaxRetry());

        // 재처리 가능한 메시지만 claim 대상에 모아둔다.
        List<RecordId> claimCandidates = new ArrayList<>();
        for (PendingMessage pendingMessage : pendingMessages) {
            if (!isIdleEnoughForReclaim(pendingMessage, reclaimMinIdleMs)) {
                continue;
            }

            // delivery count가 maxRetry를 초과한 메시지는 복구 대신 DLQ로 보낸다.
            if (hasExceededRetryLimit(pendingMessage, maxRetry)) {
                moveToDlqAndAcknowledge(pendingMessage, maxRetry);
                continue;
            }

            claimCandidates.add(pendingMessage.getId());
        }

        // reclaim 대상이 없으면 worker에 넘길 레코드도 없다.
        if (claimCandidates.isEmpty()) {
            return List.of();
        }

        return trafficStreamInfraService.claimPending(claimCandidates, reclaimMinIdleMs);
    }

    /**
     * Determines whether a pending message has been idle at least the configured reclaim threshold.
     *
     * @param pendingMessage the pending stream message to evaluate
     * @param reclaimMinIdleMs minimum idle time in milliseconds required for reclaim
     * @return true if the message's idle time is greater than or equal to {@code reclaimMinIdleMs}, false otherwise
     */
    private boolean isIdleEnoughForReclaim(PendingMessage pendingMessage, long reclaimMinIdleMs) {
        Duration elapsedSinceLastDelivery = pendingMessage.getElapsedTimeSinceLastDelivery();
        long elapsedMs = elapsedSinceLastDelivery == null ? 0L : elapsedSinceLastDelivery.toMillis();
        return elapsedMs >= reclaimMinIdleMs;
    }

    /**
     * Determine whether a pending message's delivery attempts exceed the configured retry limit.
     *
     * @param pendingMessage the pending stream message to evaluate
     * @param maxRetry the maximum allowed delivery attempts
     * @return `true` if the message's total delivery count is greater than `maxRetry`, `false` otherwise
     */
    private boolean hasExceededRetryLimit(PendingMessage pendingMessage, int maxRetry) {
        return pendingMessage.getTotalDeliveryCount() > maxRetry;
    }

    /**
     * Moves a pending message to the dead-letter queue with a reason that includes its delivery count,
     * then acknowledges the original pending entry to prevent message loss.
     *
     * @param pendingMessage the pending stream entry to move and acknowledge
     * @param maxRetry the configured maximum retry count used in the DLQ reason message
     */
    private void moveToDlqAndAcknowledge(PendingMessage pendingMessage, int maxRetry) {
        String sourceRecordId = pendingMessage.getIdAsString();
        MapRecord<String, String, String> sourceRecord =
                trafficStreamInfraService.readRecordById(pendingMessage.getId());
        String payload = sourceRecord == null ? null : trafficStreamInfraService.extractPayload(sourceRecord);

        String reason = String.format(
                "max retry 초과: deliveryCount=%d, maxRetry=%d",
                pendingMessage.getTotalDeliveryCount(),
                maxRetry
        );

        // DLQ 적재 성공 이후에 ACK해 메시지 유실을 방지한다.
        trafficStreamInfraService.writeDlq(payload, reason, sourceRecordId);
        trafficStreamInfraService.acknowledge(pendingMessage.getId());

        log.warn(
                "traffic_stream_record_moved_to_dlq recordId={} deliveryCount={} maxRetry={}",
                sourceRecordId,
                pendingMessage.getTotalDeliveryCount(),
                maxRetry
        );
    }
}
