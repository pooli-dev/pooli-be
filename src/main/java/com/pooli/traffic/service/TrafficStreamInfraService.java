package com.pooli.traffic.service;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Profile;
import org.springframework.data.domain.Range;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.Limit;
import org.springframework.data.redis.connection.RedisStreamCommands.XClaimOptions;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.PendingMessage;
import org.springframework.data.redis.connection.stream.PendingMessages;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.StreamOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import com.pooli.common.config.AppStreamsProperties;
import com.pooli.common.exception.ApplicationException;
import com.pooli.common.exception.CommonErrorCode;
import com.pooli.traffic.domain.TrafficStreamFields;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Redis Streams 소비 인프라를 담당하는 서비스입니다.
 * Consumer Group 생성, BLOCK 읽기, ACK, DLQ 적재를 공통 유틸로 제공합니다.
 */
@Slf4j
@Service
@Profile({"local", "traffic"})
@RequiredArgsConstructor
public class TrafficStreamInfraService {

    @Qualifier("streamsStringRedisTemplate")
    private final StringRedisTemplate streamsStringRedisTemplate;
    private final AppStreamsProperties appStreamsProperties;

    /**
     * Obtain the StreamOperations handle from the streams-dedicated RedisTemplate.
     *
     * @return the StreamOperations handle for interacting with Redis Streams
     */
    private StreamOperations<String, String, String> streamOps() {
        return streamsStringRedisTemplate.opsForStream();
    }

    /**
     * Ensure the Redis Streams consumer group for the traffic request stream exists.
     *
     * <p>If the consumer group already exists (BUSYGROUP), this method treats it as a successful outcome.
     *
     * @throws ApplicationException if creating the consumer group fails for reasons other than an existing group
     */
    public void ensureConsumerGroup() {
        String streamKey = appStreamsProperties.getKeyTrafficRequest();
        String group = appStreamsProperties.getGroupTraffic();

        try {
            streamOps().createGroup(streamKey, ReadOffset.latest(), group);
            log.info("traffic_stream_group_created streamKey={} group={}", streamKey, group);
        } catch (DataAccessException e) {
            if (isBusyGroupError(e)) {
                log.info("traffic_stream_group_exists streamKey={} group={}", streamKey, group);
                return;
            }
            log.error("traffic_stream_group_create_failed streamKey={} group={}", streamKey, group, e);
            throw new ApplicationException(CommonErrorCode.EXTERNAL_SYSTEM_ERROR, "Streams consumer group 생성에 실패했습니다.");
        }
    }

    /**
     * Read messages from the traffic request Redis stream for the configured consumer group using a blocking XREADGROUP.
     *
     * <p>Uses the configured read count, block timeout, group, and consumer name.
     *
     * @return the list of records read from the stream, or an empty list if no records were returned
     */
    public List<MapRecord<String, String, String>> readBlocking() {
        StreamReadOptions options = StreamReadOptions.empty()
                .count(appStreamsProperties.getReadCount())
                .block(Duration.ofMillis(appStreamsProperties.getBlockMs()));

        Consumer consumer = Consumer.from(
                appStreamsProperties.getGroupTraffic(),
                appStreamsProperties.getConsumerName()
        );

        List<MapRecord<String, String, String>> records = readGroupRecords(
                consumer,
                options,
                StreamOffset.create(appStreamsProperties.getKeyTrafficRequest(), ReadOffset.lastConsumed())
        );

        if (records == null) {
            return List.of();
        }

        return records;
    }

    /**
     * Acknowledge the specified record in the configured consumer group.
     *
     * @param recordId the ID of the record to acknowledge
     * @return the number of records acknowledged
     */
    public long acknowledge(RecordId recordId) {
        return streamOps().acknowledge(
                appStreamsProperties.getKeyTrafficRequest(),
                appStreamsProperties.getGroupTraffic(),
                recordId
        );
    }

    /**
     * Retrieve pending messages for the configured traffic request stream and consumer group.
     *
     * @param count the maximum number of pending messages to retrieve; values less than 1 are treated as 1
     * @return a list of pending messages for the group, or an empty list if none exist
     */
    public List<PendingMessage> readPendingMessages(long count) {
        long safeCount = Math.max(1L, count);
        PendingMessages pendingMessages = streamOps().pending(
                appStreamsProperties.getKeyTrafficRequest(),
                appStreamsProperties.getGroupTraffic(),
                Range.unbounded(),
                safeCount
        );

        if (pendingMessages == null || pendingMessages.isEmpty()) {
            return List.of();
        }

        List<PendingMessage> messages = new ArrayList<>(pendingMessages.size());
        for (PendingMessage pendingMessage : pendingMessages) {
            messages.add(pendingMessage);
        }
        return messages;
    }

    /**
     * Claims the specified pending stream records for the current consumer when they have been idle at least the given time.
     *
     * @param recordIds pending record IDs to claim
     * @param minIdleMs minimum idle time in milliseconds required for a record to be eligible for claim
     * @return a list of claimed records, or an empty list if none were claimed
     */
    public List<MapRecord<String, String, String>> claimPending(
            List<RecordId> recordIds,
            long minIdleMs
    ) {
        if (recordIds == null || recordIds.isEmpty()) {
            return List.of();
        }

        XClaimOptions claimOptions = XClaimOptions.minIdleMs(Math.max(0L, minIdleMs))
                .ids(recordIds.toArray(RecordId[]::new));

        List<MapRecord<String, String, String>> claimed = streamOps().claim(
                appStreamsProperties.getKeyTrafficRequest(),
                appStreamsProperties.getGroupTraffic(),
                appStreamsProperties.getConsumerName(),
                claimOptions
        );

        if (claimed == null) {
            return List.of();
        }
        return claimed;
    }

    /**
     * Retrieve a single record from the traffic request stream by its RecordId.
     *
     * @param recordId the RecordId to look up; if null the method returns null
     * @return the matching MapRecord if found, or `null` if no record matches the given id
     */
    public MapRecord<String, String, String> readRecordById(RecordId recordId) {
        if (recordId == null) {
            return null;
        }

        List<MapRecord<String, String, String>> records = streamOps().range(
                appStreamsProperties.getKeyTrafficRequest(),
                Range.closed(recordId.getValue(), recordId.getValue()),
                Limit.limit().count(1)
        );

        if (records == null || records.isEmpty()) {
            return null;
        }
        return records.get(0);
    }

    /**
     * Writes a failed message entry to the DLQ stream.
     *
     * <p>The DLQ entry contains the following fields: payload (original payload JSON or empty string if null), reason, sourceRecordId, and failedAt (epoch milliseconds).
     *
     * @param payload original payload; null is stored as an empty string
     * @param reason failure reason
     * @param sourceRecordId original stream record ID
     * @return the RecordId of the created DLQ entry
     */
    public RecordId writeDlq(String payload, String reason, String sourceRecordId) {
        Map<String, String> dlqValue = new HashMap<>();
        dlqValue.put(TrafficStreamFields.PAYLOAD, payload == null ? "" : payload);
        dlqValue.put("reason", reason);
        dlqValue.put("sourceRecordId", sourceRecordId);
        dlqValue.put("failedAt", String.valueOf(System.currentTimeMillis()));

        return streamOps().add(
                StreamRecords.string(dlqValue)
                        .withStreamKey(appStreamsProperties.getKeyTrafficDlq())
        );
    }

    /**
     * Extracts the {@code payload} field from a Redis stream record's value map.
     *
     * @param record the stream record whose value map may contain the {@code payload} field
     * @return the {@code payload} value, or {@code null} if the field is not present
     */
    public String extractPayload(MapRecord<String, String, String> record) {
        return record.getValue().get(TrafficStreamFields.PAYLOAD);
    }

    /**
     * Consumer Group 생성 예외가 "이미 그룹이 존재"하는 케이스인지 판별합니다.
     *
     * <p>라이브러리/드라이버별 예외 타입 차이를 흡수하기 위해 메시지(BUSYGROUP)와
     * 예외 클래스명(RedisBusyException)을 모두 확인합니다.
     *
     * @param throwable 판별할 예외
     * @return 이미 그룹이 존재하는 예외면 true
     */
    private boolean isBusyGroupError(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            String message = current.getMessage();
            if (message != null && message.contains("BUSYGROUP")) {
                return true;
            }

            String exceptionType = current.getClass().getName();
            if (exceptionType.contains("RedisBusyException")) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    /**
     * Read stream records for the given consumer using the specified read options and stream offsets.
     *
     * @param consumer the consumer (group + consumer name) performing the read
     * @param options  stream read options (e.g., count, block timeout)
     * @param streamOffsets the stream offsets to read from
     * @return the list of records returned by the Redis Streams read operation
     */
    @SafeVarargs
    private final List<MapRecord<String, String, String>> readGroupRecords(
            Consumer consumer,
            StreamReadOptions options,
            StreamOffset<String>... streamOffsets
    ) {
        return streamOps().read(consumer, options, streamOffsets);
    }
}
