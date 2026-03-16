package com.pooli.traffic.service.outbox;

import java.util.List;

import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pooli.common.exception.ApplicationException;
import com.pooli.common.exception.CommonErrorCode;
import com.pooli.traffic.domain.outbox.OutboxEventType;
import com.pooli.traffic.domain.outbox.OutboxStatus;
import com.pooli.traffic.domain.outbox.RedisOutboxRecord;
import com.pooli.traffic.mapper.RedisOutboxMapper;

import lombok.RequiredArgsConstructor;

/**
 * Redis Outbox 레코드 생성/조회/상태전이를 담당하는 서비스입니다.
 */
@Service
@Profile({"local", "api", "traffic"})
@RequiredArgsConstructor
public class RedisOutboxRecordService {

    private final RedisOutboxMapper redisOutboxMapper;
    private final ObjectMapper objectMapper;

    /**
     * 신규 PENDING Outbox 레코드를 생성합니다.
     *
     * @return 생성된 Outbox ID
     */
    public long createPending(OutboxEventType eventType, Object payloadObject, String uuid) {
        String payloadJson = toJson(payloadObject);
        RedisOutboxRecord record = RedisOutboxRecord.builder()
                .eventType(eventType)
                .payload(payloadJson)
                .uuid(uuid)
                .status(OutboxStatus.PENDING)
                .retryCount(0)
                .build();

        redisOutboxMapper.insert(record);
        if (record.getId() == null) {
            throw new ApplicationException(CommonErrorCode.DATABASE_ERROR, "Outbox ID 생성에 실패했습니다.");
        }
        return record.getId();
    }

    /**
     * 스케줄러 대상 레코드를 잠금 조회하고 PROCESSING으로 선점합니다.
     */
    @Transactional
    public List<RedisOutboxRecord> lockRetryCandidatesAndMarkProcessing(
            int limit,
            int pendingDelaySeconds,
            int processingStuckSeconds
    ) {
        List<RedisOutboxRecord> candidates = redisOutboxMapper.selectRetryCandidatesForUpdate(
                limit,
                pendingDelaySeconds,
                processingStuckSeconds
        );

        for (RedisOutboxRecord candidate : candidates) {
            if (candidate.getId() == null) {
                continue;
            }
            redisOutboxMapper.markProcessing(candidate.getId());
        }

        return candidates;
    }

    /**
     * Outbox 레코드를 SUCCESS로 전이합니다.
     */
    public void markSuccess(long id) {
        redisOutboxMapper.markSuccess(id);
    }

    /**
     * Outbox 레코드를 FAIL로 전이합니다(재시도 횟수 유지).
     */
    public void markFail(long id) {
        redisOutboxMapper.markFail(id);
    }

    /**
     * Outbox 레코드를 FAIL로 전이하고 retry_count를 1 증가시킵니다.
     */
    public void markFailWithRetryIncrement(long id) {
        redisOutboxMapper.markFailWithRetryIncrement(id);
    }

    /**
     * Outbox 레코드를 FAIL로 전이하고 retry_count를 지정 값으로 설정합니다.
     */
    public void markFailWithRetryCount(long id, int retryCount) {
        redisOutboxMapper.markFailWithRetryCount(id, retryCount);
    }

    /**
     * REFILL 레코드를 REVERT로 CAS 전이합니다.
     *
     * @return 1: 이번 호출에서 REVERT 전이 성공, 0: 이미 보상 처리됐거나 보상 불가 상태
     */
    public int markRevertIfCompensable(long id) {
        return redisOutboxMapper.markRevertIfCompensable(id);
    }

    /**
     * Outbox payload JSON을 타입에 맞게 역직렬화합니다.
     */
    public <T> T readPayload(RedisOutboxRecord record, Class<T> payloadType) {
        if (record == null || record.getPayload() == null || record.getPayload().isBlank()) {
            throw new ApplicationException(CommonErrorCode.INTERNAL_SERVER_ERROR, "Outbox payload가 비어 있습니다.");
        }
        try {
            return objectMapper.readValue(record.getPayload(), payloadType);
        } catch (JsonProcessingException e) {
            throw new ApplicationException(CommonErrorCode.INTERNAL_SERVER_ERROR, "Outbox payload 파싱에 실패했습니다.");
        }
    }

    /**
     * 객체를 JSON 문자열로 직렬화합니다.
     */
    private String toJson(Object payloadObject) {
        if (payloadObject == null) {
            throw new ApplicationException(CommonErrorCode.INTERNAL_SERVER_ERROR, "Outbox payload 객체가 비어 있습니다.");
        }
        try {
            return objectMapper.writeValueAsString(payloadObject);
        } catch (JsonProcessingException e) {
            throw new ApplicationException(CommonErrorCode.INTERNAL_SERVER_ERROR, "Outbox payload 직렬화에 실패했습니다.");
        }
    }
}
