package com.pooli.traffic.service.outbox.strategy;

import com.pooli.traffic.domain.outbox.OutboxEventType;
import com.pooli.traffic.domain.outbox.OutboxRetryResult;
import com.pooli.traffic.domain.outbox.RedisOutboxRecord;

/**
 * Outbox event_type별 재시도 전략 계약입니다.
 */
public interface OutboxEventRetryStrategy {

    /**
     * 이 전략이 담당하는 event_type을 반환합니다.
     */
    OutboxEventType supports();

    /**
     * Outbox 레코드 재시도를 수행하고 성공/실패를 반환합니다.
     */
    OutboxRetryResult execute(RedisOutboxRecord record);
}
