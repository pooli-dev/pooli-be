package com.pooli.traffic.domain.outbox;

/**
 * Redis Outbox에서 처리하는 이벤트 유형입니다.
 * event_type 문자열은 DB 저장값과 1:1로 매핑됩니다.
 */
public enum OutboxEventType {
    SYNC_POLICY_ACTIVATION,
    SYNC_LINE_LIMIT,
    SYNC_IMMEDIATE_BLOCK,
    SYNC_REPEAT_BLOCK,
    SYNC_APP_POLICY,
    SYNC_APP_POLICY_SNAPSHOT,
    REFILL
}
