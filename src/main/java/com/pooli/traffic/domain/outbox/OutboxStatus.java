package com.pooli.traffic.domain.outbox;

/**
 * Outbox 레코드의 처리 상태입니다.
 */
public enum OutboxStatus {
    PENDING,
    PROCESSING,
    SUCCESS,
    FAIL,
    REVERT
}
