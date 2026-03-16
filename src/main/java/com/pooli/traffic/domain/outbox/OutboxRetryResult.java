package com.pooli.traffic.domain.outbox;

/**
 * Outbox 재시도 전략 실행 결과입니다.
 */
public enum OutboxRetryResult {
    SUCCESS,
    FAIL
}
