package com.pooli.traffic.service.outbox;

/**
 * 정책 Redis 동기화 실행 결과를 표현합니다.
 */
public enum PolicySyncResult {
    SUCCESS,
    STALE_REJECTED,
    CONNECTION_FAILURE,
    RETRYABLE_FAILURE
}
