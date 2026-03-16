-- Redis failover 복구를 위한 Outbox 테이블
-- 정책 write-through/리필 Redis 반영 실패를 기록하고 스케줄러 재시도 대상으로 사용한다.

CREATE TABLE IF NOT EXISTS TRAFFIC_REDIS_OUTBOX (
    id                BIGINT       NOT NULL AUTO_INCREMENT,
    event_type        VARCHAR(64)  NOT NULL,
    payload           TEXT         NOT NULL,
    uuid              VARCHAR(64)  NULL,
    status            VARCHAR(16)  NOT NULL,
    retry_count       INT          NOT NULL DEFAULT 0,
    created_at        DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    status_updated_at DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (id),
    KEY idx_traffic_redis_outbox_status_created_at (status, created_at),
    KEY idx_traffic_redis_outbox_status_updated_at (status, status_updated_at)
);
