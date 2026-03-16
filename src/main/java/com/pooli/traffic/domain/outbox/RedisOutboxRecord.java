package com.pooli.traffic.domain.outbox;

import java.time.LocalDateTime;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * TRAFFIC_REDIS_OUTBOX 테이블 레코드 매핑 객체입니다.
 */
@Getter
@Builder(toBuilder = true)
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class RedisOutboxRecord {

    private Long id;
    private OutboxEventType eventType;
    private String payload;
    private String uuid;
    private OutboxStatus status;
    private Integer retryCount;
    private LocalDateTime createdAt;
    private LocalDateTime statusUpdatedAt;
}
