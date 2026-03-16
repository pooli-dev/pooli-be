package com.pooli.traffic.domain.outbox.payload;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * line_limit 동기화 Outbox payload입니다.
 */
@Getter
@Builder
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class LineLimitOutboxPayload {
    private Long lineId;
    private Long dailyLimit;
    private Boolean isDailyActive;
    private Long sharedLimit;
    private Boolean isSharedActive;
    private Long version;
}
