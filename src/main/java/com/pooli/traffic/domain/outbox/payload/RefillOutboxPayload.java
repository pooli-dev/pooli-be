package com.pooli.traffic.domain.outbox.payload;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * 리필 재처리에 필요한 최소 실행 정보를 담는 Outbox payload입니다.
 */
@Getter
@Builder
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class RefillOutboxPayload {
    private String uuid;
    private String poolType;
    private Long lineId;
    private Long familyId;
    private String targetMonth;
    private Long actualRefillAmount;
    private String traceId;
    private Long claimedAtEpochMillis;
}
