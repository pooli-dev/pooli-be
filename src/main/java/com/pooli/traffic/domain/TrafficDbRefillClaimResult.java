package com.pooli.traffic.domain;

import lombok.Builder;
import lombok.Getter;

/**
 * DB 원천 잔량 차감 결과를 담는 값 객체입니다.
 */
@Getter
@Builder
public class TrafficDbRefillClaimResult {

    private final Long requestedRefillAmount;
    private final Long dbRemainingBefore;
    private final Long actualRefillAmount;
    private final Long dbRemainingAfter;
    private final String refillUuid;
    private final Long outboxRecordId;
}
