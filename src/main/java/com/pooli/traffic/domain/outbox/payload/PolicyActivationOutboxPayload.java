package com.pooli.traffic.domain.outbox.payload;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * 정책 활성화 동기화 Outbox payload입니다.
 */
@Getter
@Builder
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class PolicyActivationOutboxPayload {
    private Long policyId;
    private Boolean active;
    private Long version;
}
