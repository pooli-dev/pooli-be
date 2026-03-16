package com.pooli.traffic.domain.outbox.payload;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * 앱 정책 단건 동기화 Outbox payload입니다.
 */
@Getter
@Builder
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class AppPolicyOutboxPayload {
    private Long lineId;
    private Integer appId;
    private Long version;
}
