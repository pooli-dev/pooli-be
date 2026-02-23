package com.pooli.plan.domain.entity;

import com.pooli.plan.domain.enums.NetworkType;
import lombok.*;

import java.time.LocalDateTime;

@Getter
@Builder
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class Plan {

    private Integer planId;

    private String planCategory;
    private String planName;

    // 무제한일 경우 0
    private Long basicDataAmount;

    private Long sharedPoolAmount;

    // ENUM('LTE', '5G')
    private NetworkType networkType;

    // 단위 KB, 무제한/종량제 요금제는 0
    private Integer qosSpeedLimit;

    private Boolean isUnlimited;

    private LocalDateTime createdAt;
    private LocalDateTime deletedAt;
    private LocalDateTime updatedAt;
}