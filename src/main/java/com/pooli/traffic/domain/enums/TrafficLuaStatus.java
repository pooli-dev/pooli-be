package com.pooli.traffic.domain.enums;

/**
 * Lua 차감 스크립트가 반환하는 상태 코드를 표현합니다.
 * 오케스트레이터는 이 값을 기준으로 HYDRATE/REFILL/종료 분기를 결정합니다.
 */
public enum TrafficLuaStatus {
    OK,
    NO_BALANCE,
    BLOCKED_IMMEDIATE,
    BLOCKED_REPEAT,
    HIT_DAILY_LIMIT,
    HIT_MONTHLY_SHARED_LIMIT,
    HIT_APP_DAILY_LIMIT,
    HIT_APP_SPEED,
    GLOBAL_POLICY_HYDRATE,
    HYDRATE,
    ERROR
}
