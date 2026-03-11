package com.pooli.traffic.service;

/**
 * 10-tick 루프를 "tick당 1초 슬롯" 기준으로 맞춰 주는 페이서 인터페이스입니다.
 * 구현체는 현재 tick이 시작돼야 할 시각까지 대기한 뒤, 지연(lag) 시간을 반환합니다.
 */
public interface TrafficTickPacer {

    /**
 * Waits until the scheduled start of the specified tick and returns the observed lag.
 *
 * @param orchestrationStartNano the orchestration start time in nanoseconds (as returned by System.nanoTime())
 * @param tickNumber             the 1-based tick index
 * @return                       the delay in milliseconds between the scheduled tick start and the actual return time; zero or positive
 */
    long awaitTickStart(long orchestrationStartNano, int tickNumber);
}
