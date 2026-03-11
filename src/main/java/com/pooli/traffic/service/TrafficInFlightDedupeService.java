package com.pooli.traffic.service;

import java.time.Duration;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Profile;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * traceId 기준 in-flight dedupe 선점을 담당하는 서비스입니다.
 * 동일 traceId의 중복 처리/동시 처리 경쟁을 Redis NX+TTL로 완화합니다.
 */
@Slf4j
@Service
@Profile({"local", "traffic"})
@RequiredArgsConstructor
public class TrafficInFlightDedupeService {

    @Qualifier("cacheStringRedisTemplate")
    private final StringRedisTemplate cacheStringRedisTemplate;
    private final TrafficRedisKeyFactory trafficRedisKeyFactory;

    /**
     * Attempt to acquire an in-flight deduplication lock for the given traceId.
     *
     * @param traceId identifier of the trace whose in-flight claim is being requested
     * @return `true` if the claim was acquired and processing may proceed, `false` if a claim already exists
     */
    public boolean tryClaim(String traceId) {
        // traceId에서 dedupe 키를 생성한다. (namespace 포함)
        String dedupeKey = trafficRedisKeyFactory.dedupeRunKey(traceId);

        // SET NX EX(60s)와 동일한 의미:
        // 1) 키가 없을 때만 생성(NX)
        // 2) INFLIGHT_TTL_SEC 이후 자동 만료
        Boolean claimed = cacheStringRedisTemplate.opsForValue().setIfAbsent(
                dedupeKey,
                traceId,
                Duration.ofSeconds(TrafficRedisRuntimePolicy.INFLIGHT_TTL_SEC)
        );

        boolean acquired = Boolean.TRUE.equals(claimed);
        if (!acquired) {
            log.info("traffic_dedupe_claim_skipped traceId={} key={}", traceId, dedupeKey);
        }
        return acquired;
    }

    /**
     * Removes the in-flight deduplication lock associated with the given traceId so future processing can proceed.
     *
     * @param traceId the trace identifier whose dedupe lock should be removed; ignored if null or blank
     */
    public void release(String traceId) {
        // DONE 저장/ACK 완료 후에는 in-flight 키를 정리해 불필요한 키 잔존을 줄인다.
        if (traceId == null || traceId.isBlank()) {
            return;
        }
        String dedupeKey = trafficRedisKeyFactory.dedupeRunKey(traceId);
        cacheStringRedisTemplate.delete(dedupeKey);
    }
}
