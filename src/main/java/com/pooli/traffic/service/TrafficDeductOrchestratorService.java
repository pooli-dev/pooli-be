package com.pooli.traffic.service;

import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;
import java.util.Set;

import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

import com.pooli.traffic.domain.TrafficLuaExecutionResult;
import com.pooli.traffic.domain.dto.request.TrafficPayloadReqDto;
import com.pooli.traffic.domain.dto.response.TrafficDeductResultResDto;
import com.pooli.traffic.domain.enums.TrafficFinalStatus;
import com.pooli.traffic.domain.enums.TrafficLuaStatus;
import com.pooli.traffic.domain.enums.TrafficPoolType;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * 트래픽 차감 10-tick 오케스트레이션을 담당하는 서비스입니다.
 * 개인풀 우선 차감, NO_BALANCE 시 공유풀 보완 차감, 회복 불가 상태 즉시 종료를 수행합니다.
 */
@Slf4j
@Service
@Profile({"local", "traffic"})
@RequiredArgsConstructor
public class TrafficDeductOrchestratorService {

    private static final int MAX_TICKS = 10;

    private static final Set<TrafficLuaStatus> UNRECOVERABLE_STATUSES = Set.of(
            TrafficLuaStatus.BLOCKED_IMMEDIATE,
            TrafficLuaStatus.BLOCKED_REPEAT,
            TrafficLuaStatus.HIT_DAILY_LIMIT,
            TrafficLuaStatus.HIT_MONTHLY_SHARED_LIMIT,
            TrafficLuaStatus.HIT_APP_DAILY_LIMIT,
            TrafficLuaStatus.ERROR
    );

    private final TrafficHydrateRefillAdapterService trafficHydrateRefillAdapterService;
    private final TrafficRecentUsageBucketService trafficRecentUsageBucketService;
    private final TrafficTickPacer trafficTickPacer;

    /**
     * Orchestrates a multi-tick traffic deduction flow for the provided request payload.
     *
     * <p>The method attempts up to 10 sequential ticks to deduct the requested data, preferring the
     * individual pool and falling back to the shared pool when the individual pool returns
     * `NO_BALANCE`. The orchestration terminates early if an unrecoverable Lua status is observed.
     * Final outcome is resolved as success, partial success, or failed; on unexpected exceptions the
     * final outcome is `FAILED` and the last Lua status is set to `ERROR`.</p>
     *
     * @param payload the request payload containing deduction parameters and a trace identifier
     * @return a TrafficDeductResultResDto summarizing deducted totals, remaining data, final status,
     *         last observed Lua status, and creation/finish timestamps
     */
    public TrafficDeductResultResDto orchestrate(TrafficPayloadReqDto payload) {
        LocalDateTime startedAt = LocalDateTime.now();

        // Step 1) 초기 상태를 안전하게 구성한다.
        long apiTotalData = normalizeNonNegative(payload == null ? null : payload.getApiTotalData());
        long apiRemainingData = apiTotalData;
        long deductedTotalBytes = 0L;
        TrafficLuaStatus lastLuaStatus = null;
        TrafficFinalStatus finalStatus;
        // 동일 요청 내 tick 스케줄 기준 시점은 한 번만 고정합니다.
        // 이후 tick n의 목표 시작 시각은 base + (n-1)*1초 입니다.
        long orchestrationStartNano = System.nanoTime();

        try {
            // Step 2) tick=1..10 순차 루프를 수행한다.
            for (int tick = 1; tick <= MAX_TICKS; tick++) {
                // 이미 전체 목표를 모두 소진했으면 조기 종료한다.
                if (apiRemainingData <= 0) {
                    break;
                }

                // tick당 1초 슬롯을 맞추기 위해 시작 시각까지 대기한다.
                // 처리 지연으로 슬롯이 지난 경우 대기 없이 진행하고 lag만 기록한다.
                long lagMs = trafficTickPacer.awaitTickStart(orchestrationStartNano, tick);
                long tickStartedNano = System.nanoTime();

                int remainingTicks = (MAX_TICKS - tick) + 1;
                long currentTickTargetData = calculateCurrentTickTarget(apiRemainingData, remainingTicks);
                log.debug(
                        "traffic_tick_started traceId={} tick={} lagMs={} currentTickTargetData={} apiRemainingBefore={}",
                        payload == null ? null : payload.getTraceId(),
                        tick,
                        lagMs,
                        currentTickTargetData,
                        apiRemainingData
                );

                // Step 3) 개인풀을 먼저 시도한다.
                TrafficLuaExecutionResult individualResult =
                        trafficHydrateRefillAdapterService.executeIndividualWithRecovery(payload, currentTickTargetData);
                lastLuaStatus = individualResult.getStatus();

                long indivDeducted = normalizeNonNegative(individualResult.getAnswer());
                deductedTotalBytes += indivDeducted;
                apiRemainingData = clampRemaining(apiRemainingData - indivDeducted);
                trafficRecentUsageBucketService.recordTickUsage(TrafficPoolType.INDIVIDUAL, payload, indivDeducted);

                // 개인풀 결과가 회복 불가면 즉시 종료한다.
                if (isUnrecoverableStatus(individualResult.getStatus())) {
                    log.debug(
                            "traffic_orchestrator_unrecoverable_individual traceId={} tick={} status={}",
                            payload == null ? null : payload.getTraceId(),
                            tick,
                            individualResult.getStatus()
                    );
                    break;
                }

                // Step 4) 개인풀 결과가 NO_BALANCE이고 tick 목표가 남았으면 공유풀로 보완한다.
                long tickResidualData = currentTickTargetData - indivDeducted;
                if (tickResidualData > 0 && individualResult.getStatus() == TrafficLuaStatus.NO_BALANCE) {
                    TrafficLuaExecutionResult sharedResult =
                            trafficHydrateRefillAdapterService.executeSharedWithRecovery(payload, tickResidualData);
                    lastLuaStatus = sharedResult.getStatus();

                    long sharedDeducted = normalizeNonNegative(sharedResult.getAnswer());
                    deductedTotalBytes += sharedDeducted;
                    apiRemainingData = clampRemaining(apiRemainingData - sharedDeducted);
                    trafficRecentUsageBucketService.recordTickUsage(TrafficPoolType.SHARED, payload, sharedDeducted);

                    // 공유풀 결과가 회복 불가면 즉시 종료한다.
                    if (isUnrecoverableStatus(sharedResult.getStatus())) {
                        log.debug(
                                "traffic_orchestrator_unrecoverable_shared traceId={} tick={} status={}",
                                payload == null ? null : payload.getTraceId(),
                                tick,
                                sharedResult.getStatus()
                        );
                        break;
                    }
                }

                long processingMs = nanosToMillis(System.nanoTime() - tickStartedNano);
                log.debug(
                        "traffic_tick_completed traceId={} tick={} processingMs={} apiRemainingAfter={} deductedTotalBytes={}",
                        payload == null ? null : payload.getTraceId(),
                        tick,
                        processingMs,
                        apiRemainingData,
                        deductedTotalBytes
                );
            }

            // Step 5) 루프 종료 후 최종 상태를 계산한다.
            finalStatus = resolveFinalStatus(apiRemainingData, lastLuaStatus);
        } catch (Exception e) {
            // 시스템 예외는 FAILED로 고정하고, 마지막 상태는 ERROR로 남긴다.
            log.error("traffic_orchestrator_failed traceId={}", payload == null ? null : payload.getTraceId(), e);
            finalStatus = TrafficFinalStatus.FAILED;
            lastLuaStatus = TrafficLuaStatus.ERROR;
        }

        // Step 6) 공통 결과 DTO를 생성해 상위 단계에서 영속화/ACK 정책에 활용한다.
        return TrafficDeductResultResDto.builder()
                .traceId(payload == null ? null : payload.getTraceId())
                .apiTotalData(apiTotalData)
                .deductedTotalBytes(deductedTotalBytes)
                .apiRemainingData(apiRemainingData)
                .finalStatus(finalStatus)
                .lastLuaStatus(lastLuaStatus)
                .createdAt(startedAt)
                .finishedAt(LocalDateTime.now())
                .build();
    }

    /**
     * Compute the target number of bytes to deduct for the current tick based on remaining data and remaining ticks.
     *
     * @param apiRemainingData the remaining number of bytes to fulfill
     * @param remainingTicks   the number of ticks left (including the current tick)
     * @return the target bytes for the current tick; zero if `apiRemainingData` <= 0 or `remainingTicks` <= 0
     */
    private long calculateCurrentTickTarget(long apiRemainingData, int remainingTicks) {
        // 동적 분배 규칙: ceil(apiRemainingData / remainingTicks)
        // 정수 연산으로 올림을 보장하기 위해 (x + y - 1) / y 형태를 사용한다.
        if (apiRemainingData <= 0 || remainingTicks <= 0) {
            return 0L;
        }
        return (apiRemainingData + remainingTicks - 1) / remainingTicks;
    }

    /**
     * Determine the final traffic deduction status from the remaining data and the last Lua status.
     *
     * @param apiRemainingData the remaining bytes of API data to be deducted
     * @param lastLuaStatus the last observed Lua execution status
     * @return `TrafficFinalStatus.FAILED` if `lastLuaStatus` is `ERROR`; `TrafficFinalStatus.SUCCESS` if `apiRemainingData` is less than or equal to zero; `TrafficFinalStatus.PARTIAL_SUCCESS` otherwise.
     */
    private TrafficFinalStatus resolveFinalStatus(long apiRemainingData, TrafficLuaStatus lastLuaStatus) {
        if (lastLuaStatus == TrafficLuaStatus.ERROR) {
            // Lua ERROR는 시스템/데이터 오류 성격이므로 FAILED로 해석한다.
            return TrafficFinalStatus.FAILED;
        }
        if (apiRemainingData <= 0) {
            return TrafficFinalStatus.SUCCESS;
        }
        return TrafficFinalStatus.PARTIAL_SUCCESS;
    }

    /**
     * Determine whether a Lua status is considered unrecoverable.
     *
     * @param status the Lua status to check
     * @return `true` if the status is one of the unrecoverable statuses, `false` otherwise
     */
    private boolean isUnrecoverableStatus(TrafficLuaStatus status) {
        return status != null && UNRECOVERABLE_STATUSES.contains(status);
    }

    /**
     * Clamp the given value to zero if it is less than or equal to zero.
     *
     * @param value the value to clamp
     * @return the original value if greater than zero, otherwise 0
     */
    private long clampRemaining(long value) {
        if (value <= 0) {
            return 0L;
        }
        return value;
    }

    /**
     * Normalize a possibly null or non-positive Long into a safe non-negative primitive long.
     *
     * @param value the input Long; if null or less than or equal to zero it is treated as zero
     * @return `0` if value is null or less than or equal to zero, otherwise the long value
     */
    private long normalizeNonNegative(Long value) {
        if (value == null || value <= 0) {
            return 0L;
        }
        return value;
    }

    /**
     * Convert a duration from nanoseconds to milliseconds.
     *
     * @param nanos duration in nanoseconds
     * @return the equivalent duration in milliseconds, or 0 if `nanos` is less than or equal to 0
     */
    private long nanosToMillis(long nanos) {
        if (nanos <= 0) {
            return 0L;
        }
        return TimeUnit.NANOSECONDS.toMillis(nanos);
    }
}
