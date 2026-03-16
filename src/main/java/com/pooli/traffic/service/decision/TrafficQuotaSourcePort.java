package com.pooli.traffic.service.decision;

import java.time.YearMonth;

import com.pooli.traffic.domain.TrafficDbRefillClaimResult;
import com.pooli.traffic.domain.TrafficRefillPlan;
import com.pooli.traffic.domain.dto.request.TrafficPayloadReqDto;
import com.pooli.traffic.domain.enums.TrafficPoolType;

/**
 * HYDRATE/REFILL 과정에서 필요한 원천 데이터(DB 등) 조회 포트입니다.
 * 현재 단계에서는 기본 구현을 사용하고, 이후 실제 저장소 어댑터로 교체합니다.
 */
public interface TrafficQuotaSourcePort {

    /**
     * 개인풀 잔량 해시에 저장할 QoS 값을 조회합니다.
     *
     * <p>반환값은 Redis에 바로 저장 가능한 최종 값(배율 적용 포함)이어야 합니다.
     *
     * @param payload 요청 컨텍스트(lineId 포함)
     * @return 개인풀 QoS 값(0 이상)
     */
    long loadIndividualQosSpeedLimit(TrafficPayloadReqDto payload);

    /**
     * 리필 여부 판단에 필요한 계산 결과(delta, refillUnit, threshold 등)를 반환합니다.
     *
     * <p>호출 시점:
     * 차감 결과가 NO_BALANCE일 때, 실제 리필 절차(게이트/락/DB 차감/Redis 충전)를 진행하기 전에 사용합니다.
     *
     * @param poolType 대상 풀 유형
     * @param payload  요청 컨텍스트
     * @return 리필 의사결정에 필요한 계획 객체(없으면 null 가능)
     */
    TrafficRefillPlan resolveRefillPlan(TrafficPoolType poolType, TrafficPayloadReqDto payload);

    /**
     * 리필 계획에서 "요청 리필량(refillUnit)"만 안전하게 꺼내는 편의 메서드입니다.
     *
     * <p>방어 규칙:
     * 계획이 없거나 값이 비정상(null/0/음수)이면 0을 반환해 상위 로직이 안전하게 분기하도록 합니다.
     *
     * @param poolType 대상 풀 유형
     * @param payload  요청 컨텍스트
     * @return 요청 리필량(Byte, 없으면 0)
     */
    default long resolveRefillUnit(TrafficPoolType poolType, TrafficPayloadReqDto payload) {
        TrafficRefillPlan refillPlan = resolveRefillPlan(poolType, payload);
        if (refillPlan == null || refillPlan.getRefillUnit() == null || refillPlan.getRefillUnit() <= 0) {
            return 0L;
        }
        return refillPlan.getRefillUnit();
    }

    /**
     * 리필 게이트 판단용 임계치(threshold)만 안전하게 꺼내는 편의 메서드입니다.
     *
     * <p>방어 규칙:
     * 계획이 없거나 값이 비정상(null/0/음수)이면 0을 반환합니다.
     * 실제 게이트 호출 전 최소값 보정(예: 1 이상)은 호출자에서 정책에 맞게 적용합니다.
     *
     * @param poolType 대상 풀 유형
     * @param payload  요청 컨텍스트
     * @return 리필 임계치(Byte, 없으면 0)
     */
    default long resolveRefillThreshold(TrafficPoolType poolType, TrafficPayloadReqDto payload) {
        TrafficRefillPlan refillPlan = resolveRefillPlan(poolType, payload);
        if (refillPlan == null || refillPlan.getThreshold() == null || refillPlan.getThreshold() <= 0) {
            return 0L;
        }
        return refillPlan.getThreshold();
    }

    /**
     * DB 원천 잔량에서 실제 리필 가능량을 차감하고 결과를 반환합니다.
     *
     * <p>호출 시점:
     * 리필 게이트 통과 및 락 획득 이후, Redis에 충전하기 전에 "실제로 확보 가능한 양"을 원자적으로 확정할 때 사용합니다.
     *
     * 계약:
     * - actualRefillAmount = min(requestedRefillAmount, dbRemaining)
     * - Redis 충전은 반드시 actualRefillAmount를 사용해야 합니다.
     *
     * @param poolType 대상 풀 유형
     * @param payload  요청 컨텍스트
     * @param targetMonth 조회/차감 기준 월
     * @param requestedRefillAmount 요청 리필량(Byte)
     * @return DB 차감 전/후 잔량과 실제 차감량을 담은 결과 객체
     */
    default TrafficDbRefillClaimResult claimRefillAmountFromDb(
            TrafficPoolType poolType,
            TrafficPayloadReqDto payload,
            YearMonth targetMonth,
            long requestedRefillAmount
    ) {
        return claimRefillAmountFromDb(poolType, payload, targetMonth, requestedRefillAmount, null);
    }

    TrafficDbRefillClaimResult claimRefillAmountFromDb(
            TrafficPoolType poolType,
            TrafficPayloadReqDto payload,
            YearMonth targetMonth,
            long requestedRefillAmount,
            String refillUuid
    );
}
