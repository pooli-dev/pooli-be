package com.pooli.traffic.service;

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
 * Loads the initial available amount from the source for the specified pool, payload, and target month.
 *
 * @param poolType    the type of traffic pool to query
 * @param payload     request payload identifying the customer/context for which to load the amount
 * @param targetMonth the YearMonth for which the initial amount should be retrieved
 * @return the initial available amount for the given pool and target month as a long value
 */
long loadInitialAmount(TrafficPoolType poolType, TrafficPayloadReqDto payload, YearMonth targetMonth);

    /**
 * Determines the refill plan applicable for the given traffic pool and request payload.
 *
 * @param poolType the traffic pool type to resolve the plan for
 * @param payload  request payload containing context used to select the refill plan
 * @return the matching TrafficRefillPlan, or null if no plan applies
 */
TrafficRefillPlan resolveRefillPlan(TrafficPoolType poolType, TrafficPayloadReqDto payload);

    /**
     * Retrieves the refill unit from the resolved refill plan for the given pool and payload.
     *
     * @returns the refill unit from the plan, or 0 if no plan exists or the unit is null or less than or equal to 0
     */
    default long resolveRefillUnit(TrafficPoolType poolType, TrafficPayloadReqDto payload) {
        TrafficRefillPlan refillPlan = resolveRefillPlan(poolType, payload);
        if (refillPlan == null || refillPlan.getRefillUnit() == null || refillPlan.getRefillUnit() <= 0) {
            return 0L;
        }
        return refillPlan.getRefillUnit();
    }

    /**
     * Resolve the refill threshold for the specified traffic pool and payload.
     *
     * @param poolType the traffic pool type for which to resolve the threshold
     * @param payload  the request payload containing context used to resolve the refill plan
     * @return the refill threshold in units; 0 if there is no refill plan or the plan's threshold is missing or less than or equal to 0
     */
    default long resolveRefillThreshold(TrafficPoolType poolType, TrafficPayloadReqDto payload) {
        TrafficRefillPlan refillPlan = resolveRefillPlan(poolType, payload);
        if (refillPlan == null || refillPlan.getThreshold() == null || refillPlan.getThreshold() <= 0) {
            return 0L;
        }
        return refillPlan.getThreshold();
    }

    /**
     * Claims (deducts) the actual refillable amount from the DB source balance for the given pool, payload, and month.
     *
     * The actual amount charged is the lesser of the requested refill amount and the DB remaining balance.
     * Contract:
     * - actualRefillAmount = min(requestedRefillAmount, dbRemaining)
     * - Any downstream charging (e.g., Redis) must use the `actualRefillAmount`.
     *
     * @param poolType the traffic pool type to operate on
     * @param payload  the request payload containing tenant/context information
     * @param targetMonth the YearMonth for which the refill is claimed
     * @param requestedRefillAmount the desired amount to refill; the method will charge at most this amount
     * @return a TrafficDbRefillClaimResult describing the outcome, including the actual amount claimed and remaining balances
     */
    TrafficDbRefillClaimResult claimRefillAmountFromDb(
            TrafficPoolType poolType,
            TrafficPayloadReqDto payload,
            YearMonth targetMonth,
            long requestedRefillAmount
    );
}
