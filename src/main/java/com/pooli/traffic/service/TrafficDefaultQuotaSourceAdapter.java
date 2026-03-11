package com.pooli.traffic.service;

import java.time.YearMonth;

import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.pooli.traffic.domain.TrafficDbRefillClaimResult;
import com.pooli.traffic.domain.TrafficRefillPlan;
import com.pooli.traffic.domain.dto.request.TrafficPayloadReqDto;
import com.pooli.traffic.domain.enums.TrafficPoolType;
import com.pooli.traffic.mapper.TrafficRefillSourceMapper;

import lombok.RequiredArgsConstructor;

/**
 * HYDRATE/REFILL 원천 데이터를 DB에서 조회/차감하는 기본 어댑터입니다.
 * 리필량은 명세에 따라 `actual=min(requested, dbRemaining)` 계약으로 계산합니다.
 */
@Component
@Profile({"local", "traffic"})
@RequiredArgsConstructor
public class TrafficDefaultQuotaSourceAdapter implements TrafficQuotaSourcePort {

    private final TrafficRefillSourceMapper trafficRefillSourceMapper;
    private final TrafficRecentUsageBucketService trafficRecentUsageBucketService;

    /**
         * Load the initial remaining quota from the database to initialize Redis during hydrate.
         *
         * <p>If the payload lacks identifiers or the stored value is null/negative, the returned value
         * is normalized to zero.
         *
         * @param poolType   the quota pool type (INDIVIDUAL or SHARED)
         * @param payload    request context containing identifiers such as lineId or familyId
         * @param targetMonth month context for the lookup
         * @return the initial remaining amount to use for Redis hydration, normalized to zero or greater
         */
    @Override
    public long loadInitialAmount(TrafficPoolType poolType, TrafficPayloadReqDto payload, YearMonth targetMonth) {
        // hydrate 시점의 원천 잔량을 DB에서 읽어 Redis 초기값으로 사용한다.
        return readRemainingAmount(poolType, payload);
    }

    /**
     * Computes the refill plan (delta, bucket count, refill unit, and threshold) for the specified traffic pool and request context.
     *
     * @param poolType the traffic pool type to compute the plan for
     * @param payload  the request context containing identifiers and usage parameters; may be null
     * @return a TrafficRefillPlan containing delta, bucketCount, refillUnit, and threshold values
     */
    @Override
    public TrafficRefillPlan resolveRefillPlan(TrafficPoolType poolType, TrafficPayloadReqDto payload) {
        return trafficRecentUsageBucketService.resolveRefillPlan(poolType, payload);
    }

    /**
     * Atomically claim (deduct) an available refill amount from the database and return the before/after state.
     *
     * <p>The method normalizes a negative request to zero, locks the relevant DB row to determine the available
     * amount, computes the actual deduction as the smaller of the request and the available amount, attempts the
     * update, and on update failure re-reads the remaining amount to return a conservative result.
     *
     * @param poolType              the traffic pool type to target (e.g., INDIVIDUAL or SHARED)
     * @param payload               request context containing identifiers such as lineId or familyId
     * @param targetMonth           month context used for query key consistency
     * @param requestedRefillAmount the requested refill amount (will be normalized to zero if negative)
     * @return a TrafficDbRefillClaimResult containing the requested amount, DB remaining before the claim,
     *         the actual amount deducted, and the DB remaining after the claim
     */
    @Override
    @Transactional
    public TrafficDbRefillClaimResult claimRefillAmountFromDb(
            TrafficPoolType poolType,
            TrafficPayloadReqDto payload,
            YearMonth targetMonth,
            long requestedRefillAmount
    ) {
        long normalizedRequestedAmount = Math.max(0L, requestedRefillAmount);
        long dbRemainingBefore = normalizePositive(readRemainingAmountForUpdate(poolType, payload));
        if (normalizedRequestedAmount <= 0 || dbRemainingBefore <= 0) {
            return buildClaimResult(normalizedRequestedAmount, dbRemainingBefore, 0L, dbRemainingBefore);
        }

        long actualRefillAmount = Math.min(normalizedRequestedAmount, dbRemainingBefore);
        int updatedRows = deductRemainingAmount(poolType, payload, actualRefillAmount);
        if (updatedRows <= 0) {
            long reloadedRemaining = readRemainingAmount(poolType, payload);
            return buildClaimResult(normalizedRequestedAmount, dbRemainingBefore, 0L, reloadedRemaining);
        }

        long dbRemainingAfter = Math.max(0L, dbRemainingBefore - actualRefillAmount);
        return buildClaimResult(normalizedRequestedAmount, dbRemainingBefore, actualRefillAmount, dbRemainingAfter);
    }

    /**
     * Assembles a TrafficDbRefillClaimResult summarizing a database refill deduction.
     *
     * @param requestedRefillAmount the refill amount requested by the caller
     * @param dbRemainingBefore     the remaining amount recorded in the database before deduction
     * @param actualRefillAmount    the amount actually deducted from the database
     * @param dbRemainingAfter      the remaining amount recorded in the database after deduction
     * @return a TrafficDbRefillClaimResult containing the requested amount, before/after DB remaining amounts, and the actual deducted amount
     */
    private TrafficDbRefillClaimResult buildClaimResult(
            long requestedRefillAmount,
            long dbRemainingBefore,
            long actualRefillAmount,
            long dbRemainingAfter
    ) {
        return TrafficDbRefillClaimResult.builder()
                .requestedRefillAmount(requestedRefillAmount)
                .dbRemainingBefore(dbRemainingBefore)
                .actualRefillAmount(actualRefillAmount)
                .dbRemainingAfter(dbRemainingAfter)
                .build();
    }

    /**
     * Read the current remaining amount from the database without acquiring a lock and normalize it to be zero or greater.
     *
     * @param poolType the traffic pool type (e.g., INDIVIDUAL or SHARED) to determine which remaining amount to read
     * @param payload  the request payload containing identifiers (such as lineId or familyId) used to locate the record
     * @return the remaining amount from the database; if the stored value is null or negative, returns 0
     */
    private long readRemainingAmount(TrafficPoolType poolType, TrafficPayloadReqDto payload) {
        return normalizePositive(readRemainingAmountRaw(poolType, payload));
    }

    /**
     * Perform a locked read (FOR UPDATE) of the current remaining amount for use within a refill transaction.
     *
     * If `payload` is null or the required identifier (lineId for INDIVIDUAL, familyId for SHARED) is missing,
     * the method returns `null` to indicate there is no actionable record to deduct from; callers typically treat
     * `null` as zero available amount.
     *
     * @param poolType the traffic pool type (INDIVIDUAL or SHARED) determining which identifier to use
     * @param payload  the request payload containing identifying information (may be null)
     * @return the remaining amount locked for update, or `null` if the payload or required identifier is absent
     */
    private Long readRemainingAmountForUpdate(TrafficPoolType poolType, TrafficPayloadReqDto payload) {
        if (payload == null) {
            return null;
        }

        return switch (poolType) {
            case INDIVIDUAL -> payload.getLineId() == null
                    ? null
                    : trafficRefillSourceMapper.selectIndividualRemainingForUpdate(payload.getLineId());
            case SHARED -> payload.getFamilyId() == null
                    ? null
                    : trafficRefillSourceMapper.selectSharedRemainingForUpdate(payload.getFamilyId());
        };
    }

    /**
     * Read the raw remaining amount from the database for the given pool and payload without acquiring a lock.
     *
     * <p>The returned value is the raw DB value and is not normalized (may be null or negative); callers must handle normalization and missing identifiers.
     *
     * @param poolType the traffic pool type (INDIVIDUAL or SHARED)
     * @param payload  the request payload containing identifiers used to look up the remaining amount
     * @return the raw remaining amount from the database for the specified pool and payload, or `null` if the payload or required identifier is missing
     */
    private Long readRemainingAmountRaw(TrafficPoolType poolType, TrafficPayloadReqDto payload) {
        if (payload == null) {
            return null;
        }

        return switch (poolType) {
            case INDIVIDUAL -> payload.getLineId() == null
                    ? null
                    : trafficRefillSourceMapper.selectIndividualRemaining(payload.getLineId());
            case SHARED -> payload.getFamilyId() == null
                    ? null
                    : trafficRefillSourceMapper.selectSharedRemaining(payload.getFamilyId());
        };
    }

    /**
     * Execute a database update to deduct remaining quota.
     *
     * If the deduction cannot be applied (deductAmount <= 0, payload is null, or the poolType-specific identifier
     * is missing), no update is performed and the method returns 0.
     *
     * @param poolType the traffic pool type determining which identifier to use (INDIVIDUAL uses lineId, SHARED uses familyId)
     * @param payload  the request payload containing identifiers; may be null
     * @param deductAmount the amount to deduct from the remaining quota
     * @return the number of rows updated in the database (0 indicates no deduction was applied)
     */
    private int deductRemainingAmount(TrafficPoolType poolType, TrafficPayloadReqDto payload, long deductAmount) {
        if (deductAmount <= 0 || payload == null) {
            return 0;
        }

        return switch (poolType) {
            case INDIVIDUAL -> payload.getLineId() == null
                    ? 0
                    : trafficRefillSourceMapper.deductIndividualRemaining(payload.getLineId(), deductAmount);
            case SHARED -> payload.getFamilyId() == null
                    ? 0
                    : trafficRefillSourceMapper.deductSharedRemaining(payload.getFamilyId(), deductAmount);
        };
    }

    /**
     * Normalize a database-derived value to be zero or positive.
     *
     * <p>If the input is null, zero, or negative, returns 0 to prevent negative remaining amounts from propagating.
     *
     * @param value a possibly-null value read from the database
     * @return the original value when greater than zero, otherwise 0
     */
    private long normalizePositive(Long value) {
        if (value == null || value <= 0) {
            return 0L;
        }
        return value;
    }
}
