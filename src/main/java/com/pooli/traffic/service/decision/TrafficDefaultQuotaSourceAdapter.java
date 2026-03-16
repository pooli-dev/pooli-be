package com.pooli.traffic.service.decision;

import java.time.YearMonth;
import java.util.UUID;

import com.pooli.traffic.service.runtime.TrafficRecentUsageBucketService;
import com.pooli.traffic.service.outbox.RedisOutboxRecordService;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.pooli.traffic.domain.TrafficDbRefillClaimResult;
import com.pooli.traffic.domain.TrafficRefillPlan;
import com.pooli.traffic.domain.dto.request.TrafficPayloadReqDto;
import com.pooli.traffic.domain.enums.TrafficPoolType;
import com.pooli.traffic.domain.outbox.OutboxEventType;
import com.pooli.traffic.domain.outbox.payload.RefillOutboxPayload;
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

    private static final long QOS_UPLOAD_MULTIPLIER = 125L;

    private final TrafficRefillSourceMapper trafficRefillSourceMapper;
    private final TrafficRecentUsageBucketService trafficRecentUsageBucketService;
    private final RedisOutboxRecordService redisOutboxRecordService;

    /**
     * 개인풀 잔량 해시에 저장할 QoS 값을 조회합니다.
     * LINE -> PLAN 조인으로 qos_speed_limit 원천값을 읽고, Redis 저장 규격에 맞게 125배로 변환합니다.
     */
    @Override
    public long loadIndividualQosSpeedLimit(TrafficPayloadReqDto payload) {
        if (payload == null || payload.getLineId() == null) {
            return 0L;
        }

        Long rawQosSpeedLimit = trafficRefillSourceMapper.selectIndividualQosSpeedLimit(payload.getLineId());
        long normalizedQosSpeedLimit = normalizeNonNegative(rawQosSpeedLimit);
        return normalizedQosSpeedLimit * QOS_UPLOAD_MULTIPLIER;
    }

    /**
     * 리필 계획(delta, bucketCount, refillUnit, threshold)을 계산해 반환합니다.
     *
     * <p>이 메서드는 DB 계산을 직접 수행하지 않고, 최근 사용량 버킷 집계 서비스에 위임합니다.
     * 즉, "최근 10초 평균 -> fallback -> unit/threshold 계산" 규칙은
     * {@link TrafficRecentUsageBucketService}가 책임집니다.
     *
     * @param poolType 계산 대상 풀 유형
     * @param payload  요청 컨텍스트
     * @return 리필 의사결정에 필요한 계산 결과 묶음
     */
    @Override
    public TrafficRefillPlan resolveRefillPlan(TrafficPoolType poolType, TrafficPayloadReqDto payload) {
        return trafficRecentUsageBucketService.resolveRefillPlan(poolType, payload);
    }

    /**
     * DB 원천 잔량에서 실제 리필 가능량을 "원자적으로" 확보(차감)하고 결과를 반환합니다.
     *
     * <p>핵심 계약:
     * 1) 요청 리필량(requested)을 0 이상으로 보정합니다.
     * 2) `SELECT ... FOR UPDATE`로 현재 잔량을 잠금 조회합니다.
     * 3) 실제 차감량은 `actual = min(requested, dbRemainingBefore)` 입니다.
     * 4) 업데이트 성공 시 DB after 값을 계산해 반환합니다.
     * 5) 업데이트 실패(경합/행 부재 등) 시 잔량을 재조회해 보수적으로 반환합니다.
     * 6) 어떤 경우에도 음수 차감은 허용하지 않습니다.
     *
     * @param poolType             차감 대상 풀 유형
     * @param payload              요청 컨텍스트(lineId/familyId 포함)
     * @param targetMonth          월 기준 파라미터(조회 키 정합성 유지용)
     * @param requestedRefillAmount 요청 리필량(Byte)
     * @return DB 차감 전/후와 실제 차감량을 담은 결과 객체
     */
    @Override
    public TrafficDbRefillClaimResult claimRefillAmountFromDb(
            TrafficPoolType poolType,
            TrafficPayloadReqDto payload,
            YearMonth targetMonth,
            long requestedRefillAmount
    ) {
        return claimRefillAmountFromDb(poolType, payload, targetMonth, requestedRefillAmount, null);
    }

    @Override
    @Transactional
    public TrafficDbRefillClaimResult claimRefillAmountFromDb(
            TrafficPoolType poolType,
            TrafficPayloadReqDto payload,
            YearMonth targetMonth,
            long requestedRefillAmount,
            String refillUuid
    ) {
        long normalizedRequestedAmount = Math.max(0L, requestedRefillAmount);
        long dbRemainingBefore = normalizePositive(readRemainingAmountForUpdate(poolType, payload));
        if (normalizedRequestedAmount <= 0 || dbRemainingBefore <= 0) {
            return buildClaimResult(normalizedRequestedAmount, dbRemainingBefore, 0L, dbRemainingBefore, null, null);
        }

        long actualRefillAmount = Math.min(normalizedRequestedAmount, dbRemainingBefore);
        int updatedRows = deductRemainingAmount(poolType, payload, actualRefillAmount);
        if (updatedRows <= 0) {
            long reloadedRemaining = readRemainingAmount(poolType, payload);
            return buildClaimResult(normalizedRequestedAmount, dbRemainingBefore, 0L, reloadedRemaining, null, null);
        }

        long outboxRecordId = createRefillOutboxRecord(
                poolType,
                payload,
                targetMonth,
                actualRefillAmount,
                refillUuid
        );
        long dbRemainingAfter = Math.max(0L, dbRemainingBefore - actualRefillAmount);
        return buildClaimResult(
                normalizedRequestedAmount,
                dbRemainingBefore,
                actualRefillAmount,
                dbRemainingAfter,
                refillUuid,
                outboxRecordId
        );
    }

    /**
     * DB 리필 차감 결과를 도메인 결과 객체로 일관되게 조립합니다.
     *
     * @param requestedRefillAmount 호출자가 요청한 리필량
     * @param dbRemainingBefore      차감 전 DB 잔량
     * @param actualRefillAmount     실제 차감된 리필량
     * @param dbRemainingAfter       차감 후 DB 잔량
     * @return 표준화된 리필 차감 결과 객체
     */
    private TrafficDbRefillClaimResult buildClaimResult(
            long requestedRefillAmount,
            long dbRemainingBefore,
            long actualRefillAmount,
            long dbRemainingAfter,
            String refillUuid,
            Long outboxRecordId
    ) {
        return TrafficDbRefillClaimResult.builder()
                .requestedRefillAmount(requestedRefillAmount)
                .dbRemainingBefore(dbRemainingBefore)
                .actualRefillAmount(actualRefillAmount)
                .dbRemainingAfter(dbRemainingAfter)
                .refillUuid(refillUuid)
                .outboxRecordId(outboxRecordId)
                .build();
    }

    /**
     * DB 차감 성공 직후 Outbox PENDING 레코드를 같은 트랜잭션에서 생성합니다.
     */
    private long createRefillOutboxRecord(
            TrafficPoolType poolType,
            TrafficPayloadReqDto payload,
            YearMonth targetMonth,
            long actualRefillAmount,
            String refillUuid
    ) {
        String normalizedRefillUuid = normalizeRefillUuid(refillUuid);
        RefillOutboxPayload refillOutboxPayload = RefillOutboxPayload.builder()
                .uuid(normalizedRefillUuid)
                .poolType(poolType.name())
                .lineId(payload == null ? null : payload.getLineId())
                .familyId(payload == null ? null : payload.getFamilyId())
                .targetMonth(targetMonth == null ? null : targetMonth.toString())
                .actualRefillAmount(actualRefillAmount)
                .traceId(payload == null ? null : payload.getTraceId())
                .claimedAtEpochMillis(System.currentTimeMillis())
                .build();

        return redisOutboxRecordService.createPending(OutboxEventType.REFILL, refillOutboxPayload, normalizedRefillUuid);
    }

    /**
     * 리필 멱등 UUID를 검증하고 정규화합니다.
     */
    private String normalizeRefillUuid(String refillUuid) {
        if (refillUuid == null || refillUuid.isBlank()) {
            // 호출자가 UUID를 전달하지 않은 경우(테스트/레거시 경로)는 안전한 랜덤 UUID를 사용한다.
            return UUID.randomUUID().toString();
        }
        return refillUuid.trim();
    }

    /**
     * 일반 조회(잠금 없음)로 현재 DB 잔량을 읽고 0 이상으로 보정합니다.
     *
     * <p>조회 결과가 NULL/음수면 0으로 정규화해 상위 레이어가 안전하게 분기할 수 있게 합니다.
     */
    private long readRemainingAmount(TrafficPoolType poolType, TrafficPayloadReqDto payload) {
        return normalizePositive(readRemainingAmountRaw(poolType, payload));
    }

    /**
     * 리필 차감 트랜잭션에서 사용할 잠금 조회(`FOR UPDATE`)를 수행합니다.
     *
     * <p>payload 또는 식별자(lineId/familyId)가 없으면 조회하지 않고 NULL을 반환합니다.
     * NULL 반환은 상위에서 0으로 보정되어 "차감 불가"로 처리됩니다.
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
     * 일반 조회(잠금 없음)로 현재 DB 잔량 원시값을 읽습니다.
     *
     * <p>이 메서드는 정규화(0 보정)를 하지 않으며, 호출 측에서 정규화를 적용합니다.
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
     * DB 잔량 차감 업데이트를 실행합니다.
     *
     * <p>방어 규칙:
     * 1) 차감량이 0 이하이거나 payload가 없으면 업데이트를 수행하지 않습니다.
     * 2) poolType에 맞는 식별자(lineId/familyId)가 없으면 업데이트를 수행하지 않습니다.
     *
     * @return 업데이트된 행 수(0이면 차감 미적용)
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
     * DB 원시값을 "0 이상" 규칙으로 정규화합니다.
     *
     * <p>NULL/0/음수는 모두 0으로 보정해 상위 로직에서 음수 잔량이 전파되지 않게 합니다.
     */
    private long normalizePositive(Long value) {
        if (value == null || value <= 0) {
            return 0L;
        }
        return value;
    }

    /**
     * QoS 원천값은 0 이상만 허용합니다.
     * null/음수는 비정상 데이터로 간주하고 0으로 보정합니다.
     */
    private long normalizeNonNegative(Long value) {
        if (value == null || value < 0) {
            return 0L;
        }
        return value;
    }
}
