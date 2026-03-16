package com.pooli.traffic.service.outbox;

import java.time.Duration;
import java.time.YearMonth;
import java.time.format.DateTimeParseException;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Profile;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.pooli.common.exception.ApplicationException;
import com.pooli.common.exception.CommonErrorCode;
import com.pooli.traffic.domain.dto.request.TrafficPayloadReqDto;
import com.pooli.traffic.domain.enums.TrafficPoolType;
import com.pooli.traffic.domain.outbox.payload.RefillOutboxPayload;
import com.pooli.traffic.mapper.TrafficRefillSourceMapper;
import com.pooli.traffic.service.runtime.TrafficRedisKeyFactory;
import com.pooli.traffic.service.runtime.TrafficRedisRuntimePolicy;

import lombok.RequiredArgsConstructor;

/**
 * 리필 Outbox 공통 처리(멱등키/DB 반납/예외 분류)를 담당하는 서비스입니다.
 */
@Service
@Profile({"local", "api", "traffic"})
@RequiredArgsConstructor
public class TrafficRefillOutboxSupportService {

    private static final long REFILL_IDEMPOTENCY_TTL_SECONDS = 600L;

    @Qualifier("cacheStringRedisTemplate")
    private final StringRedisTemplate cacheStringRedisTemplate;
    private final TrafficRedisKeyFactory trafficRedisKeyFactory;
    private final TrafficRedisRuntimePolicy trafficRedisRuntimePolicy;
    private final TrafficRefillSourceMapper trafficRefillSourceMapper;
    private final RedisOutboxRecordService redisOutboxRecordService;

    /**
     * 리필 UUID 멱등키를 NX 조건으로 등록합니다.
     *
     * @return true면 신규 등록 성공, false면 이미 존재(중복 처리)
     */
    public boolean tryRegisterIdempotency(String uuid) {
        String idempotencyKey = resolveIdempotencyKey(uuid);
        Boolean created = cacheStringRedisTemplate.opsForValue().setIfAbsent(
                idempotencyKey,
                "1",
                Duration.ofSeconds(REFILL_IDEMPOTENCY_TTL_SECONDS)
        );
        return Boolean.TRUE.equals(created);
    }

    /**
     * UUID로 REFILL 멱등키를 계산합니다.
     */
    public String resolveIdempotencyKey(String uuid) {
        return trafficRedisKeyFactory.refillIdempotencyKey(uuid);
    }

    /**
     * REFILL 멱등 TTL(seconds)을 노출합니다.
     */
    public long refillIdempotencyTtlSeconds() {
        return REFILL_IDEMPOTENCY_TTL_SECONDS;
    }

    /**
     * 성공 처리 이후 REFILL 멱등키를 즉시 제거합니다.
     */
    public void clearIdempotency(String uuid) {
        if (uuid == null || uuid.isBlank()) {
            return;
        }
        String idempotencyKey = resolveIdempotencyKey(uuid);
        cacheStringRedisTemplate.delete(idempotencyKey);
    }

    /**
     * claim 결과 정보로 DB 원천 잔량을 반납합니다.
     */
    public void restoreClaimedAmount(TrafficPoolType poolType, TrafficPayloadReqDto payload, long restoreAmount) {
        if (restoreAmount <= 0 || payload == null) {
            return;
        }

        switch (poolType) {
            case INDIVIDUAL -> {
                if (payload.getLineId() != null) {
                    trafficRefillSourceMapper.restoreIndividualRemaining(payload.getLineId(), restoreAmount);
                }
            }
            case SHARED -> {
                if (payload.getFamilyId() != null) {
                    trafficRefillSourceMapper.restoreSharedRemaining(payload.getFamilyId(), restoreAmount);
                }
            }
        }
    }

    /**
     * Outbox payload 기준으로 DB 원천 잔량을 반납합니다.
     */
    public void restoreClaimedAmount(RefillOutboxPayload payload) {
        if (payload == null || payload.getActualRefillAmount() == null || payload.getActualRefillAmount() <= 0) {
            return;
        }

        TrafficPoolType poolType = parsePoolType(payload.getPoolType());
        switch (poolType) {
            case INDIVIDUAL -> {
                if (payload.getLineId() != null) {
                    trafficRefillSourceMapper.restoreIndividualRemaining(payload.getLineId(), payload.getActualRefillAmount());
                }
            }
            case SHARED -> {
                if (payload.getFamilyId() != null) {
                    trafficRefillSourceMapper.restoreSharedRemaining(payload.getFamilyId(), payload.getActualRefillAmount());
                }
            }
        }
    }

    /**
     * REFILL Outbox 보상을 CAS 기반으로 1회만 수행합니다.
     */
    @Transactional
    public void compensateRefillOnce(Long outboxId, RefillOutboxPayload payload) {
        if (outboxId == null || outboxId <= 0) {
            return;
        }
        int updated = redisOutboxRecordService.markRevertIfCompensable(outboxId);
        if (updated == 0) {
            return;
        }
        restoreClaimedAmount(payload);
    }

    /**
     * 실시간 REFILL 경로에서 claim 결과를 CAS 기반으로 1회만 반납합니다.
     */
    @Transactional
    public void compensateRefillOnce(
            Long outboxId,
            TrafficPoolType poolType,
            TrafficPayloadReqDto payload,
            long restoreAmount
    ) {
        if (outboxId == null || outboxId <= 0) {
            return;
        }
        int updated = redisOutboxRecordService.markRevertIfCompensable(outboxId);
        if (updated == 0) {
            return;
        }
        restoreClaimedAmount(poolType, payload, restoreAmount);
    }

    /**
     * payload 정보를 바탕으로 리필 대상 balance key를 계산합니다.
     */
    public String resolveBalanceKey(RefillOutboxPayload payload) {
        if (payload == null) {
            throw new ApplicationException(CommonErrorCode.INTERNAL_SERVER_ERROR, "REFILL payload가 비어 있습니다.");
        }

        YearMonth targetMonth = parseTargetMonth(payload.getTargetMonth());
        TrafficPoolType poolType = parsePoolType(payload.getPoolType());
        return switch (poolType) {
            case INDIVIDUAL -> trafficRedisKeyFactory.remainingIndivAmountKey(payload.getLineId(), targetMonth);
            case SHARED -> trafficRedisKeyFactory.remainingSharedAmountKey(payload.getFamilyId(), targetMonth);
        };
    }

    /**
     * payload 정보로 monthly 만료 시각(epoch seconds)을 계산합니다.
     */
    public long resolveMonthlyExpireAt(RefillOutboxPayload payload) {
        YearMonth targetMonth = parseTargetMonth(payload.getTargetMonth());
        return trafficRedisRuntimePolicy.resolveMonthlyExpireAtEpochSeconds(targetMonth);
    }

    /**
     * 연결 실패 계열 예외인지 판별합니다.
     */
    public boolean isConnectionFailure(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            if (current instanceof RedisConnectionFailureException) {
                return true;
            }

            String className = current.getClass().getSimpleName();
            String message = current.getMessage();
            if (className != null && className.toLowerCase().contains("connection")) {
                return true;
            }
            if (message != null) {
                String normalized = message.toLowerCase();
                if (normalized.contains("connection")
                        || normalized.contains("connect timed out")
                        || normalized.contains("connection refused")
                        || normalized.contains("unable to connect")) {
                    return true;
                }
            }
            current = current.getCause();
        }
        return false;
    }

    /**
     * 타임아웃 계열 예외인지 판별합니다.
     */
    public boolean isTimeoutFailure(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            String className = current.getClass().getSimpleName();
            String message = current.getMessage();

            if (className != null && className.toLowerCase().contains("timeout")) {
                return true;
            }
            if (message != null && message.toLowerCase().contains("timeout")) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    /**
     * DataAccessException을 런타임 예외로 감싼 경우를 펼쳐서 분류할 수 있게 합니다.
     */
    public RuntimeException unwrapRuntimeException(RuntimeException exception) {
        if (exception == null) {
            return new IllegalStateException("unknown redis failure");
        }
        if (exception.getCause() instanceof DataAccessException dataAccessException) {
            return dataAccessException;
        }
        return exception;
    }

    private TrafficPoolType parsePoolType(String poolTypeText) {
        if (poolTypeText == null || poolTypeText.isBlank()) {
            throw new ApplicationException(CommonErrorCode.INTERNAL_SERVER_ERROR, "REFILL poolType이 비어 있습니다.");
        }
        try {
            return TrafficPoolType.valueOf(poolTypeText);
        } catch (IllegalArgumentException e) {
            throw new ApplicationException(CommonErrorCode.INTERNAL_SERVER_ERROR, "REFILL poolType 파싱에 실패했습니다.");
        }
    }

    private YearMonth parseTargetMonth(String targetMonthText) {
        if (targetMonthText == null || targetMonthText.isBlank()) {
            throw new ApplicationException(CommonErrorCode.INTERNAL_SERVER_ERROR, "REFILL targetMonth가 비어 있습니다.");
        }
        try {
            return YearMonth.parse(targetMonthText);
        } catch (DateTimeParseException e) {
            throw new ApplicationException(CommonErrorCode.INTERNAL_SERVER_ERROR, "REFILL targetMonth 파싱에 실패했습니다.");
        }
    }
}
