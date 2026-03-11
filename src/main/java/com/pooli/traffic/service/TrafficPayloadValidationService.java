package com.pooli.traffic.service;

import java.util.List;
import java.util.Set;

import org.springframework.stereotype.Component;

import com.pooli.common.dto.Violation;
import com.pooli.traffic.domain.dto.request.TrafficPayloadReqDto;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import lombok.RequiredArgsConstructor;

/**
 * Streams에서 읽은 payload DTO를 Bean Validation으로 검증하고,
 * 검증 실패 정보를 공통 포맷(Violation)으로 변환해 상위 계층에 전달하는 서비스입니다.
 */
@Component
@RequiredArgsConstructor
public class TrafficPayloadValidationService {

    private final Validator validator;

    /**
     * Validate a TrafficPayloadReqDto and produce validation results in a common Violation format.
     *
     * @param payload the payload to validate; if `null`, a single Violation with target "payload",
     *                name "payload", and reason "payload는 필수입니다." is returned
     * @return a list of Violation objects representing validation failures; empty list if there are no violations
     */
    public List<Violation> validate(TrafficPayloadReqDto payload) {
        // payload 자체가 null이면 이후 필드 단위 검증이 불가능하므로
        // 명확한 단일 오류를 반환해 호출 측에서 DLQ/로그 분기를 쉽게 처리한다.
        if (payload == null) {
            return List.of(
                    Violation.builder()
                            .target("payload")
                            .name("payload")
                            .reason("payload는 필수입니다.")
                            .build()
            );
        }

        // Bean Validation 어노테이션(@NotNull, @Positive, ...)을 일괄 실행한다.
        Set<ConstraintViolation<TrafficPayloadReqDto>> violations = validator.validate(payload);

        // 공통 오류 포맷(Violation)으로 변환해 상위 계층에서 일관되게 다룰 수 있게 한다.
        return violations.stream()
                .map(this::toViolation)
                .toList();
    }

    /**
     * Convert a ConstraintViolation for TrafficPayloadReqDto into a Violation DTO.
     *
     * @param violation the constraint violation to convert (may contain a null property path)
     * @return a Violation with target "payload", name set to the violation's property path or "unknown" if absent, and reason set to the violation message
     */
    private Violation toViolation(ConstraintViolation<TrafficPayloadReqDto> violation) {
        String fieldName = violation.getPropertyPath() == null
                ? "unknown"
                : violation.getPropertyPath().toString();

        return Violation.builder()
                .target("payload")
                .name(fieldName)
                .reason(violation.getMessage())
                .build();
    }
}
