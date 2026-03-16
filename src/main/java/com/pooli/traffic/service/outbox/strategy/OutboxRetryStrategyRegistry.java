package com.pooli.traffic.service.outbox.strategy;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import com.pooli.common.exception.ApplicationException;
import com.pooli.common.exception.CommonErrorCode;
import com.pooli.traffic.domain.outbox.OutboxEventType;

/**
 * event_type -> 재시도 전략 매핑 레지스트리입니다.
 */
@Component
@Profile({"local", "api", "traffic"})
public class OutboxRetryStrategyRegistry {

    private final Map<OutboxEventType, OutboxEventRetryStrategy> strategyMap;

    public OutboxRetryStrategyRegistry(List<OutboxEventRetryStrategy> strategies) {
        EnumMap<OutboxEventType, OutboxEventRetryStrategy> registry = new EnumMap<>(OutboxEventType.class);
        for (OutboxEventRetryStrategy strategy : strategies) {
            OutboxEventType eventType = strategy.supports();
            OutboxEventRetryStrategy previous = registry.putIfAbsent(eventType, strategy);
            if (previous != null) {
                throw new ApplicationException(
                        CommonErrorCode.INTERNAL_SERVER_ERROR,
                        "중복 Outbox 전략이 존재합니다. eventType=" + eventType
                );
            }
        }
        this.strategyMap = Map.copyOf(registry);
    }

    /**
     * event_type에 해당하는 전략을 조회합니다.
     */
    public OutboxEventRetryStrategy get(OutboxEventType eventType) {
        OutboxEventRetryStrategy strategy = strategyMap.get(eventType);
        if (strategy == null) {
            throw new ApplicationException(
                    CommonErrorCode.INTERNAL_SERVER_ERROR,
                    "Outbox 전략을 찾을 수 없습니다. eventType=" + eventType
            );
        }
        return strategy;
    }
}
