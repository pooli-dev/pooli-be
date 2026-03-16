package com.pooli.traffic.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pooli.common.config.AppStreamsProperties;
import com.pooli.monitoring.metrics.TrafficRequestMetrics;
import com.pooli.traffic.controller.TrafficController;
import com.pooli.traffic.service.decision.TrafficDeductOrchestratorService;
import com.pooli.traffic.service.invoke.*;
import com.pooli.traffic.service.outbox.RedisOutboxRecordService;
import com.pooli.traffic.service.outbox.TrafficPolicyVersionedRedisService;
import com.pooli.traffic.service.policy.TrafficPolicyWriteThroughService;
import com.pooli.traffic.service.runtime.TrafficInFlightDedupeService;
import com.pooli.traffic.service.runtime.TrafficRedisKeyFactory;
import com.pooli.traffic.service.runtime.TrafficRedisRuntimePolicy;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.core.StringRedisTemplate;

/**
 * 트래픽 관련 핵심 빈의 프로파일 부팅 규칙(local/api/traffic)을 검증하는 테스트입니다.
 * 전체 애플리케이션 부팅 대신, 필요한 빈만 구성해 빠르게 프로파일 활성/비활성 매트릭스를 확인합니다.
 */
class TrafficProfileBootTest {

    // 공통 테스트 컨텍스트:
    // - 프로파일 대상 빈(Controller/Enqueue/Consumer/Reclaim/WriteThrough)을 모두 등록
    // - 의존성은 최소 mock/기본 객체로 채워 프로파일 조건 자체만 검증
    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withUserConfiguration(
                    TrafficController.class,
                    TrafficRequestEnqueueService.class,
                    TrafficStreamConsumerRunner.class,
                    TrafficStreamReclaimService.class,
                    TrafficPolicyWriteThroughService.class,
                    TrafficProfileBootTest.TestBeans.class
            )
            .withBean(AppStreamsProperties.class, AppStreamsProperties::new)
            .withBean(ObjectMapper.class, ObjectMapper::new)
            .withBean("streamsStringRedisTemplate", StringRedisTemplate.class, () -> mock(StringRedisTemplate.class))
            .withBean("cacheStringRedisTemplate", StringRedisTemplate.class, () -> mock(StringRedisTemplate.class))
            .withBean(TrafficStreamInfraService.class, () -> mock(TrafficStreamInfraService.class))
            .withBean(TrafficDeductOrchestratorService.class, () -> mock(TrafficDeductOrchestratorService.class))
            .withBean(TrafficInFlightDedupeService.class, () -> mock(TrafficInFlightDedupeService.class))
            .withBean(TrafficDeductDoneLogService.class, () -> mock(TrafficDeductDoneLogService.class))
            .withBean(TrafficRedisKeyFactory.class, () -> mock(TrafficRedisKeyFactory.class))
            .withBean(TrafficRequestMetrics.class, () -> mock(TrafficRequestMetrics.class))
            .withBean(TrafficPolicyVersionedRedisService.class, () -> mock(TrafficPolicyVersionedRedisService.class))
            .withBean(RedisOutboxRecordService.class, () -> mock(RedisOutboxRecordService.class));

    @Nested
    @DisplayName("프로파일별 빈 활성화 검증")
    class ProfileActivationTest {

        @Test
        @DisplayName("api 프로파일에서는 producer/controller/write-through가 활성화")
        void apiProfileActivatesProducerOnly() {
            contextRunner
                    .withPropertyValues("spring.profiles.active=api")
                    .run(context -> {
                        assertThat(context).hasSingleBean(TrafficController.class);
                        assertThat(context).hasSingleBean(TrafficRequestEnqueueService.class);
                        assertThat(context).hasSingleBean(TrafficPolicyWriteThroughService.class);

                        assertThat(context).doesNotHaveBean(TrafficStreamConsumerRunner.class);
                        assertThat(context).doesNotHaveBean(TrafficStreamReclaimService.class);
                    });
        }

        @Test
        @DisplayName("traffic 프로파일에서는 consumer/reclaim/write-through만 활성화")
        void trafficProfileActivatesConsumerOnly() {
            contextRunner
                    .withPropertyValues("spring.profiles.active=traffic")
                    .run(context -> {
                        assertThat(context).doesNotHaveBean(TrafficController.class);
                        assertThat(context).doesNotHaveBean(TrafficRequestEnqueueService.class);

                        assertThat(context).hasSingleBean(TrafficStreamConsumerRunner.class);
                        assertThat(context).hasSingleBean(TrafficStreamReclaimService.class);
                        assertThat(context).hasSingleBean(TrafficPolicyWriteThroughService.class);
                    });
        }

        @Test
        @DisplayName("local 프로파일에서는 producer/consumer 관련 빈이 모두 활성화")
        void localProfileActivatesAllTrafficBeans() {
            contextRunner
                    .withPropertyValues("spring.profiles.active=local")
                    .run(context -> {
                        assertThat(context).hasSingleBean(TrafficController.class);
                        assertThat(context).hasSingleBean(TrafficRequestEnqueueService.class);
                        assertThat(context).hasSingleBean(TrafficStreamConsumerRunner.class);
                        assertThat(context).hasSingleBean(TrafficStreamReclaimService.class);
                        assertThat(context).hasSingleBean(TrafficPolicyWriteThroughService.class);
                    });
        }
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class TestBeans {

        @Bean
        TrafficRedisRuntimePolicy trafficRedisRuntimePolicy() {
            // write-through 서비스가 어떤 프로파일에서도 안정적으로 주입되도록 테스트 전용 빈을 둔다.
            return mock(TrafficRedisRuntimePolicy.class);
        }
    }
}
