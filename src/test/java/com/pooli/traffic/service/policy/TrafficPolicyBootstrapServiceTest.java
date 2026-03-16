package com.pooli.traffic.service.policy;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;

import com.pooli.traffic.service.runtime.TrafficRedisKeyFactory;
import com.pooli.traffic.service.runtime.TrafficRedisRuntimePolicy;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.core.script.RedisScript;

import com.pooli.common.exception.ApplicationException;
import com.pooli.policy.domain.dto.response.PolicyActivationSnapshotResDto;
import com.pooli.policy.mapper.PolicyBackOfficeMapper;

@ExtendWith(MockitoExtension.class)
class TrafficPolicyBootstrapServiceTest {

    @Mock
    private StringRedisTemplate cacheStringRedisTemplate;

    @Mock
    private PolicyBackOfficeMapper policyBackOfficeMapper;

    @Mock
    private TrafficRedisKeyFactory trafficRedisKeyFactory;

    @Mock
    private TrafficRedisRuntimePolicy trafficRedisRuntimePolicy;

    @Mock
    private ValueOperations<String, String> valueOperations;

    @InjectMocks
    private TrafficPolicyBootstrapService trafficPolicyBootstrapService;

    @Nested
    @DisplayName("bootstrapOnStartup 테스트")
    class BootstrapOnStartupTest {

        @Test
        @DisplayName("필수 POLICY ID(1~7) 누락 시 fail-fast 예외 발생")
        void throwsWhenRequiredPolicyIdsMissing() {
            // given
            when(policyBackOfficeMapper.selectPolicyActivationSnapshot()).thenReturn(missingWhitelistPolicySnapshots());

            // when & then
            assertThrows(ApplicationException.class, () -> trafficPolicyBootstrapService.bootstrapOnStartup());
        }

        @Test
        @DisplayName("분산락 미획득 시 bootstrap을 스킵하고 정상 종료")
        void skipsWhenBootstrapLockNotAcquired() {
            // given
            String lockKey = "pooli:policy:bootstrap:lock";
            when(policyBackOfficeMapper.selectPolicyActivationSnapshot()).thenReturn(allPolicySnapshots());
            when(trafficRedisKeyFactory.policyBootstrapLockKey()).thenReturn(lockKey);
            when(cacheStringRedisTemplate.opsForValue()).thenReturn(valueOperations);
            when(valueOperations.setIfAbsent(
                    eq(lockKey),
                    anyString(),
                    eq(Duration.ofMillis(30_000L))
            )).thenReturn(false);

            // when
            trafficPolicyBootstrapService.bootstrapOnStartup();

            // then
            verify(cacheStringRedisTemplate, never()).executePipelined(any(org.springframework.data.redis.core.SessionCallback.class));
            verify(cacheStringRedisTemplate, never()).execute(any(RedisScript.class), any(), anyString());
        }

        @Test
        @DisplayName("분산락 획득 시 pipeline 동기화 후 lock 해제")
        void pipelinesAndReleasesLockWhenAcquired() {
            // given
            String lockKey = "pooli:policy:bootstrap:lock";
            when(policyBackOfficeMapper.selectPolicyActivationSnapshot()).thenReturn(allPolicySnapshots());
            when(trafficRedisKeyFactory.policyBootstrapLockKey()).thenReturn(lockKey);
            when(trafficRedisKeyFactory.policyBootstrapVersionKey()).thenReturn("pooli:policy_bootstrap_version");
            when(cacheStringRedisTemplate.opsForValue()).thenReturn(valueOperations);
            when(valueOperations.setIfAbsent(
                    eq(lockKey),
                    anyString(),
                    eq(Duration.ofMillis(30_000L))
            )).thenReturn(true);
            when(trafficRedisRuntimePolicy.zoneId()).thenReturn(ZoneId.of("Asia/Seoul"));
            when(cacheStringRedisTemplate.executePipelined(any(org.springframework.data.redis.core.SessionCallback.class)))
                    .thenReturn(List.of());
            when(cacheStringRedisTemplate.execute(any(RedisScript.class), eq(List.of(lockKey)), anyString()))
                    .thenReturn(1L);

            // when
            trafficPolicyBootstrapService.bootstrapOnStartup();

            // then
            verify(cacheStringRedisTemplate, times(1))
                    .executePipelined(any(org.springframework.data.redis.core.SessionCallback.class));
            verify(cacheStringRedisTemplate, times(1))
                    .execute(any(RedisScript.class), eq(List.of(lockKey)), anyString());
        }
    }

    @Nested
    @DisplayName("reconcilePolicyActivationSnapshot 테스트")
    class ReconcilePolicyActivationSnapshotTest {

        @Test
        @DisplayName("reconciliation 중 예외가 발생해도 스케줄러 메서드는 예외를 전파하지 않음")
        void doesNotThrowWhenReconciliationFails() {
            // given
            when(policyBackOfficeMapper.selectPolicyActivationSnapshot())
                    .thenThrow(new RuntimeException("db temporary error"));

            // when & then
            assertDoesNotThrow(() -> trafficPolicyBootstrapService.reconcilePolicyActivationSnapshot());
        }
    }

    @Nested
    @DisplayName("hydrateOnDemand 테스트")
    class HydrateOnDemandTest {

        @Test
        @DisplayName("필수 POLICY ID 일부 누락이어도 fail-fast 예외 없이 종료")
        void doesNotThrowWhenRequiredPolicyIdsMissing() {
            // given
            when(policyBackOfficeMapper.selectPolicyActivationSnapshot()).thenReturn(missingWhitelistPolicySnapshots());

            // when & then
            assertDoesNotThrow(() -> trafficPolicyBootstrapService.hydrateOnDemand());
        }

        @Test
        @DisplayName("락 획득 시 기존 bootstrap과 동일하게 pipeline 반영 후 lock 해제")
        void pipelinesAndReleasesLockWhenAcquired() {
            // given
            String lockKey = "pooli:policy:bootstrap:lock";
            when(policyBackOfficeMapper.selectPolicyActivationSnapshot()).thenReturn(allPolicySnapshots());
            when(trafficRedisKeyFactory.policyBootstrapLockKey()).thenReturn(lockKey);
            when(trafficRedisKeyFactory.policyBootstrapVersionKey()).thenReturn("pooli:policy_bootstrap_version");
            when(cacheStringRedisTemplate.opsForValue()).thenReturn(valueOperations);
            when(valueOperations.setIfAbsent(
                    eq(lockKey),
                    anyString(),
                    eq(Duration.ofMillis(30_000L))
            )).thenReturn(true);
            when(trafficRedisRuntimePolicy.zoneId()).thenReturn(ZoneId.of("Asia/Seoul"));
            when(cacheStringRedisTemplate.executePipelined(any(org.springframework.data.redis.core.SessionCallback.class)))
                    .thenReturn(List.of());
            when(cacheStringRedisTemplate.execute(any(RedisScript.class), eq(List.of(lockKey)), anyString()))
                    .thenReturn(1L);

            // when
            trafficPolicyBootstrapService.hydrateOnDemand();

            // then
            verify(cacheStringRedisTemplate, times(1))
                    .executePipelined(any(org.springframework.data.redis.core.SessionCallback.class));
            verify(cacheStringRedisTemplate, times(1))
                    .execute(any(RedisScript.class), eq(List.of(lockKey)), anyString());
        }
    }

    private List<PolicyActivationSnapshotResDto> allPolicySnapshots() {
        LocalDateTime now = LocalDateTime.of(2026, 3, 11, 22, 0, 0);
        return List.of(
                snapshot(1, true, now),
                snapshot(2, true, now),
                snapshot(3, true, now),
                snapshot(4, true, now),
                snapshot(5, true, now),
                snapshot(6, true, now),
                snapshot(7, true, now)
        );
    }

    private List<PolicyActivationSnapshotResDto> missingWhitelistPolicySnapshots() {
        LocalDateTime now = LocalDateTime.of(2026, 3, 11, 22, 0, 0);
        return List.of(
                snapshot(1, true, now),
                snapshot(2, true, now),
                snapshot(3, true, now),
                snapshot(4, true, now),
                snapshot(5, true, now),
                snapshot(6, true, now)
        );
    }

    private PolicyActivationSnapshotResDto snapshot(int policyId, boolean isActive, LocalDateTime now) {
        return PolicyActivationSnapshotResDto.builder()
                .policyId(policyId)
                .isActive(isActive)
                .createdAt(now.minusDays(1))
                .updatedAt(now)
                .build();
    }
}
