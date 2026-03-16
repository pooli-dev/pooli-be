package com.pooli.traffic.service.policy;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

import com.pooli.policy.domain.dto.response.ImmediateBlockResDto;
import com.pooli.policy.domain.entity.AppPolicy;
import com.pooli.policy.domain.entity.LineLimit;
import com.pooli.policy.mapper.AppPolicyMapper;
import com.pooli.policy.mapper.ImmediateBlockMapper;
import com.pooli.policy.mapper.LineLimitMapper;
import com.pooli.policy.mapper.RepeatBlockMapper;
import com.pooli.traffic.service.runtime.TrafficLuaScriptInfraService;
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

@ExtendWith(MockitoExtension.class)
class TrafficLinePolicyHydrationServiceTest {

    @Mock
    private StringRedisTemplate cacheStringRedisTemplate;

    @Mock
    private TrafficRedisKeyFactory trafficRedisKeyFactory;

    @Mock
    private TrafficPolicyWriteThroughService trafficPolicyWriteThroughService;

    @Mock
    private LineLimitMapper lineLimitMapper;

    @Mock
    private ImmediateBlockMapper immediateBlockMapper;

    @Mock
    private RepeatBlockMapper repeatBlockMapper;

    @Mock
    private AppPolicyMapper appPolicyMapper;

    @Mock
    private TrafficLuaScriptInfraService trafficLuaScriptInfraService;

    @Mock
    private ValueOperations<String, String> valueOperations;

    @InjectMocks
    private TrafficLinePolicyHydrationService trafficLinePolicyHydrationService;

    @Nested
    @DisplayName("ensureLoaded 테스트")
    class EnsureLoadedTest {

        @Test
        @DisplayName("ready key가 이미 존재하면 DB/Redis 동기화를 생략")
        void skipsWhenReadyKeyExists() {
            // given
            when(trafficRedisKeyFactory.linePolicyReadyKey(11L)).thenReturn("pooli:line_policy_ready:11");
            when(cacheStringRedisTemplate.hasKey("pooli:line_policy_ready:11")).thenReturn(true);

            // when
            assertDoesNotThrow(() -> trafficLinePolicyHydrationService.ensureLoaded(11L));

            // then
            verifyNoInteractions(
                    lineLimitMapper,
                    immediateBlockMapper,
                    repeatBlockMapper,
                    appPolicyMapper,
                    trafficPolicyWriteThroughService,
                    trafficLuaScriptInfraService
            );
            verify(trafficRedisKeyFactory, never()).linePolicyHydrateLockKey(anyLong());
        }

        @Test
        @DisplayName("락 획득 성공 시 스냅샷 적재 후 ready 설정 및 락 해제")
        void hydratesAndSetsReadyWhenLockAcquired() {
            // given
            String readyKey = "pooli:line_policy_ready:11";
            String lockKey = "pooli:line_policy_hydrate_lock:11";
            when(trafficRedisKeyFactory.linePolicyReadyKey(11L)).thenReturn(readyKey);
            when(trafficRedisKeyFactory.linePolicyHydrateLockKey(11L)).thenReturn(lockKey);
            when(cacheStringRedisTemplate.hasKey(readyKey)).thenReturn(false);
            when(cacheStringRedisTemplate.opsForValue()).thenReturn(valueOperations);
            when(valueOperations.setIfAbsent(
                    eq(lockKey),
                    anyString(),
                    eq(Duration.ofMillis(TrafficRedisRuntimePolicy.LOCK_TTL_MS))
            )).thenReturn(true);

            when(lineLimitMapper.getExistLineLimitByLineId(11L))
                    .thenReturn(Optional.of(LineLimit.builder()
                            .lineId(11L)
                            .dailyDataLimit(50_000L)
                            .isDailyLimitActive(true)
                            .sharedDataLimit(90_000L)
                            .isSharedLimitActive(false)
                            .build()));
            when(immediateBlockMapper.selectImmediateBlockPolicy(11L))
                    .thenReturn(ImmediateBlockResDto.builder()
                            .lineId(11L)
                            .blockEndAt(LocalDateTime.of(2026, 3, 12, 10, 0))
                            .build());
            when(repeatBlockMapper.selectRepeatBlocksByLineId(11L)).thenReturn(List.of());
            when(appPolicyMapper.findAllEntityByLineId(11L)).thenReturn(List.of(
                    AppPolicy.builder()
                            .lineId(11L)
                            .applicationId(301)
                            .dataLimit(1024L)
                            .speedLimit(2048)
                            .isActive(true)
                            .isWhitelist(true)
                            .build()
            ));

            // when
            trafficLinePolicyHydrationService.ensureLoaded(11L);

            // then
            verify(trafficPolicyWriteThroughService).syncLineLimitUntracked(
                    eq(11L),
                    eq(50_000L),
                    eq(true),
                    eq(90_000L),
                    eq(false),
                    anyLong()
            );
            verify(trafficPolicyWriteThroughService).syncImmediateBlockEndUntracked(
                    eq(11L),
                    eq(LocalDateTime.of(2026, 3, 12, 10, 0)),
                    anyLong()
            );
            verify(trafficPolicyWriteThroughService).syncRepeatBlockUntracked(eq(11L), eq(List.of()), anyLong());
            verify(trafficPolicyWriteThroughService).syncAppPolicySnapshotUntracked(eq(11L), any(), anyLong());
            verify(valueOperations).set(readyKey, "1", Duration.ofSeconds(60L));
            verify(trafficLuaScriptInfraService).executeLockRelease(eq(lockKey), anyString());
        }

        @Test
        @DisplayName("락 미획득 후 ready가 생성되면 self-hydrate 없이 종료")
        void skipsSelfHydrateWhenReadyAppearsAfterLockMiss() {
            // given
            String readyKey = "pooli:line_policy_ready:11";
            String lockKey = "pooli:line_policy_hydrate_lock:11";
            when(trafficRedisKeyFactory.linePolicyReadyKey(11L)).thenReturn(readyKey);
            when(trafficRedisKeyFactory.linePolicyHydrateLockKey(11L)).thenReturn(lockKey);
            when(cacheStringRedisTemplate.hasKey(readyKey)).thenReturn(false, true);
            when(cacheStringRedisTemplate.opsForValue()).thenReturn(valueOperations);
            when(valueOperations.setIfAbsent(
                    eq(lockKey),
                    anyString(),
                    eq(Duration.ofMillis(TrafficRedisRuntimePolicy.LOCK_TTL_MS))
            )).thenReturn(false);

            // when
            trafficLinePolicyHydrationService.ensureLoaded(11L);

            // then
            verifyNoInteractions(lineLimitMapper, immediateBlockMapper, repeatBlockMapper, appPolicyMapper, trafficPolicyWriteThroughService);
            verify(trafficLuaScriptInfraService, never()).executeLockRelease(anyString(), anyString());
        }

        @Test
        @DisplayName("락 미획득이고 ready도 없으면 self-hydrate 1회 수행")
        void performsSelfHydrateWhenReadyStillMissing() {
            // given
            String readyKey = "pooli:line_policy_ready:11";
            String lockKey = "pooli:line_policy_hydrate_lock:11";
            when(trafficRedisKeyFactory.linePolicyReadyKey(11L)).thenReturn(readyKey);
            when(trafficRedisKeyFactory.linePolicyHydrateLockKey(11L)).thenReturn(lockKey);
            when(cacheStringRedisTemplate.hasKey(readyKey)).thenReturn(false);
            when(cacheStringRedisTemplate.opsForValue()).thenReturn(valueOperations);
            when(valueOperations.setIfAbsent(
                    eq(lockKey),
                    anyString(),
                    eq(Duration.ofMillis(TrafficRedisRuntimePolicy.LOCK_TTL_MS))
            )).thenReturn(false);

            when(lineLimitMapper.getExistLineLimitByLineId(11L)).thenReturn(Optional.empty());
            when(immediateBlockMapper.selectImmediateBlockPolicy(11L)).thenReturn(null);
            when(repeatBlockMapper.selectRepeatBlocksByLineId(11L)).thenReturn(List.of());
            when(appPolicyMapper.findAllEntityByLineId(11L)).thenReturn(List.of());

            // when
            trafficLinePolicyHydrationService.ensureLoaded(11L);

            // then
            verify(trafficPolicyWriteThroughService).syncLineLimitUntracked(
                    eq(11L),
                    eq(-1L),
                    eq(false),
                    eq(-1L),
                    eq(false),
                    anyLong()
            );
            verify(trafficPolicyWriteThroughService).syncImmediateBlockEndUntracked(eq(11L), isNull(), anyLong());
            verify(trafficPolicyWriteThroughService).syncRepeatBlockUntracked(eq(11L), eq(List.of()), anyLong());
            verify(trafficPolicyWriteThroughService).syncAppPolicySnapshotUntracked(eq(11L), eq(List.of()), anyLong());
            verify(valueOperations).set(readyKey, "1", Duration.ofSeconds(60L));
            verify(trafficLuaScriptInfraService, never()).executeLockRelease(anyString(), anyString());
        }

        @Test
        @DisplayName("스냅샷 적재 실패 시 ready는 설정하지 않고 예외를 전파")
        void propagatesExceptionWithoutSettingReadyWhenHydrationFails() {
            // given
            String readyKey = "pooli:line_policy_ready:11";
            String lockKey = "pooli:line_policy_hydrate_lock:11";
            when(trafficRedisKeyFactory.linePolicyReadyKey(11L)).thenReturn(readyKey);
            when(trafficRedisKeyFactory.linePolicyHydrateLockKey(11L)).thenReturn(lockKey);
            when(cacheStringRedisTemplate.hasKey(readyKey)).thenReturn(false);
            when(cacheStringRedisTemplate.opsForValue()).thenReturn(valueOperations);
            when(valueOperations.setIfAbsent(
                    eq(lockKey),
                    anyString(),
                    eq(Duration.ofMillis(TrafficRedisRuntimePolicy.LOCK_TTL_MS))
            )).thenReturn(true);
            when(lineLimitMapper.getExistLineLimitByLineId(11L)).thenThrow(new RuntimeException("db error"));

            // when / then
            assertThrows(RuntimeException.class, () -> trafficLinePolicyHydrationService.ensureLoaded(11L));
            verify(valueOperations, never()).set(eq(readyKey), eq("1"), any(Duration.class));
            verify(trafficLuaScriptInfraService).executeLockRelease(eq(lockKey), anyString());
        }
    }
}
