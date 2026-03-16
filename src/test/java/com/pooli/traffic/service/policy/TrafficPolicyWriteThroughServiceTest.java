package com.pooli.traffic.service.policy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.pooli.policy.domain.dto.response.RepeatBlockDayResDto;
import com.pooli.policy.domain.dto.response.RepeatBlockPolicyResDto;
import com.pooli.policy.domain.entity.AppPolicy;
import com.pooli.policy.domain.enums.DayOfWeek;
import com.pooli.traffic.domain.outbox.OutboxEventType;
import com.pooli.traffic.domain.outbox.payload.AppPolicyOutboxPayload;
import com.pooli.traffic.domain.outbox.payload.LineLimitOutboxPayload;
import com.pooli.traffic.domain.outbox.payload.LineScopedOutboxPayload;
import com.pooli.traffic.domain.outbox.payload.PolicyActivationOutboxPayload;
import com.pooli.traffic.service.outbox.PolicySyncResult;
import com.pooli.traffic.service.outbox.RedisOutboxRecordService;
import com.pooli.traffic.service.outbox.TrafficPolicyVersionedRedisService;
import com.pooli.traffic.service.runtime.TrafficRedisKeyFactory;
import com.pooli.traffic.service.runtime.TrafficRedisRuntimePolicy;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * 정책 write-through가 Outbox + CAS 규칙으로 동작하는지 검증합니다.
 */
@ExtendWith(MockitoExtension.class)
class TrafficPolicyWriteThroughServiceTest {

    @Mock
    private TrafficRedisKeyFactory trafficRedisKeyFactory;

    @Mock
    private TrafficRedisRuntimePolicy trafficRedisRuntimePolicy;

    @Mock
    private TrafficPolicyVersionedRedisService trafficPolicyVersionedRedisService;

    @Mock
    private RedisOutboxRecordService redisOutboxRecordService;

    @InjectMocks
    private TrafficPolicyWriteThroughService trafficPolicyWriteThroughService;

    @Nested
    @DisplayName("syncLineLimit 테스트")
    class SyncLineLimitTest {

        @Test
        @DisplayName("활성 상태에 맞춰 daily/shared limit를 Hash CAS로 갱신")
        void writesDailyAndSharedLimitValuesViaCas() {
            // given
            when(redisOutboxRecordService.createPending(eq(OutboxEventType.SYNC_LINE_LIMIT), any(), isNull())).thenReturn(11L);
            when(trafficRedisKeyFactory.dailyTotalLimitKey(101L)).thenReturn("pooli:daily_total_limit:101");
            when(trafficRedisKeyFactory.monthlySharedLimitKey(101L)).thenReturn("pooli:monthly_shared_limit:101");
            when(trafficPolicyVersionedRedisService.syncVersionedValue(anyString(), anyString(), anyLong()))
                    .thenReturn(PolicySyncResult.SUCCESS);

            // when
            trafficPolicyWriteThroughService.syncLineLimit(101L, 50_000L, true, 90_000L, false);

            // then
            verify(redisOutboxRecordService).createPending(
                    eq(OutboxEventType.SYNC_LINE_LIMIT),
                    argThat(payload -> {
                        if (!(payload instanceof LineLimitOutboxPayload linePayload)) {
                            return false;
                        }
                        return linePayload.getLineId() != null
                                && linePayload.getLineId() == 101L
                                && linePayload.getDailyLimit() != null
                                && linePayload.getDailyLimit() == 50_000L
                                && Boolean.TRUE.equals(linePayload.getIsDailyActive())
                                && linePayload.getSharedLimit() != null
                                && linePayload.getSharedLimit() == 90_000L
                                && Boolean.FALSE.equals(linePayload.getIsSharedActive())
                                && linePayload.getVersion() != null
                                && linePayload.getVersion() > 0;
                    }),
                    isNull()
            );

            ArgumentCaptor<Long> versionCaptor = ArgumentCaptor.forClass(Long.class);
            verify(trafficPolicyVersionedRedisService).syncVersionedValue(
                    eq("pooli:daily_total_limit:101"),
                    eq("50000"),
                    versionCaptor.capture()
            );
            verify(trafficPolicyVersionedRedisService).syncVersionedValue(
                    eq("pooli:monthly_shared_limit:101"),
                    eq("-1"),
                    versionCaptor.capture()
            );

            List<Long> versions = versionCaptor.getAllValues();
            assertEquals(2, versions.size());
            assertEquals(versions.get(0), versions.get(1));
            assertTrue(versions.get(0) > 0);
            verify(redisOutboxRecordService).markSuccess(11L);
            verify(redisOutboxRecordService, never()).markFail(anyLong());
        }
    }

    @Nested
    @DisplayName("syncAppPolicy 테스트")
    class SyncAppPolicyTest {

        @Test
        @DisplayName("활성 정책이면 속도 제한값을 125배로 변환해 CAS로 반영")
        void writesSpeedLimitWithMultiplier() {
            // given
            when(redisOutboxRecordService.createPending(eq(OutboxEventType.SYNC_APP_POLICY), any(), isNull())).thenReturn(12L);
            when(trafficRedisKeyFactory.appDataDailyLimitKey(101L)).thenReturn("pooli:app_data_daily_limit:101");
            when(trafficRedisKeyFactory.appSpeedLimitKey(101L)).thenReturn("pooli:app_speed_limit:101");
            when(trafficRedisKeyFactory.appWhitelistKey(101L)).thenReturn("pooli:app_whitelist:101");
            when(trafficPolicyVersionedRedisService.syncAppPolicySingle(
                    anyString(), anyString(), anyString(), anyInt(), anyBoolean(), anyLong(), anyInt(), anyBoolean(), anyLong()
            )).thenReturn(PolicySyncResult.SUCCESS);

            // when
            trafficPolicyWriteThroughService.syncAppPolicy(101L, 301, true, 1_000L, 20, false);

            // then
            verify(redisOutboxRecordService).createPending(
                    eq(OutboxEventType.SYNC_APP_POLICY),
                    argThat(payload -> {
                        if (!(payload instanceof AppPolicyOutboxPayload appPayload)) {
                            return false;
                        }
                        return appPayload.getLineId() != null
                                && appPayload.getLineId() == 101L
                                && appPayload.getAppId() != null
                                && appPayload.getAppId() == 301
                                && appPayload.getVersion() != null
                                && appPayload.getVersion() > 0;
                    }),
                    isNull()
            );

            verify(trafficPolicyVersionedRedisService).syncAppPolicySingle(
                    eq("pooli:app_data_daily_limit:101"),
                    eq("pooli:app_speed_limit:101"),
                    eq("pooli:app_whitelist:101"),
                    eq(301),
                    eq(true),
                    eq(1_000L),
                    eq(2_500),
                    eq(false),
                    anyLong()
            );
            verify(redisOutboxRecordService).markSuccess(12L);
        }

        @Test
        @DisplayName("속도 제한이 무제한(-1)이면 센티널 값을 그대로 전달")
        void keepsUnlimitedSpeedSentinel() {
            // given
            when(redisOutboxRecordService.createPending(eq(OutboxEventType.SYNC_APP_POLICY), any(), isNull())).thenReturn(13L);
            when(trafficRedisKeyFactory.appDataDailyLimitKey(101L)).thenReturn("pooli:app_data_daily_limit:101");
            when(trafficRedisKeyFactory.appSpeedLimitKey(101L)).thenReturn("pooli:app_speed_limit:101");
            when(trafficRedisKeyFactory.appWhitelistKey(101L)).thenReturn("pooli:app_whitelist:101");
            when(trafficPolicyVersionedRedisService.syncAppPolicySingle(
                    anyString(), anyString(), anyString(), anyInt(), anyBoolean(), anyLong(), anyInt(), anyBoolean(), anyLong()
            )).thenReturn(PolicySyncResult.SUCCESS);

            // when
            trafficPolicyWriteThroughService.syncAppPolicy(101L, 301, true, 1_000L, -1, false);

            // then
            verify(trafficPolicyVersionedRedisService).syncAppPolicySingle(
                    eq("pooli:app_data_daily_limit:101"),
                    eq("pooli:app_speed_limit:101"),
                    eq("pooli:app_whitelist:101"),
                    eq(301),
                    eq(true),
                    eq(1_000L),
                    eq(-1),
                    eq(false),
                    anyLong()
            );
            verify(redisOutboxRecordService).markSuccess(13L);
        }
    }

    @Nested
    @DisplayName("syncAppPolicySnapshot 테스트")
    class SyncAppPolicySnapshotTest {

        @Test
        @DisplayName("line 단위 app 정책 스냅샷을 active 정책만으로 재작성")
        void rebuildsAppPolicySnapshotWithActivePoliciesOnly() {
            // given
            when(redisOutboxRecordService.createPending(eq(OutboxEventType.SYNC_APP_POLICY_SNAPSHOT), any(), isNull())).thenReturn(14L);
            when(trafficRedisKeyFactory.appDataDailyLimitKey(101L)).thenReturn("pooli:app_data_daily_limit:101");
            when(trafficRedisKeyFactory.appSpeedLimitKey(101L)).thenReturn("pooli:app_speed_limit:101");
            when(trafficRedisKeyFactory.appWhitelistKey(101L)).thenReturn("pooli:app_whitelist:101");
            when(trafficPolicyVersionedRedisService.syncAppPolicySnapshot(
                    anyString(), anyString(), anyString(), any(), any(), any(), anyLong()
            )).thenReturn(PolicySyncResult.SUCCESS);

            AppPolicy activeWhitelisted = AppPolicy.builder()
                    .lineId(101L)
                    .applicationId(301)
                    .dataLimit(1_024L)
                    .speedLimit(2_048)
                    .isActive(true)
                    .isWhitelist(true)
                    .build();
            AppPolicy inactive = AppPolicy.builder()
                    .lineId(101L)
                    .applicationId(302)
                    .dataLimit(333L)
                    .speedLimit(444)
                    .isActive(false)
                    .isWhitelist(true)
                    .build();

            // when
            trafficPolicyWriteThroughService.syncAppPolicySnapshot(101L, List.of(activeWhitelisted, inactive));

            // then
            verify(redisOutboxRecordService).createPending(
                    eq(OutboxEventType.SYNC_APP_POLICY_SNAPSHOT),
                    argThat(payload -> {
                        if (!(payload instanceof LineScopedOutboxPayload linePayload)) {
                            return false;
                        }
                        return linePayload.getLineId() != null
                                && linePayload.getLineId() == 101L
                                && linePayload.getVersion() != null
                                && linePayload.getVersion() > 0;
                    }),
                    isNull()
            );

            verify(trafficPolicyVersionedRedisService).syncAppPolicySnapshot(
                    eq("pooli:app_data_daily_limit:101"),
                    eq("pooli:app_speed_limit:101"),
                    eq("pooli:app_whitelist:101"),
                    eq(Map.of("limit:301", "1024")),
                    eq(Map.of("speed:301", "256000")),
                    eq(Set.of("301")),
                    anyLong()
            );
            verify(redisOutboxRecordService).markSuccess(14L);
        }

        @Test
        @DisplayName("활성 정책이 없으면 빈 스냅샷으로 동기화")
        void writesEmptySnapshotWhenNoActivePolicies() {
            // given
            when(redisOutboxRecordService.createPending(eq(OutboxEventType.SYNC_APP_POLICY_SNAPSHOT), any(), isNull())).thenReturn(15L);
            when(trafficRedisKeyFactory.appDataDailyLimitKey(101L)).thenReturn("pooli:app_data_daily_limit:101");
            when(trafficRedisKeyFactory.appSpeedLimitKey(101L)).thenReturn("pooli:app_speed_limit:101");
            when(trafficRedisKeyFactory.appWhitelistKey(101L)).thenReturn("pooli:app_whitelist:101");
            when(trafficPolicyVersionedRedisService.syncAppPolicySnapshot(
                    anyString(), anyString(), anyString(), any(), any(), any(), anyLong()
            )).thenReturn(PolicySyncResult.SUCCESS);

            AppPolicy inactive = AppPolicy.builder()
                    .lineId(101L)
                    .applicationId(302)
                    .isActive(false)
                    .build();

            // when
            trafficPolicyWriteThroughService.syncAppPolicySnapshot(101L, List.of(inactive));

            // then
            verify(trafficPolicyVersionedRedisService).syncAppPolicySnapshot(
                    eq("pooli:app_data_daily_limit:101"),
                    eq("pooli:app_speed_limit:101"),
                    eq("pooli:app_whitelist:101"),
                    eq(Map.of()),
                    eq(Map.of()),
                    eq(Set.of()),
                    anyLong()
            );
            verify(redisOutboxRecordService).markSuccess(15L);
        }
    }

    @Nested
    @DisplayName("syncRepeatBlock 테스트")
    class SyncRepeatBlockTest {

        @Test
        @DisplayName("활성 repeat block 목록을 day hash 규격으로 변환해 저장")
        void writesRepeatBlockHash() {
            // given
            when(redisOutboxRecordService.createPending(eq(OutboxEventType.SYNC_REPEAT_BLOCK), any(), isNull())).thenReturn(16L);
            when(trafficRedisKeyFactory.repeatBlockKey(101L)).thenReturn("pooli:repeat_block:101");
            when(trafficPolicyVersionedRedisService.syncRepeatBlockSnapshot(anyString(), any(), anyLong()))
                    .thenReturn(PolicySyncResult.SUCCESS);

            RepeatBlockDayResDto day = RepeatBlockDayResDto.builder()
                    .dayOfWeek(DayOfWeek.MON)
                    .startAt(LocalTime.of(1, 0, 0))
                    .endAt(LocalTime.of(2, 0, 0))
                    .build();
            RepeatBlockPolicyResDto repeatBlock = RepeatBlockPolicyResDto.builder()
                    .repeatBlockId(77L)
                    .lineId(101L)
                    .isActive(true)
                    .days(List.of(day))
                    .build();

            // when
            trafficPolicyWriteThroughService.syncRepeatBlock(101L, List.of(repeatBlock));

            // then
            verify(trafficPolicyVersionedRedisService).syncRepeatBlockSnapshot(
                    eq("pooli:repeat_block:101"),
                    eq(Map.of("day:1:77", "3600:7200")),
                    anyLong()
            );
            verify(redisOutboxRecordService).markSuccess(16L);
        }
    }

    @Nested
    @DisplayName("재시도/실패 처리 테스트")
    class RetryTest {

        @Test
        @DisplayName("첫 시도 RETRYABLE 실패면 재시도 후 성공 처리")
        void retriesAndSucceeds() {
            // given
            when(redisOutboxRecordService.createPending(eq(OutboxEventType.SYNC_POLICY_ACTIVATION), any(), isNull())).thenReturn(17L);
            when(trafficRedisKeyFactory.policyKey(1001L)).thenReturn("pooli:policy:1001");
            when(trafficPolicyVersionedRedisService.syncVersionedValue(eq("pooli:policy:1001"), eq("1"), anyLong()))
                    .thenReturn(PolicySyncResult.RETRYABLE_FAILURE)
                    .thenReturn(PolicySyncResult.SUCCESS);

            // when
            trafficPolicyWriteThroughService.syncPolicyActivation(1001L, true);

            // then
            verify(trafficPolicyVersionedRedisService, times(2)).syncVersionedValue(
                    eq("pooli:policy:1001"),
                    eq("1"),
                    anyLong()
            );
            verify(redisOutboxRecordService).markSuccess(17L);
            verify(redisOutboxRecordService, never()).markFail(anyLong());
        }

        @Test
        @DisplayName("RETRYABLE 실패가 계속되면 FAIL로 남긴다")
        void marksFailWhenRetryableFailureExhausted() {
            // given
            when(redisOutboxRecordService.createPending(eq(OutboxEventType.SYNC_POLICY_ACTIVATION), any(), isNull())).thenReturn(18L);
            when(trafficRedisKeyFactory.policyKey(1001L)).thenReturn("pooli:policy:1001");
            when(trafficPolicyVersionedRedisService.syncVersionedValue(eq("pooli:policy:1001"), eq("1"), anyLong()))
                    .thenReturn(PolicySyncResult.RETRYABLE_FAILURE);

            // when
            trafficPolicyWriteThroughService.syncPolicyActivation(1001L, true);

            // then
            verify(trafficPolicyVersionedRedisService, times(3)).syncVersionedValue(
                    eq("pooli:policy:1001"),
                    eq("1"),
                    anyLong()
            );
            verify(redisOutboxRecordService).markFail(18L);
            verify(redisOutboxRecordService, never()).markSuccess(anyLong());
        }

        @Test
        @DisplayName("즉시 차단 시간은 Asia/Seoul epoch second 문자열로 저장")
        void storesImmediateBlockAsEpochSecond() {
            // given
            when(redisOutboxRecordService.createPending(eq(OutboxEventType.SYNC_IMMEDIATE_BLOCK), any(), isNull())).thenReturn(19L);
            when(trafficRedisKeyFactory.immediatelyBlockEndKey(101L)).thenReturn("pooli:immediately_block_end:101");
            when(trafficRedisRuntimePolicy.zoneId()).thenReturn(ZoneId.of("Asia/Seoul"));
            when(trafficPolicyVersionedRedisService.syncVersionedValue(anyString(), anyString(), anyLong()))
                    .thenReturn(PolicySyncResult.SUCCESS);

            LocalDateTime blockEndAt = LocalDateTime.of(2026, 3, 11, 12, 0, 0);
            long expectedEpochSecond = blockEndAt.atZone(ZoneId.of("Asia/Seoul")).toEpochSecond();

            // when
            trafficPolicyWriteThroughService.syncImmediateBlockEnd(101L, blockEndAt);

            // then
            verify(redisOutboxRecordService).createPending(
                    eq(OutboxEventType.SYNC_IMMEDIATE_BLOCK),
                    argThat(payload -> {
                        if (!(payload instanceof com.pooli.traffic.domain.outbox.payload.ImmediateBlockOutboxPayload immediatePayload)) {
                            return false;
                        }
                        return immediatePayload.getLineId() != null
                                && immediatePayload.getLineId() == 101L
                                && immediatePayload.getBlockEndEpochSecond() != null
                                && immediatePayload.getBlockEndEpochSecond() == expectedEpochSecond;
                    }),
                    isNull()
            );
            verify(trafficPolicyVersionedRedisService).syncVersionedValue(
                    eq("pooli:immediately_block_end:101"),
                    eq(String.valueOf(expectedEpochSecond)),
                    anyLong()
            );
            verify(redisOutboxRecordService).markSuccess(19L);
        }

        @Test
        @DisplayName("connection failure는 실패로 처리해 markFail")
        void treatsConnectionFailureAsFail() {
            // given
            when(redisOutboxRecordService.createPending(eq(OutboxEventType.SYNC_POLICY_ACTIVATION), any(), isNull())).thenReturn(20L);
            when(trafficRedisKeyFactory.policyKey(2002L)).thenReturn("pooli:policy:2002");
            when(trafficPolicyVersionedRedisService.syncVersionedValue(eq("pooli:policy:2002"), eq("0"), anyLong()))
                    .thenReturn(PolicySyncResult.CONNECTION_FAILURE);

            // when
            trafficPolicyWriteThroughService.syncPolicyActivation(2002L, false);

            // then
            verify(redisOutboxRecordService).createPending(
                    eq(OutboxEventType.SYNC_POLICY_ACTIVATION),
                    argThat(payload -> {
                        if (!(payload instanceof PolicyActivationOutboxPayload activationPayload)) {
                            return false;
                        }
                        return activationPayload.getPolicyId() != null
                                && activationPayload.getPolicyId() == 2002L
                                && Boolean.FALSE.equals(activationPayload.getActive());
                    }),
                    isNull()
            );
            verify(redisOutboxRecordService).markFail(20L);
            verify(redisOutboxRecordService, never()).markSuccess(anyLong());
        }
    }
}
