package com.pooli.traffic.service.decision;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;

import com.pooli.traffic.service.policy.TrafficLinePolicyHydrationService;
import com.pooli.traffic.service.policy.TrafficPolicyBootstrapService;
import com.pooli.traffic.service.outbox.RedisOutboxRecordService;
import com.pooli.traffic.service.outbox.TrafficRefillOutboxSupportService;
import com.pooli.traffic.service.runtime.TrafficLuaScriptInfraService;
import com.pooli.traffic.service.runtime.TrafficQuotaCacheService;
import com.pooli.traffic.service.runtime.TrafficRedisKeyFactory;
import com.pooli.traffic.service.runtime.TrafficRedisRuntimePolicy;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.test.util.ReflectionTestUtils;

import com.pooli.monitoring.metrics.TrafficHydrateMetrics;
import com.pooli.monitoring.metrics.TrafficRefillMetrics;
import com.pooli.traffic.domain.TrafficDbRefillClaimResult;
import com.pooli.traffic.domain.TrafficLuaExecutionResult;
import com.pooli.traffic.domain.TrafficRefillPlan;
import com.pooli.traffic.domain.dto.request.TrafficPayloadReqDto;
import com.pooli.traffic.domain.enums.TrafficLuaStatus;
import com.pooli.traffic.domain.enums.TrafficPoolType;
import com.pooli.traffic.domain.enums.TrafficRefillGateStatus;

@ExtendWith(MockitoExtension.class)
class TrafficHydrateRefillAdapterServiceTest {

    @Mock
    private StringRedisTemplate cacheStringRedisTemplate;

    @Mock
    private ValueOperations<String, String> valueOperations;

    @Mock
    private TrafficLuaScriptInfraService trafficLuaScriptInfraService;

    @Mock
    private TrafficRedisKeyFactory trafficRedisKeyFactory;

    @Mock
    private TrafficRedisRuntimePolicy trafficRedisRuntimePolicy;

    @Mock
    private TrafficQuotaSourcePort trafficQuotaSourcePort;

    @Mock
    private TrafficQuotaCacheService trafficQuotaCacheService;

    @Mock
    private TrafficLinePolicyHydrationService trafficLinePolicyHydrationService;

    @Mock
    private TrafficPolicyBootstrapService trafficPolicyBootstrapService;

    @Mock
    private TrafficHydrateMetrics trafficHydrateMetrics;

    @Mock
    private TrafficRefillMetrics trafficRefillMetrics;

    @Mock
    private RedisOutboxRecordService redisOutboxRecordService;

    @Mock
    private TrafficRefillOutboxSupportService trafficRefillOutboxSupportService;

    @InjectMocks
    private TrafficHydrateRefillAdapterService trafficHydrateRefillAdapterService;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(trafficHydrateRefillAdapterService, "hydrateLockEnabled", true);
        ReflectionTestUtils.setField(trafficHydrateRefillAdapterService, "hydrateLockWaitMs", 0L);
        Mockito.lenient().when(trafficRefillOutboxSupportService.resolveIdempotencyKey(anyString()))
                .thenAnswer(invocation -> "pooli:refill:idempotency:" + invocation.getArgument(0, String.class));
        Mockito.lenient().when(trafficRefillOutboxSupportService.refillIdempotencyTtlSeconds()).thenReturn(600L);
        Mockito.lenient().when(trafficQuotaCacheService.applyRefillWithIdempotency(anyString(), anyString(), anyString(), anyLong(), anyLong(), anyLong(), anyBoolean()))
                .thenReturn(true);
        Mockito.lenient().when(trafficRefillOutboxSupportService.unwrapRuntimeException(any(RuntimeException.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));
        Mockito.lenient().when(trafficRefillOutboxSupportService.isConnectionFailure(any())).thenReturn(false);
        Mockito.lenient().when(trafficRefillOutboxSupportService.isTimeoutFailure(any())).thenReturn(false);
    }

    @Nested
    class ExecuteIndividualWithRecoveryTest {

        @Test
        void globalPolicyHydrateThenRetrySameLuaSuccess() {
            // given
            TrafficPayloadReqDto payload = createPayload();
            stubIndividualDeductKeys();
            when(trafficRedisRuntimePolicy.zoneId()).thenReturn(ZoneId.of("Asia/Seoul"));
            when(trafficRedisKeyFactory.remainingIndivAmountKey(eq(11L), any())).thenReturn("pooli:remaining_indiv_amount:11:202603");
            when(trafficLuaScriptInfraService.executeDeductIndividual(anyList(), anyList()))
                    .thenReturn(luaResult(0L, TrafficLuaStatus.GLOBAL_POLICY_HYDRATE))
                    .thenReturn(luaResult(80L, TrafficLuaStatus.OK));

            // when
            TrafficLuaExecutionResult result =
                    trafficHydrateRefillAdapterService.executeIndividualWithRecovery(payload, 100L);

            // then
            assertAll(
                    () -> assertEquals(TrafficLuaStatus.OK, result.getStatus()),
                    () -> assertEquals(80L, result.getAnswer())
            );
            verify(trafficPolicyBootstrapService, times(1)).hydrateOnDemand();
            verify(trafficLuaScriptInfraService, times(2)).executeDeductIndividual(anyList(), anyList());
            verify(trafficQuotaCacheService, never()).hydrateBalance(anyString(), anyLong());
        }

        @Test
        void keepsGlobalPolicyHydrateWhenRetryExhausted() {
            // given
            TrafficPayloadReqDto payload = createPayload();
            stubIndividualDeductKeys();
            when(trafficRedisRuntimePolicy.zoneId()).thenReturn(ZoneId.of("Asia/Seoul"));
            when(trafficRedisKeyFactory.remainingIndivAmountKey(eq(11L), any())).thenReturn("pooli:remaining_indiv_amount:11:202603");
            when(trafficLuaScriptInfraService.executeDeductIndividual(anyList(), anyList()))
                    .thenReturn(luaResult(0L, TrafficLuaStatus.GLOBAL_POLICY_HYDRATE));

            // when
            TrafficLuaExecutionResult result =
                    trafficHydrateRefillAdapterService.executeIndividualWithRecovery(payload, 100L);

            // then
            assertAll(
                    () -> assertEquals(TrafficLuaStatus.GLOBAL_POLICY_HYDRATE, result.getStatus()),
                    () -> assertEquals(0L, result.getAnswer())
            );
            verify(trafficPolicyBootstrapService, times(10)).hydrateOnDemand();
            verify(trafficLuaScriptInfraService, times(11)).executeDeductIndividual(anyList(), anyList());
        }

        @Test
        void hydrateThenRetrySuccess() {
            // given
            TrafficPayloadReqDto payload = createPayload();
            stubIndividualDeductKeys();
            when(trafficRedisRuntimePolicy.zoneId()).thenReturn(ZoneId.of("Asia/Seoul"));
            when(trafficRedisKeyFactory.remainingIndivAmountKey(eq(11L), any())).thenReturn("pooli:remaining_indiv_amount:11:202603");
            when(trafficRedisKeyFactory.indivHydrateLockKey(11L)).thenReturn("pooli:indiv_hydrate_lock:11");
            when(trafficRedisRuntimePolicy.resolveMonthlyExpireAtEpochSeconds(any())).thenReturn(1_770_000_000L);
            when(cacheStringRedisTemplate.opsForValue()).thenReturn(valueOperations);
            when(valueOperations.setIfAbsent(
                    "pooli:indiv_hydrate_lock:11",
                    payload.getTraceId(),
                    java.time.Duration.ofMillis(TrafficRedisRuntimePolicy.LOCK_TTL_MS)
            )).thenReturn(true);
            when(trafficLuaScriptInfraService.executeDeductIndividual(anyList(), anyList()))
                    .thenReturn(luaResult(0L, TrafficLuaStatus.HYDRATE))
                    .thenReturn(luaResult(80L, TrafficLuaStatus.OK));
            when(trafficQuotaSourcePort.loadIndividualQosSpeedLimit(payload)).thenReturn(2_500L);

            // when
            TrafficLuaExecutionResult result =
                    trafficHydrateRefillAdapterService.executeIndividualWithRecovery(payload, 100L);

            // then
            assertAll(
                    () -> assertEquals(TrafficLuaStatus.OK, result.getStatus()),
                    () -> assertEquals(80L, result.getAnswer())
            );
            verify(trafficQuotaCacheService).hydrateBalance("pooli:remaining_indiv_amount:11:202603", 1_770_000_000L);
            verify(trafficQuotaCacheService).putQos("pooli:remaining_indiv_amount:11:202603", 2_500L);
            verify(trafficLuaScriptInfraService).executeLockRelease("pooli:indiv_hydrate_lock:11", payload.getTraceId());
        }

        @Test
        void hydrateLockMissThenRetrySuccess() {
            // given
            TrafficPayloadReqDto payload = createPayload();
            stubIndividualDeductKeys();
            when(trafficRedisRuntimePolicy.zoneId()).thenReturn(ZoneId.of("Asia/Seoul"));
            when(trafficRedisKeyFactory.remainingIndivAmountKey(eq(11L), any())).thenReturn("pooli:remaining_indiv_amount:11:202603");
            when(trafficRedisKeyFactory.indivHydrateLockKey(11L)).thenReturn("pooli:indiv_hydrate_lock:11");
            when(cacheStringRedisTemplate.opsForValue()).thenReturn(valueOperations);
            when(valueOperations.setIfAbsent(
                    "pooli:indiv_hydrate_lock:11",
                    payload.getTraceId(),
                    java.time.Duration.ofMillis(TrafficRedisRuntimePolicy.LOCK_TTL_MS)
            )).thenReturn(false);
            when(trafficLuaScriptInfraService.executeDeductIndividual(anyList(), anyList()))
                    .thenReturn(luaResult(0L, TrafficLuaStatus.HYDRATE))
                    .thenReturn(luaResult(40L, TrafficLuaStatus.OK));

            // when
            TrafficLuaExecutionResult result =
                    trafficHydrateRefillAdapterService.executeIndividualWithRecovery(payload, 100L);

            // then
            assertAll(
                    () -> assertEquals(TrafficLuaStatus.OK, result.getStatus()),
                    () -> assertEquals(40L, result.getAnswer())
            );
            verify(trafficQuotaCacheService, never()).hydrateBalance(anyString(), anyLong());
            verify(trafficLuaScriptInfraService, never()).executeLockRelease("pooli:indiv_hydrate_lock:11", payload.getTraceId());
            verify(trafficLuaScriptInfraService, times(2)).executeDeductIndividual(anyList(), anyList());
        }

        @Test
        void refillGateOkThenRetrySuccess() {
            // given
            TrafficPayloadReqDto payload = createPayload();
            stubIndividualDeductKeys();
            when(trafficRedisRuntimePolicy.zoneId()).thenReturn(ZoneId.of("Asia/Seoul"));
            when(trafficRedisKeyFactory.remainingIndivAmountKey(eq(11L), any())).thenReturn("pooli:remaining_indiv_amount:11:202603");
            when(trafficRedisKeyFactory.indivRefillLockKey(11L)).thenReturn("pooli:indiv_refill_lock:11");
            when(trafficRedisRuntimePolicy.resolveMonthlyExpireAtEpochSeconds(any())).thenReturn(1_770_000_000L);

            when(trafficLuaScriptInfraService.executeDeductIndividual(anyList(), anyList()))
                    .thenReturn(luaResult(0L, TrafficLuaStatus.NO_BALANCE))
                    .thenReturn(luaResult(60L, TrafficLuaStatus.OK));
            when(trafficQuotaCacheService.readAmountOrDefault("pooli:remaining_indiv_amount:11:202603", 0L)).thenReturn(0L);
            when(trafficQuotaSourcePort.resolveRefillPlan(TrafficPoolType.INDIVIDUAL, payload))
                    .thenReturn(refillPlan(10L, 2, 20L, 100L, 30L, "RECENT_10S"));
            when(trafficLuaScriptInfraService.executeRefillGate(
                    "pooli:indiv_refill_lock:11",
                    "pooli:remaining_indiv_amount:11:202603",
                    payload.getTraceId(),
                    TrafficRedisRuntimePolicy.LOCK_TTL_MS,
                    0L,
                    30L
            )).thenReturn(TrafficRefillGateStatus.OK);
            when(trafficLuaScriptInfraService.executeLockHeartbeat(
                    "pooli:indiv_refill_lock:11",
                    payload.getTraceId(),
                    TrafficRedisRuntimePolicy.LOCK_TTL_MS
            )).thenReturn(true, true);
            when(trafficQuotaSourcePort.claimRefillAmountFromDb(
                    eq(TrafficPoolType.INDIVIDUAL),
                    eq(payload),
                    eq(java.time.YearMonth.of(2026, 3)),
                    eq(100L),
                    anyString()
            )).thenReturn(claimResult(100L, 1_000L, 100L, 900L));

            // when
            TrafficLuaExecutionResult result =
                    trafficHydrateRefillAdapterService.executeIndividualWithRecovery(payload, 100L);

            // then
            assertAll(
                    () -> assertEquals(TrafficLuaStatus.OK, result.getStatus()),
                    () -> assertEquals(60L, result.getAnswer())
            );
            // writeDbEmptyFlag는 더 이상 별도 호출되지 않는다 — Lua 내부에서 원자적으로 처리된다.
            verify(trafficQuotaCacheService, never()).writeDbEmptyFlag(anyString(), anyBoolean());
            verify(trafficQuotaCacheService).applyRefillWithIdempotency(
                    eq("pooli:remaining_indiv_amount:11:202603"),
                    eq("pooli:refill:idempotency:refill-uuid-100-100"),
                    eq("refill-uuid-100-100"),
                    eq(100L),
                    eq(1_770_000_000L),
                    eq(600L),
                    eq(false) // dbRemainingAfter=900 > 0
            );
            verify(trafficLuaScriptInfraService).executeLockRelease("pooli:indiv_refill_lock:11", payload.getTraceId());
        }

        @Test
        void refillGateWaitKeepsNoBalance() {
            // given
            TrafficPayloadReqDto payload = createPayload();
            stubIndividualDeductKeys();
            when(trafficRedisRuntimePolicy.zoneId()).thenReturn(ZoneId.of("Asia/Seoul"));
            when(trafficRedisKeyFactory.remainingIndivAmountKey(eq(11L), any())).thenReturn("pooli:remaining_indiv_amount:11:202603");
            when(trafficRedisKeyFactory.indivRefillLockKey(11L)).thenReturn("pooli:indiv_refill_lock:11");

            when(trafficLuaScriptInfraService.executeDeductIndividual(anyList(), anyList()))
                    .thenReturn(luaResult(0L, TrafficLuaStatus.NO_BALANCE));
            when(trafficQuotaCacheService.readAmountOrDefault("pooli:remaining_indiv_amount:11:202603", 0L)).thenReturn(0L);
            when(trafficQuotaSourcePort.resolveRefillPlan(TrafficPoolType.INDIVIDUAL, payload))
                    .thenReturn(refillPlan(10L, 2, 20L, 100L, 30L, "RECENT_10S"));
            when(trafficLuaScriptInfraService.executeRefillGate(
                    "pooli:indiv_refill_lock:11",
                    "pooli:remaining_indiv_amount:11:202603",
                    payload.getTraceId(),
                    TrafficRedisRuntimePolicy.LOCK_TTL_MS,
                    0L,
                    30L
            )).thenReturn(TrafficRefillGateStatus.WAIT);

            // when
            TrafficLuaExecutionResult result =
                    trafficHydrateRefillAdapterService.executeIndividualWithRecovery(payload, 100L);

            // then
            assertAll(
                    () -> assertEquals(TrafficLuaStatus.NO_BALANCE, result.getStatus()),
                    () -> assertEquals(0L, result.getAnswer())
            );
            verify(trafficQuotaCacheService, never()).refillBalance(anyString(), anyLong(), anyLong());
            verify(trafficLuaScriptInfraService, never()).executeLockRelease(anyString(), anyString());
        }

        @Test
        void keepsNoBalanceWhenGateSkips() {
            // given
            TrafficPayloadReqDto payload = createPayload();
            stubIndividualDeductKeys();
            when(trafficRedisRuntimePolicy.zoneId()).thenReturn(ZoneId.of("Asia/Seoul"));
            when(trafficRedisKeyFactory.remainingIndivAmountKey(eq(11L), any())).thenReturn("pooli:remaining_indiv_amount:11:202603");
            when(trafficRedisKeyFactory.indivRefillLockKey(11L)).thenReturn("pooli:indiv_refill_lock:11");
            when(trafficLuaScriptInfraService.executeDeductIndividual(anyList(), anyList()))
                    .thenReturn(luaResult(0L, TrafficLuaStatus.NO_BALANCE));
            when(trafficQuotaCacheService.readAmountOrDefault("pooli:remaining_indiv_amount:11:202603", 0L)).thenReturn(0L);
            when(trafficQuotaSourcePort.resolveRefillPlan(TrafficPoolType.INDIVIDUAL, payload))
                    .thenReturn(refillPlan(10L, 2, 20L, 100L, 30L, "RECENT_10S"));
            when(trafficLuaScriptInfraService.executeRefillGate(
                    "pooli:indiv_refill_lock:11",
                    "pooli:remaining_indiv_amount:11:202603",
                    payload.getTraceId(),
                    TrafficRedisRuntimePolicy.LOCK_TTL_MS,
                    0L,
                    30L
            )).thenReturn(TrafficRefillGateStatus.SKIP);

            // when
            TrafficLuaExecutionResult result =
                    trafficHydrateRefillAdapterService.executeIndividualWithRecovery(payload, 100L);

            // then
            assertAll(
                    () -> assertEquals(TrafficLuaStatus.NO_BALANCE, result.getStatus()),
                    () -> assertEquals(0L, result.getAnswer())
            );
            verify(trafficLuaScriptInfraService).executeRefillGate(
                    "pooli:indiv_refill_lock:11",
                    "pooli:remaining_indiv_amount:11:202603",
                    payload.getTraceId(),
                    TrafficRedisRuntimePolicy.LOCK_TTL_MS,
                    0L,
                    30L
            );
            verify(trafficQuotaSourcePort, never()).claimRefillAmountFromDb(any(), any(), any(), anyLong(), anyString());
            verify(trafficQuotaCacheService, never()).refillBalance(anyString(), anyLong(), anyLong());
        }

        @Test
        void writesDbEmptyFlagTrueWhenDbRemainingAfterIsZero() {
            // given
            TrafficPayloadReqDto payload = createPayload();
            stubIndividualDeductKeys();
            when(trafficRedisRuntimePolicy.zoneId()).thenReturn(ZoneId.of("Asia/Seoul"));
            when(trafficRedisKeyFactory.remainingIndivAmountKey(eq(11L), any())).thenReturn("pooli:remaining_indiv_amount:11:202603");
            when(trafficRedisKeyFactory.indivRefillLockKey(11L)).thenReturn("pooli:indiv_refill_lock:11");
            when(trafficLuaScriptInfraService.executeDeductIndividual(anyList(), anyList()))
                    .thenReturn(luaResult(0L, TrafficLuaStatus.NO_BALANCE));
            when(trafficQuotaCacheService.readAmountOrDefault("pooli:remaining_indiv_amount:11:202603", 0L)).thenReturn(0L);
            when(trafficQuotaSourcePort.resolveRefillPlan(TrafficPoolType.INDIVIDUAL, payload))
                    .thenReturn(refillPlan(10L, 2, 20L, 100L, 30L, "RECENT_10S"));
            when(trafficLuaScriptInfraService.executeRefillGate(
                    "pooli:indiv_refill_lock:11",
                    "pooli:remaining_indiv_amount:11:202603",
                    payload.getTraceId(),
                    TrafficRedisRuntimePolicy.LOCK_TTL_MS,
                    0L,
                    30L
            )).thenReturn(TrafficRefillGateStatus.OK);
            when(trafficLuaScriptInfraService.executeLockHeartbeat(
                    "pooli:indiv_refill_lock:11",
                    payload.getTraceId(),
                    TrafficRedisRuntimePolicy.LOCK_TTL_MS
            )).thenReturn(true);
            when(trafficQuotaSourcePort.claimRefillAmountFromDb(
                    eq(TrafficPoolType.INDIVIDUAL),
                    eq(payload),
                    eq(java.time.YearMonth.of(2026, 3)),
                    eq(100L),
                    anyString()
            )).thenReturn(claimResult(100L, 0L, 0L, 0L));

            // when
            TrafficLuaExecutionResult result =
                    trafficHydrateRefillAdapterService.executeIndividualWithRecovery(payload, 100L);

            // then
            assertEquals(TrafficLuaStatus.NO_BALANCE, result.getStatus());
            // actualRefillAmount=0 → db_noop 경로로 return, applyRefillWithIdempotency 미호출
            // writeDbEmptyFlag도 별도 호출되지 않음 (Lua 통합)
            verify(trafficQuotaCacheService, never()).writeDbEmptyFlag(anyString(), anyBoolean());
            verify(trafficQuotaCacheService, never()).applyRefillWithIdempotency(
                    anyString(), anyString(), anyString(), anyLong(), anyLong(), anyLong(), anyBoolean());
            verify(trafficLuaScriptInfraService).executeLockRelease("pooli:indiv_refill_lock:11", payload.getTraceId());
        }

        @Test
        void invalidPayloadReturnsError() {
            // given
            TrafficPayloadReqDto invalidPayload = TrafficPayloadReqDto.builder()
                    .traceId("")
                    .lineId(11L)
                    .familyId(22L)
                    .appId(33)
                    .apiTotalData(100L)
                    .enqueuedAt(System.currentTimeMillis())
                    .build();

            // when
            TrafficLuaExecutionResult result =
                    trafficHydrateRefillAdapterService.executeIndividualWithRecovery(invalidPayload, 100L);

            // then
            assertAll(
                    () -> assertEquals(TrafficLuaStatus.ERROR, result.getStatus()),
                    () -> assertEquals(-1L, result.getAnswer())
            );
            verifyNoInteractions(trafficLuaScriptInfraService);
        }

        @Test
        void ensuresLinePolicyBeforeDeduct() {
            // given
            TrafficPayloadReqDto payload = createPayload();
            stubIndividualDeductKeys();
            when(trafficRedisRuntimePolicy.zoneId()).thenReturn(ZoneId.of("Asia/Seoul"));
            when(trafficRedisKeyFactory.remainingIndivAmountKey(eq(11L), any())).thenReturn("pooli:remaining_indiv_amount:11:202603");
            when(trafficLuaScriptInfraService.executeDeductIndividual(anyList(), anyList()))
                    .thenReturn(luaResult(1L, TrafficLuaStatus.OK));

            // when
            TrafficLuaExecutionResult result =
                    trafficHydrateRefillAdapterService.executeIndividualWithRecovery(payload, 100L);

            // then
            assertEquals(TrafficLuaStatus.OK, result.getStatus());
            InOrder inOrder = inOrder(trafficLinePolicyHydrationService, trafficLuaScriptInfraService);
            inOrder.verify(trafficLinePolicyHydrationService).ensureLoaded(11L);
            inOrder.verify(trafficLuaScriptInfraService).executeDeductIndividual(anyList(), anyList());
        }

        @Test
        void returnsErrorWhenLinePolicyHydrationFails() {
            // given
            TrafficPayloadReqDto payload = createPayload();
            doThrow(new RuntimeException("hydrate-fail"))
                    .when(trafficLinePolicyHydrationService)
                    .ensureLoaded(11L);

            // when
            TrafficLuaExecutionResult result =
                    trafficHydrateRefillAdapterService.executeIndividualWithRecovery(payload, 100L);

            // then
            assertAll(
                    () -> assertEquals(TrafficLuaStatus.ERROR, result.getStatus()),
                    () -> assertEquals(-1L, result.getAnswer())
            );
            verify(trafficLuaScriptInfraService, never()).executeDeductIndividual(anyList(), anyList());
        }

        @Test
        void includesPolicyKeysForIndividualDeduct() {
            // given
            TrafficPayloadReqDto payload = createPayload();
            stubIndividualDeductKeys();
            when(trafficRedisRuntimePolicy.zoneId()).thenReturn(ZoneId.of("Asia/Seoul"));
            when(trafficRedisKeyFactory.remainingIndivAmountKey(eq(11L), any())).thenReturn("pooli:remaining_indiv_amount:11:202603");
            when(trafficLuaScriptInfraService.executeDeductIndividual(anyList(), anyList()))
                    .thenReturn(luaResult(0L, TrafficLuaStatus.BLOCKED_IMMEDIATE));

            // when
            TrafficLuaExecutionResult result = trafficHydrateRefillAdapterService.executeIndividualWithRecovery(payload, 100L);

            // then
            assertEquals(TrafficLuaStatus.BLOCKED_IMMEDIATE, result.getStatus());
            ArgumentCaptor<List<String>> keysCaptor = ArgumentCaptor.forClass(List.class);
            verify(trafficLuaScriptInfraService).executeDeductIndividual(keysCaptor.capture(), anyList());
            List<String> keys = keysCaptor.getValue();
            assertAll(
                    () -> assertTrue(keys.contains("pooli:policy:1")),
                    () -> assertTrue(keys.contains("pooli:policy:2")),
                    () -> assertTrue(keys.contains("pooli:policy:4")),
                    () -> assertTrue(keys.contains("pooli:policy:5")),
                    () -> assertTrue(keys.contains("pooli:policy:6")),
                    () -> assertTrue(keys.contains("pooli:policy:7"))
            );
        }

        @Test
        void keepsNoBalanceWhenLockNotOwned() {
            // given
            TrafficPayloadReqDto payload = createPayload();
            stubIndividualDeductKeys();
            when(trafficRedisRuntimePolicy.zoneId()).thenReturn(ZoneId.of("Asia/Seoul"));
            when(trafficRedisKeyFactory.remainingIndivAmountKey(eq(11L), any())).thenReturn("pooli:remaining_indiv_amount:11:202603");
            when(trafficRedisKeyFactory.indivRefillLockKey(11L)).thenReturn("pooli:indiv_refill_lock:11");
            when(trafficLuaScriptInfraService.executeDeductIndividual(anyList(), anyList()))
                    .thenReturn(luaResult(0L, TrafficLuaStatus.NO_BALANCE));
            when(trafficQuotaCacheService.readAmountOrDefault("pooli:remaining_indiv_amount:11:202603", 0L)).thenReturn(0L);
            when(trafficQuotaSourcePort.resolveRefillPlan(TrafficPoolType.INDIVIDUAL, payload))
                    .thenReturn(refillPlan(10L, 2, 20L, 100L, 30L, "RECENT_10S"));
            when(trafficLuaScriptInfraService.executeRefillGate(
                    "pooli:indiv_refill_lock:11",
                    "pooli:remaining_indiv_amount:11:202603",
                    payload.getTraceId(),
                    TrafficRedisRuntimePolicy.LOCK_TTL_MS,
                    0L,
                    30L
            )).thenReturn(TrafficRefillGateStatus.OK);
            when(trafficLuaScriptInfraService.executeLockHeartbeat(
                    "pooli:indiv_refill_lock:11",
                    payload.getTraceId(),
                    TrafficRedisRuntimePolicy.LOCK_TTL_MS
            )).thenReturn(false);

            // when
            TrafficLuaExecutionResult result =
                    trafficHydrateRefillAdapterService.executeIndividualWithRecovery(payload, 100L);

            // then
            assertAll(
                    () -> assertEquals(TrafficLuaStatus.NO_BALANCE, result.getStatus()),
                    () -> assertEquals(0L, result.getAnswer())
            );
            verify(trafficQuotaSourcePort, never()).claimRefillAmountFromDb(any(), any(), any(), anyLong(), anyString());
            verify(trafficQuotaCacheService, never()).refillBalance(anyString(), anyLong(), anyLong());
            verify(trafficLuaScriptInfraService, never()).executeLockRelease(anyString(), anyString());
            verify(trafficRefillMetrics).increment("INDIVIDUAL", "lock_not_owned");
        }

        @Test
        void keepsNoBalanceWhenDbClaimNoop() {
            // given
            TrafficPayloadReqDto payload = createPayload();
            stubIndividualDeductKeys();
            when(trafficRedisRuntimePolicy.zoneId()).thenReturn(ZoneId.of("Asia/Seoul"));
            when(trafficRedisKeyFactory.remainingIndivAmountKey(eq(11L), any())).thenReturn("pooli:remaining_indiv_amount:11:202603");
            when(trafficRedisKeyFactory.indivRefillLockKey(11L)).thenReturn("pooli:indiv_refill_lock:11");
            when(trafficLuaScriptInfraService.executeDeductIndividual(anyList(), anyList()))
                    .thenReturn(luaResult(0L, TrafficLuaStatus.NO_BALANCE));
            when(trafficQuotaCacheService.readAmountOrDefault("pooli:remaining_indiv_amount:11:202603", 0L)).thenReturn(0L);
            when(trafficQuotaSourcePort.resolveRefillPlan(TrafficPoolType.INDIVIDUAL, payload))
                    .thenReturn(refillPlan(10L, 2, 20L, 100L, 30L, "RECENT_10S"));
            when(trafficLuaScriptInfraService.executeRefillGate(
                    "pooli:indiv_refill_lock:11",
                    "pooli:remaining_indiv_amount:11:202603",
                    payload.getTraceId(),
                    TrafficRedisRuntimePolicy.LOCK_TTL_MS,
                    0L,
                    30L
            )).thenReturn(TrafficRefillGateStatus.OK);
            when(trafficLuaScriptInfraService.executeLockHeartbeat(
                    "pooli:indiv_refill_lock:11",
                    payload.getTraceId(),
                    TrafficRedisRuntimePolicy.LOCK_TTL_MS
            )).thenReturn(true);
            when(trafficQuotaSourcePort.claimRefillAmountFromDb(
                    eq(TrafficPoolType.INDIVIDUAL),
                    eq(payload),
                    eq(java.time.YearMonth.of(2026, 3)),
                    eq(100L),
                    anyString()
            )).thenReturn(claimResult(100L, 0L, 0L, 0L));

            // when
            TrafficLuaExecutionResult result =
                    trafficHydrateRefillAdapterService.executeIndividualWithRecovery(payload, 100L);

            // then
            assertAll(
                    () -> assertEquals(TrafficLuaStatus.NO_BALANCE, result.getStatus()),
                    () -> assertEquals(0L, result.getAnswer())
            );
            verify(trafficQuotaCacheService, never()).refillBalance(anyString(), anyLong(), anyLong());
            verify(trafficLuaScriptInfraService).executeLockRelease("pooli:indiv_refill_lock:11", payload.getTraceId());
            verify(trafficRefillMetrics).increment("INDIVIDUAL", "db_noop");
        }

        @Test
        void releasesLockWhenDbClaimThrows() {
            // given
            TrafficPayloadReqDto payload = createPayload();
            stubIndividualDeductKeys();
            when(trafficRedisRuntimePolicy.zoneId()).thenReturn(ZoneId.of("Asia/Seoul"));
            when(trafficRedisKeyFactory.remainingIndivAmountKey(eq(11L), any())).thenReturn("pooli:remaining_indiv_amount:11:202603");
            when(trafficRedisKeyFactory.indivRefillLockKey(11L)).thenReturn("pooli:indiv_refill_lock:11");
            when(trafficLuaScriptInfraService.executeDeductIndividual(anyList(), anyList()))
                    .thenReturn(luaResult(0L, TrafficLuaStatus.NO_BALANCE));
            when(trafficQuotaCacheService.readAmountOrDefault("pooli:remaining_indiv_amount:11:202603", 0L)).thenReturn(0L);
            when(trafficQuotaSourcePort.resolveRefillPlan(TrafficPoolType.INDIVIDUAL, payload))
                    .thenReturn(refillPlan(10L, 2, 20L, 100L, 30L, "RECENT_10S"));
            when(trafficLuaScriptInfraService.executeRefillGate(
                    "pooli:indiv_refill_lock:11",
                    "pooli:remaining_indiv_amount:11:202603",
                    payload.getTraceId(),
                    TrafficRedisRuntimePolicy.LOCK_TTL_MS,
                    0L,
                    30L
            )).thenReturn(TrafficRefillGateStatus.OK);
            when(trafficLuaScriptInfraService.executeLockHeartbeat(
                    "pooli:indiv_refill_lock:11",
                    payload.getTraceId(),
                    TrafficRedisRuntimePolicy.LOCK_TTL_MS
            )).thenReturn(true);
            when(trafficQuotaSourcePort.claimRefillAmountFromDb(
                    eq(TrafficPoolType.INDIVIDUAL),
                    eq(payload),
                    eq(java.time.YearMonth.of(2026, 3)),
                    eq(100L),
                    anyString()
            )).thenThrow(new IllegalStateException("db unavailable"));

            // when
            TrafficLuaExecutionResult result =
                    trafficHydrateRefillAdapterService.executeIndividualWithRecovery(payload, 100L);

            // then
            assertAll(
                    () -> assertEquals(TrafficLuaStatus.NO_BALANCE, result.getStatus()),
                    () -> assertEquals(0L, result.getAnswer())
            );
            verify(trafficLuaScriptInfraService).executeLockRelease("pooli:indiv_refill_lock:11", payload.getTraceId());
            verify(trafficRefillMetrics).increment("INDIVIDUAL", "db_error");
        }

        @Test
        void returnsErrorWhenStillHydrateAfterRetry() {
            // given
            TrafficPayloadReqDto payload = createPayload();
            stubIndividualDeductKeys();
            when(trafficRedisRuntimePolicy.zoneId()).thenReturn(ZoneId.of("Asia/Seoul"));
            when(trafficRedisKeyFactory.remainingIndivAmountKey(eq(11L), any())).thenReturn("pooli:remaining_indiv_amount:11:202603");
            when(trafficRedisKeyFactory.indivHydrateLockKey(11L)).thenReturn("pooli:indiv_hydrate_lock:11");
            when(trafficRedisRuntimePolicy.resolveMonthlyExpireAtEpochSeconds(any())).thenReturn(1_770_000_000L);
            when(cacheStringRedisTemplate.opsForValue()).thenReturn(valueOperations);
            when(valueOperations.setIfAbsent(
                    "pooli:indiv_hydrate_lock:11",
                    payload.getTraceId(),
                    java.time.Duration.ofMillis(TrafficRedisRuntimePolicy.LOCK_TTL_MS)
            )).thenReturn(true);
            when(trafficLuaScriptInfraService.executeDeductIndividual(anyList(), anyList()))
                    .thenReturn(luaResult(0L, TrafficLuaStatus.HYDRATE))
                    .thenReturn(luaResult(0L, TrafficLuaStatus.HYDRATE));

            // when
            TrafficLuaExecutionResult result =
                    trafficHydrateRefillAdapterService.executeIndividualWithRecovery(payload, 100L);

            // then
            assertAll(
                    () -> assertEquals(TrafficLuaStatus.HYDRATE, result.getStatus()),
                    () -> assertEquals(0L, result.getAnswer())
            );
            verify(trafficQuotaCacheService, times(10))
                    .hydrateBalance("pooli:remaining_indiv_amount:11:202603", 1_770_000_000L);
            verify(trafficLuaScriptInfraService, never())
                    .executeRefillGate(anyString(), anyString(), anyString(), anyLong(), anyLong(), anyLong());
        }
    }

    @Test
    void globalPolicyHydrateThenRetrySameSharedLuaSuccess() {
        // given
        TrafficPayloadReqDto payload = createPayload();
        stubSharedDeductKeys();
        when(trafficRedisRuntimePolicy.zoneId()).thenReturn(ZoneId.of("Asia/Seoul"));
        when(trafficRedisKeyFactory.remainingSharedAmountKey(eq(22L), any())).thenReturn("pooli:remaining_shared_amount:22:202603");
        when(trafficLuaScriptInfraService.executeDeductShared(anyList(), anyList()))
                .thenReturn(luaResult(0L, TrafficLuaStatus.GLOBAL_POLICY_HYDRATE))
                .thenReturn(luaResult(30L, TrafficLuaStatus.HIT_MONTHLY_SHARED_LIMIT));

        // when
        TrafficLuaExecutionResult result = trafficHydrateRefillAdapterService.executeSharedWithRecovery(payload, 100L);

        // then
        assertAll(
                () -> assertEquals(TrafficLuaStatus.HIT_MONTHLY_SHARED_LIMIT, result.getStatus()),
                () -> assertEquals(30L, result.getAnswer())
        );
        verify(trafficPolicyBootstrapService, times(1)).hydrateOnDemand();
        verify(trafficLuaScriptInfraService, times(2)).executeDeductShared(anyList(), anyList());
    }

    @Test
    void includesPolicyAndMonthlyKeysForSharedDeduct() {
        // given
        TrafficPayloadReqDto payload = createPayload();
        stubSharedDeductKeys();
        when(trafficRedisRuntimePolicy.zoneId()).thenReturn(ZoneId.of("Asia/Seoul"));
        when(trafficRedisKeyFactory.remainingSharedAmountKey(eq(22L), any())).thenReturn("pooli:remaining_shared_amount:22:202603");
        when(trafficLuaScriptInfraService.executeDeductShared(anyList(), anyList()))
                .thenReturn(luaResult(0L, TrafficLuaStatus.HIT_MONTHLY_SHARED_LIMIT));

        // when
        TrafficLuaExecutionResult result = trafficHydrateRefillAdapterService.executeSharedWithRecovery(payload, 100L);

        // then
        assertEquals(TrafficLuaStatus.HIT_MONTHLY_SHARED_LIMIT, result.getStatus());
        ArgumentCaptor<List<String>> keysCaptor = ArgumentCaptor.forClass(List.class);
        verify(trafficLuaScriptInfraService).executeDeductShared(keysCaptor.capture(), anyList());
        List<String> keys = keysCaptor.getValue();
        assertAll(
                () -> assertTrue(keys.contains("pooli:policy:1")),
                () -> assertTrue(keys.contains("pooli:policy:2")),
                () -> assertTrue(keys.contains("pooli:policy:3")),
                () -> assertTrue(keys.contains("pooli:policy:4")),
                () -> assertTrue(keys.contains("pooli:policy:5")),
                () -> assertTrue(keys.contains("pooli:policy:6")),
                () -> assertTrue(keys.contains("pooli:policy:7")),
                () -> assertTrue(keys.contains("pooli:monthly_shared_limit:11")),
                () -> assertTrue(keys.contains("pooli:monthly_shared_usage:11:202603"))
        );
    }

    @Test
    void sharedRefillRetryUsesResidualAndReturnsAccumulatedAnswer() {
        // given
        TrafficPayloadReqDto payload = createPayload();
        stubSharedDeductKeys();
        when(trafficRedisRuntimePolicy.zoneId()).thenReturn(ZoneId.of("Asia/Seoul"));
        when(trafficRedisKeyFactory.remainingSharedAmountKey(eq(22L), any())).thenReturn("pooli:remaining_shared_amount:22:202603");
        when(trafficRedisKeyFactory.sharedRefillLockKey(22L)).thenReturn("pooli:shared_refill_lock:22");
        when(trafficRedisRuntimePolicy.resolveMonthlyExpireAtEpochSeconds(any())).thenReturn(1_770_000_000L);

        when(trafficLuaScriptInfraService.executeDeductShared(anyList(), anyList()))
                .thenReturn(luaResult(41L, TrafficLuaStatus.NO_BALANCE))
                .thenReturn(luaResult(9L, TrafficLuaStatus.OK));
        when(trafficQuotaCacheService.readAmountOrDefault("pooli:remaining_shared_amount:22:202603", 0L)).thenReturn(0L);
        when(trafficQuotaSourcePort.resolveRefillPlan(TrafficPoolType.SHARED, payload))
                .thenReturn(refillPlan(5L, 2, 10L, 50L, 15L, "RECENT_10S"));
        when(trafficLuaScriptInfraService.executeRefillGate(
                "pooli:shared_refill_lock:22",
                "pooli:remaining_shared_amount:22:202603",
                payload.getTraceId(),
                TrafficRedisRuntimePolicy.LOCK_TTL_MS,
                0L,
                15L
        )).thenReturn(TrafficRefillGateStatus.OK);
        when(trafficLuaScriptInfraService.executeLockHeartbeat(
                "pooli:shared_refill_lock:22",
                payload.getTraceId(),
                TrafficRedisRuntimePolicy.LOCK_TTL_MS
        )).thenReturn(true, true);
        when(trafficQuotaSourcePort.claimRefillAmountFromDb(
                eq(TrafficPoolType.SHARED),
                eq(payload),
                eq(java.time.YearMonth.of(2026, 3)),
                eq(50L),
                anyString()
        )).thenReturn(claimResult(50L, 50L, 50L, 0L));

        // when
        TrafficLuaExecutionResult result = trafficHydrateRefillAdapterService.executeSharedWithRecovery(payload, 50L);

        // then
        assertAll(
                () -> assertEquals(TrafficLuaStatus.OK, result.getStatus()),
                () -> assertEquals(50L, result.getAnswer())
        );
        // writeDbEmptyFlag는 더 이상 별도 호출되지 않는다 — Lua 내부에서 원자적으로 처리된다.
        verify(trafficQuotaCacheService, never()).writeDbEmptyFlag(anyString(), anyBoolean());
        verify(trafficQuotaCacheService).applyRefillWithIdempotency(
                eq("pooli:remaining_shared_amount:22:202603"),
                eq("pooli:refill:idempotency:refill-uuid-50-50"),
                eq("refill-uuid-50-50"),
                eq(50L),
                eq(1_770_000_000L),
                eq(600L),
                eq(true) // dbRemainingAfter=0 → is_empty=true
        );
        verify(trafficLuaScriptInfraService).executeLockRelease("pooli:shared_refill_lock:22", payload.getTraceId());

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<String>> argsCaptor = ArgumentCaptor.forClass(List.class);
        verify(trafficLuaScriptInfraService, times(2)).executeDeductShared(anyList(), argsCaptor.capture());
        List<List<String>> allArgs = argsCaptor.getAllValues();
        assertAll(
                () -> assertEquals("50", allArgs.get(0).get(0)),
                () -> assertEquals("9", allArgs.get(1).get(0))
        );
    }

    private TrafficPayloadReqDto createPayload() {
        long enqueuedAt = LocalDateTime.of(2026, 3, 11, 13, 0, 0)
                .toInstant(ZoneOffset.ofHours(9))
                .toEpochMilli();

        return TrafficPayloadReqDto.builder()
                .traceId("trace-001")
                .lineId(11L)
                .familyId(22L)
                .appId(33)
                .apiTotalData(100L)
                .enqueuedAt(enqueuedAt)
                .build();
    }

    private void stubIndividualDeductKeys() {
        for (long policyId = 1; policyId <= 7; policyId++) {
            when(trafficRedisKeyFactory.policyKey(policyId)).thenReturn("pooli:policy:" + policyId);
        }
        when(trafficRedisKeyFactory.appWhitelistKey(11L)).thenReturn("pooli:app_whitelist:11");
        when(trafficRedisKeyFactory.immediatelyBlockEndKey(11L)).thenReturn("pooli:immediately_block_end:11");
        when(trafficRedisKeyFactory.repeatBlockKey(11L)).thenReturn("pooli:repeat_block:11");
        when(trafficRedisKeyFactory.dailyTotalLimitKey(11L)).thenReturn("pooli:daily_total_limit:11");
        when(trafficRedisKeyFactory.dailyTotalUsageKey(eq(11L), any())).thenReturn("pooli:daily_total_usage:11:20260312");
        when(trafficRedisKeyFactory.appDataDailyLimitKey(11L)).thenReturn("pooli:app_data_daily_limit:11");
        when(trafficRedisKeyFactory.dailyAppUsageKey(eq(11L), any())).thenReturn("pooli:daily_app_usage:11:20260312");
        when(trafficRedisKeyFactory.appSpeedLimitKey(11L)).thenReturn("pooli:app_speed_limit:11");
        when(trafficRedisKeyFactory.speedBucketIndividualKey(eq(11L), anyLong())).thenReturn("pooli:speed_bucket:individual:11:1");
        when(trafficRedisRuntimePolicy.resolveDailyExpireAtEpochSeconds(any())).thenReturn(1_741_800_000L);
    }

    private void stubSharedDeductKeys() {
        for (long policyId = 1; policyId <= 7; policyId++) {
            when(trafficRedisKeyFactory.policyKey(policyId)).thenReturn("pooli:policy:" + policyId);
        }
        when(trafficRedisKeyFactory.appWhitelistKey(11L)).thenReturn("pooli:app_whitelist:11");
        when(trafficRedisKeyFactory.immediatelyBlockEndKey(11L)).thenReturn("pooli:immediately_block_end:11");
        when(trafficRedisKeyFactory.repeatBlockKey(11L)).thenReturn("pooli:repeat_block:11");
        when(trafficRedisKeyFactory.dailyTotalLimitKey(11L)).thenReturn("pooli:daily_total_limit:11");
        when(trafficRedisKeyFactory.dailyTotalUsageKey(eq(11L), any())).thenReturn("pooli:daily_total_usage:11:20260312");
        when(trafficRedisKeyFactory.monthlySharedLimitKey(11L)).thenReturn("pooli:monthly_shared_limit:11");
        when(trafficRedisKeyFactory.monthlySharedUsageKey(eq(11L), any())).thenReturn("pooli:monthly_shared_usage:11:202603");
        when(trafficRedisKeyFactory.appDataDailyLimitKey(11L)).thenReturn("pooli:app_data_daily_limit:11");
        when(trafficRedisKeyFactory.dailyAppUsageKey(eq(11L), any())).thenReturn("pooli:daily_app_usage:11:20260312");
        when(trafficRedisKeyFactory.appSpeedLimitKey(11L)).thenReturn("pooli:app_speed_limit:11");
        when(trafficRedisKeyFactory.speedBucketSharedKey(eq(22L), anyLong())).thenReturn("pooli:speed_bucket:shared:22:1");
        when(trafficRedisRuntimePolicy.resolveDailyExpireAtEpochSeconds(any())).thenReturn(1_741_800_000L);
        when(trafficRedisRuntimePolicy.resolveMonthlyExpireAtEpochSeconds(any())).thenReturn(1_742_700_000L);
    }

    private TrafficLuaExecutionResult luaResult(long answer, TrafficLuaStatus status) {
        return TrafficLuaExecutionResult.builder()
                .answer(answer)
                .status(status)
                .build();
    }

    private TrafficDbRefillClaimResult claimResult(
            long requestedRefillAmount,
            long dbRemainingBefore,
            long actualRefillAmount,
            long dbRemainingAfter
    ) {
        String refillUuid = "refill-uuid-" + requestedRefillAmount + "-" + actualRefillAmount;
        return TrafficDbRefillClaimResult.builder()
                .requestedRefillAmount(requestedRefillAmount)
                .dbRemainingBefore(dbRemainingBefore)
                .actualRefillAmount(actualRefillAmount)
                .dbRemainingAfter(dbRemainingAfter)
                .refillUuid(refillUuid)
                .outboxRecordId(701L)
                .build();
    }

    private TrafficRefillPlan refillPlan(
            long delta,
            int bucketCount,
            long bucketSum,
            long refillUnit,
            long threshold,
            String source
    ) {
        return TrafficRefillPlan.builder()
                .delta(delta)
                .bucketCount(bucketCount)
                .bucketSum(bucketSum)
                .refillUnit(refillUnit)
                .threshold(threshold)
                .source(source)
                .build();
    }
}
