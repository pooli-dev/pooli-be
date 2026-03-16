package com.pooli.traffic.service.decision;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.YearMonth;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.pooli.traffic.service.outbox.RedisOutboxRecordService;
import com.pooli.traffic.service.runtime.TrafficRecentUsageBucketService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import com.pooli.traffic.domain.TrafficDbRefillClaimResult;
import com.pooli.traffic.domain.dto.request.TrafficPayloadReqDto;
import com.pooli.traffic.domain.enums.TrafficPoolType;
import com.pooli.traffic.mapper.TrafficRefillSourceMapper;

@ExtendWith(MockitoExtension.class)
class TrafficDefaultQuotaSourceAdapterTest {

    @Mock
    private TrafficRefillSourceMapper trafficRefillSourceMapper;

    @Mock
    private TrafficRecentUsageBucketService trafficRecentUsageBucketService;

    @Mock
    private RedisOutboxRecordService redisOutboxRecordService;

    @InjectMocks
    private TrafficDefaultQuotaSourceAdapter trafficDefaultQuotaSourceAdapter;

    @Nested
    @DisplayName("claimRefillAmountFromDb 테스트")
    class ClaimRefillAmountFromDbTest {

        @Test
        @DisplayName("요청 리필량이 0 이하면 DB 차감 없이 actual=0을 반환한다")
        void returnsNoopWhenRequestedAmountIsNonPositive() {
            // given
            TrafficPayloadReqDto payload = payload();
            when(trafficRefillSourceMapper.selectIndividualRemainingForUpdate(11L)).thenReturn(100L);

            // when
            TrafficDbRefillClaimResult result = trafficDefaultQuotaSourceAdapter.claimRefillAmountFromDb(
                    TrafficPoolType.INDIVIDUAL,
                    payload,
                    YearMonth.of(2026, 3),
                    0L
            );

            // then
            assertAll(
                    () -> assertEquals(0L, result.getRequestedRefillAmount()),
                    () -> assertEquals(100L, result.getDbRemainingBefore()),
                    () -> assertEquals(0L, result.getActualRefillAmount()),
                    () -> assertEquals(100L, result.getDbRemainingAfter())
            );
            verify(trafficRefillSourceMapper, never()).deductIndividualRemaining(anyLong(), anyLong());
        }

        @Test
        @DisplayName("요청량이 DB 잔량보다 크면 actual=min(requested,remaining)으로 제한된다")
        void capsActualRefillByDbRemaining() {
            // given
            TrafficPayloadReqDto payload = payload();
            when(trafficRefillSourceMapper.selectIndividualRemainingForUpdate(11L)).thenReturn(40L);
            when(trafficRefillSourceMapper.deductIndividualRemaining(11L, 40L)).thenReturn(1);
            when(redisOutboxRecordService.createPending(Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(1L);

            // when
            TrafficDbRefillClaimResult result = trafficDefaultQuotaSourceAdapter.claimRefillAmountFromDb(
                    TrafficPoolType.INDIVIDUAL,
                    payload,
                    YearMonth.of(2026, 3),
                    100L
            );

            // then
            assertAll(
                    () -> assertEquals(100L, result.getRequestedRefillAmount()),
                    () -> assertEquals(40L, result.getDbRemainingBefore()),
                    () -> assertEquals(40L, result.getActualRefillAmount()),
                    () -> assertEquals(0L, result.getDbRemainingAfter())
            );
            verify(trafficRefillSourceMapper).deductIndividualRemaining(11L, 40L);
        }

        @Test
        @DisplayName("조건부 UPDATE가 0건이면 actual=0으로 반환하고 현재 잔량을 재조회한다")
        void returnsNoopAndReloadsRemainingWhenUpdateFails() {
            // given
            TrafficPayloadReqDto payload = payload();
            when(trafficRefillSourceMapper.selectIndividualRemainingForUpdate(11L)).thenReturn(30L);
            when(trafficRefillSourceMapper.deductIndividualRemaining(11L, 20L)).thenReturn(0);
            when(trafficRefillSourceMapper.selectIndividualRemaining(11L)).thenReturn(12L);

            // when
            TrafficDbRefillClaimResult result = trafficDefaultQuotaSourceAdapter.claimRefillAmountFromDb(
                    TrafficPoolType.INDIVIDUAL,
                    payload,
                    YearMonth.of(2026, 3),
                    20L
            );

            // then
            assertAll(
                    () -> assertEquals(30L, result.getDbRemainingBefore()),
                    () -> assertEquals(0L, result.getActualRefillAmount()),
                    () -> assertEquals(12L, result.getDbRemainingAfter())
            );
        }

        @Test
        @DisplayName("경합 상황에서도 총 actual 합은 초기 잔량을 넘지 않고 음수가 발생하지 않는다")
        void doesNotOverDeductOrGoNegativeUnderConcurrentClaims() throws InterruptedException, ExecutionException {
            // given
            TrafficPayloadReqDto payload = payload();
            AtomicLong remaining = new AtomicLong(100L);
            Mockito.lenient().when(redisOutboxRecordService.createPending(Mockito.any(), Mockito.any(), Mockito.any()))
                    .thenReturn(1L);

            when(trafficRefillSourceMapper.selectIndividualRemainingForUpdate(11L))
                    .thenAnswer(invocation -> remaining.get());
            Mockito.lenient().when(trafficRefillSourceMapper.selectIndividualRemaining(11L))
                    .thenAnswer(invocation -> remaining.get());
            when(trafficRefillSourceMapper.deductIndividualRemaining(11L, 10L))
                    .thenAnswer(invocation -> {
                        synchronized (remaining) {
                            long current = remaining.get();
                            if (current < 10L) {
                                return 0;
                            }
                            remaining.addAndGet(-10L);
                            return 1;
                        }
                    });

            ExecutorService executorService = Executors.newFixedThreadPool(8);
            List<Callable<TrafficDbRefillClaimResult>> tasks = new ArrayList<>();
            for (int i = 0; i < 20; i++) {
                tasks.add(() -> trafficDefaultQuotaSourceAdapter.claimRefillAmountFromDb(
                        TrafficPoolType.INDIVIDUAL,
                        payload,
                        YearMonth.of(2026, 3),
                        10L
                ));
            }

            // when
            List<Future<TrafficDbRefillClaimResult>> futures = executorService.invokeAll(tasks);
            executorService.shutdown();
            executorService.awaitTermination(3, TimeUnit.SECONDS);

            long totalActualRefill = 0L;
            for (Future<TrafficDbRefillClaimResult> future : futures) {
                TrafficDbRefillClaimResult result = future.get();
                totalActualRefill += result.getActualRefillAmount();
                assertTrue(result.getActualRefillAmount() >= 0L);
                assertTrue(result.getDbRemainingAfter() >= 0L);
            }

            // then
            assertTrue(totalActualRefill <= 100L, "총 리필량이 초기 잔량을 넘으면 안 됩니다.");
            assertTrue(remaining.get() >= 0L, "최종 잔량이 음수가 되면 안 됩니다.");
        }
    }

    @Nested
    @DisplayName("loadIndividualQosSpeedLimit 테스트")
    class LoadIndividualQosSpeedLimitTest {

        @Test
        @DisplayName("qos_speed_limit 원천값에 125를 곱해 반환")
        void multipliesQosSpeedLimitBy125() {
            // given
            TrafficPayloadReqDto payload = payload();
            when(trafficRefillSourceMapper.selectIndividualQosSpeedLimit(11L)).thenReturn(40L);

            // when
            long result = trafficDefaultQuotaSourceAdapter.loadIndividualQosSpeedLimit(payload);

            // then
            assertEquals(5_000L, result);
        }

        @Test
        @DisplayName("qos_speed_limit이 0이면 0을 반환")
        void keepsZeroQosValue() {
            // given
            TrafficPayloadReqDto payload = payload();
            when(trafficRefillSourceMapper.selectIndividualQosSpeedLimit(11L)).thenReturn(0L);

            // when
            long result = trafficDefaultQuotaSourceAdapter.loadIndividualQosSpeedLimit(payload);

            // then
            assertEquals(0L, result);
        }

        @Test
        @DisplayName("qos_speed_limit이 null/음수면 0으로 보정")
        void normalizesInvalidQosValuesToZero() {
            // given
            TrafficPayloadReqDto payload = payload();
            when(trafficRefillSourceMapper.selectIndividualQosSpeedLimit(11L))
                    .thenReturn(null)
                    .thenReturn(-3L);

            // when
            long resultWhenNull = trafficDefaultQuotaSourceAdapter.loadIndividualQosSpeedLimit(payload);
            long resultWhenNegative = trafficDefaultQuotaSourceAdapter.loadIndividualQosSpeedLimit(payload);

            // then
            assertAll(
                    () -> assertEquals(0L, resultWhenNull),
                    () -> assertEquals(0L, resultWhenNegative)
            );
        }
    }

    private TrafficPayloadReqDto payload() {
        return TrafficPayloadReqDto.builder()
                .traceId("trace-001")
                .lineId(11L)
                .familyId(22L)
                .appId(33)
                .apiTotalData(100L)
                .enqueuedAt(System.currentTimeMillis())
                .build();
    }
}
