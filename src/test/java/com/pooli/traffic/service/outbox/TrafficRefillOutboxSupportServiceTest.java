package com.pooli.traffic.service.outbox;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.pooli.traffic.domain.dto.request.TrafficPayloadReqDto;
import com.pooli.traffic.domain.enums.TrafficPoolType;
import com.pooli.traffic.domain.outbox.payload.RefillOutboxPayload;
import com.pooli.traffic.mapper.TrafficRefillSourceMapper;
import com.pooli.traffic.service.runtime.TrafficRedisKeyFactory;
import com.pooli.traffic.service.runtime.TrafficRedisRuntimePolicy;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.StringRedisTemplate;

@ExtendWith(MockitoExtension.class)
class TrafficRefillOutboxSupportServiceTest {

    @Mock
    private StringRedisTemplate cacheStringRedisTemplate;

    @Mock
    private TrafficRedisKeyFactory trafficRedisKeyFactory;

    @Mock
    private TrafficRedisRuntimePolicy trafficRedisRuntimePolicy;

    @Mock
    private TrafficRefillSourceMapper trafficRefillSourceMapper;

    @Mock
    private RedisOutboxRecordService redisOutboxRecordService;

    @InjectMocks
    private TrafficRefillOutboxSupportService trafficRefillOutboxSupportService;

    @Test
    @DisplayName("CAS 전이가 1건 성공하면 payload 기준 DB 반납을 수행한다")
    void compensateRefillOnceRestoresWhenCasSucceeded() {
        // given
        RefillOutboxPayload payload = RefillOutboxPayload.builder()
                .uuid("refill-uuid-1")
                .poolType("INDIVIDUAL")
                .lineId(101L)
                .actualRefillAmount(500L)
                .build();
        when(redisOutboxRecordService.markRevertIfCompensable(701L)).thenReturn(1);

        // when
        trafficRefillOutboxSupportService.compensateRefillOnce(701L, payload);

        // then
        verify(redisOutboxRecordService).markRevertIfCompensable(701L);
        verify(trafficRefillSourceMapper).restoreIndividualRemaining(101L, 500L);
    }

    @Test
    @DisplayName("CAS 전이 영향 행이 0이면 이미 보상된 것으로 보고 반납하지 않는다")
    void compensateRefillOnceSkipsRestoreWhenCasReturnedZero() {
        // given
        RefillOutboxPayload payload = RefillOutboxPayload.builder()
                .uuid("refill-uuid-1")
                .poolType("INDIVIDUAL")
                .lineId(101L)
                .actualRefillAmount(500L)
                .build();
        when(redisOutboxRecordService.markRevertIfCompensable(701L)).thenReturn(0);

        // when
        trafficRefillOutboxSupportService.compensateRefillOnce(701L, payload);

        // then
        verify(redisOutboxRecordService).markRevertIfCompensable(701L);
        verify(trafficRefillSourceMapper, never()).restoreIndividualRemaining(101L, 500L);
    }

    @Test
    @DisplayName("실시간 경로 보상도 CAS 전이 성공 시에만 반납한다")
    void compensateRefillOnceRealtimeRestoresOnlyOnce() {
        // given
        TrafficPayloadReqDto payload = TrafficPayloadReqDto.builder()
                .familyId(303L)
                .build();
        when(redisOutboxRecordService.markRevertIfCompensable(702L)).thenReturn(1, 0);

        // when
        trafficRefillOutboxSupportService.compensateRefillOnce(702L, TrafficPoolType.SHARED, payload, 700L);
        trafficRefillOutboxSupportService.compensateRefillOnce(702L, TrafficPoolType.SHARED, payload, 700L);

        // then
        verify(redisOutboxRecordService, times(2)).markRevertIfCompensable(702L);
        verify(trafficRefillSourceMapper).restoreSharedRemaining(303L, 700L);
    }
}
