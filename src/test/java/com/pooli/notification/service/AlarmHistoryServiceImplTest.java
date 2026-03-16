package com.pooli.notification.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pooli.common.exception.ApplicationException;
import com.pooli.notification.domain.dto.request.NotiSendReqDto;
import com.pooli.notification.domain.dto.response.NotiSendResDto;
import com.pooli.notification.domain.dto.response.UnreadCountsResDto;
import com.pooli.notification.domain.enums.AlarmCode;
import com.pooli.notification.domain.enums.NotificationTargetType;
import com.pooli.notification.exception.NotificationErrorCode;
import com.pooli.notification.mapper.AlarmHistoryMapper;
import com.pooli.notification.mapper.NotificationLineMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

class AlarmHistoryServiceImplTest {

    private AlarmHistoryMapper alarmHistoryMapper;
    private NotificationLineMapper notificationLineMapper;
    private ObjectMapper objectMapper;
    private AlarmHistoryServiceImpl service;

    @BeforeEach
    void setup() {
        alarmHistoryMapper = mock(AlarmHistoryMapper.class);
        notificationLineMapper = mock(NotificationLineMapper.class);
        objectMapper = new ObjectMapper();
        service = new AlarmHistoryServiceImpl(alarmHistoryMapper, objectMapper, notificationLineMapper);
    }

    @Test
    @DisplayName("DIRECT 알람 전송 성공")
    void sendNotification_direct_success() throws Exception {
        NotiSendReqDto req = new NotiSendReqDto();
        req.setTargetType(NotificationTargetType.DIRECT);
        req.setLineId(Arrays.asList(1L, 2L));

        when(notificationLineMapper.findExistingLineIds(Arrays.asList(1L, 2L)))
                .thenReturn(Arrays.asList(1L, 2L));

        service.sendNotification(req);

        ArgumentCaptor<List<Long>> captor = ArgumentCaptor.forClass(List.class);
        verify(alarmHistoryMapper).insertNotificationAlarms(captor.capture(), eq(AlarmCode.OTHERS.name()), anyString());
        List<Long> capturedLineIds = captor.getValue();

        assertThat(capturedLineIds).containsExactlyInAnyOrder(1L, 2L);
    }

    @Test
    @DisplayName("DIRECT 알람 전송 실패 - lineId 누락")
    void sendNotification_direct_missingLineId_throwsException() {
        NotiSendReqDto req = new NotiSendReqDto();
        req.setTargetType(NotificationTargetType.DIRECT);

        ApplicationException ex = assertThrows(ApplicationException.class,
                () -> service.sendNotification(req));
        assertThat(ex.getErrorCode()).isEqualTo(NotificationErrorCode.LINE_ID_REQUIRED);
    }

    @Test
    @DisplayName("DIRECT 알람 전송 실패 - 존재하지 않는 대상")
    void sendNotification_direct_notFoundTarget_throwsException() {
        NotiSendReqDto req = new NotiSendReqDto();
        req.setTargetType(NotificationTargetType.DIRECT);
        req.setLineId(Arrays.asList(1L, 2L));

        when(notificationLineMapper.findExistingLineIds(Arrays.asList(1L, 2L)))
                .thenReturn(Collections.singletonList(1L)); // 일부만 존재

        ApplicationException ex = assertThrows(ApplicationException.class,
                () -> service.sendNotification(req));
        assertThat(ex.getErrorCode()).isEqualTo(NotificationErrorCode.NOTIFICATION_TARGET_NOT_FOUND);
    }

    @Test
    @DisplayName("ALL 알람 전송 성공")
    void sendNotification_all_success() {
        NotiSendReqDto req = new NotiSendReqDto();
        req.setTargetType(NotificationTargetType.ALL);

        service.sendNotification(req);

        verify(alarmHistoryMapper)
                .insertNotificationAll(eq(AlarmCode.OTHERS.name()), anyString());
    }

    @Test
    @DisplayName("단일 알람 읽기 - 성공")
    void readOne_existingAlarm_returnsAlarm() {
        NotiSendResDto alarm = NotiSendResDto.builder()
                .alarmHistoryId(1L)
                .lineId(10L)
                .isRead(false)
                .build();

        when(alarmHistoryMapper.findOneByAlarmHistoryIdAndLineId(1L, 10L)).thenReturn(alarm);

        service.readOne(1L, 10L);

        verify(alarmHistoryMapper, times(1)).updateReadOne(1L, 10L);
    }

    @Test
    @DisplayName("단일 알람 읽기 - 존재하지 않는 알람")
    void readOne_nonExistingAlarm_throwsException() {
        when(alarmHistoryMapper.findOneByAlarmHistoryIdAndLineId(1L, 10L))
                .thenReturn(null);

        ApplicationException ex = assertThrows(ApplicationException.class,
                () -> service.readOne(1L, 10L));
        assertThat(ex.getErrorCode()).isEqualTo(NotificationErrorCode.ALARM_HISTORY_NOT_FOUND);
    }

    @Test
    @DisplayName("전체 알람 읽기 - 성공")
    void readAll_success() {
        when(alarmHistoryMapper.updateReadAll(10L)).thenReturn(5);

        UnreadCountsResDto result = service.readAll(10L);

        assertThat(result.getLineId()).isEqualTo(10L);
        assertThat(result.getUnreadCount()).isZero();
        assertThat(result.getReadCount()).isEqualTo(5L);
    }
}