package com.pooli.notification.mapper;

import com.pooli.notification.domain.dto.response.NotiSendResDto;
import com.pooli.notification.domain.entity.AlarmHistory;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

@Mapper
public interface AlarmHistoryMapper {

    int insertAlarmHistory(
            @Param("lineId") Long lineId,
            @Param("alarmCode") String alarmCode,
            @Param("value") String value
    );

    int insertNotificationAlarms(
            @Param("lineIds") List<Long> lineIds,
            @Param("alarmCode") String alarmCode,
            @Param("value") String value
    );

    List<AlarmHistory> findAlarmHistoryPage(
            @Param("userId") Long userId,
            @Param("lineId") Long lineId,
            @Param("isRead") Boolean isRead,
            @Param("code") String code,
            @Param("offset") int offset,
            @Param("size") int size
    );

    Long countAlarmHistory(
            @Param("userId") Long userId,
            @Param("lineId") Long lineId,
            @Param("isRead") Boolean isRead,
            @Param("code") String code
    );

    Long countUnreadByLineId(
            @Param("userId") Long userId,
            @Param("lineId") Long lineId
    );

    int updateReadOne(
            @Param("alarmHistoryId") Long alarmHistoryId,
            @Param("lineId") Long lineId
    );

    int updateReadAll(@Param("lineId") Long lineId);

    NotiSendResDto findOneByAlarmHistoryIdAndLineId(
            @Param("alarmHistoryId") Long alarmHistoryId,
            @Param("lineId") Long lineId
    );

    int insertNotificationAll(
            @Param("alarmCode") String alarmCode,
            @Param("value") String value
    );

    int insertNotificationOwner(
            @Param("alarmCode") String alarmCode,
            @Param("value") String value
    );

    int insertNotificationMember(
            @Param("alarmCode") String alarmCode,
            @Param("value") String value
    );
}