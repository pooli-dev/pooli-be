package com.pooli.notification.domain.entity;

import java.time.LocalDateTime;

import com.pooli.notification.domain.enums.AlarmCode;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class AlarmHistory {

	Long alarmHistoryId;
	Long lineId;
	AlarmCode alarmCode;
	String value;
	LocalDateTime  createdAt;
	LocalDateTime deletedAt;
	LocalDateTime  readAt;
}
