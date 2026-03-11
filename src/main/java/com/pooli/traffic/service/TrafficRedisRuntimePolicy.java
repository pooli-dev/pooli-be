package com.pooli.traffic.service;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.YearMonth;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * 트래픽 차감 Redis 키의 시간/TTL 규칙을 계산하는 정책 컴포넌트입니다.
 * 명세 고정값(Asia/Seoul, 일말+8h, 월말+10d, lock/inflight 상수)을 한 곳에서 관리합니다.
 */
@Component
@Profile({"local", "traffic"})
public class TrafficRedisRuntimePolicy {

    public static final long LOCK_TTL_MS = 3000L;
    public static final long LOCK_HEARTBEAT_MS = 1000L;
    public static final long INFLIGHT_TTL_SEC = 60L;
    public static final long APP_SPEED_USED_TTL_SEC = 3L;

    private static final ZoneId ASIA_SEOUL_ZONE_ID = ZoneId.of("Asia/Seoul");
    private static final DateTimeFormatter YYYYMMDD_FORMATTER = DateTimeFormatter.BASIC_ISO_DATE;
    private static final DateTimeFormatter YYYYMM_FORMATTER = DateTimeFormatter.ofPattern("yyyyMM");

    /**
     * Provides the ZoneId used for policy calculations.
     *
     * @return the ZoneId for the Asia/Seoul time zone used by this policy
     */
    public ZoneId zoneId() {
        return ASIA_SEOUL_ZONE_ID;
    }

    /**
     * Format a LocalDate as the domain key suffix using the pattern yyyyMMdd.
     *
     * @param targetDate the date to format as yyyyMMdd
     * @return the formatted date string in yyyyMMdd
     * @throws NullPointerException if {@code targetDate} is null
     */
    public String formatYyyyMmDd(LocalDate targetDate) {
        // 키 suffix(yyyymmdd) 규칙을 일관되게 유지한다.
        return Objects.requireNonNull(targetDate, "targetDate must not be null").format(YYYYMMDD_FORMATTER);
    }

    /**
     * Format a YearMonth as the domain key suffix in `yyyyMM` form.
     *
     * @param targetMonth the year-month to format
     * @return the formatted year-month string in `yyyyMM` format
     * @throws NullPointerException if `targetMonth` is null
     */
    public String formatYyyyMm(YearMonth targetMonth) {
        // 키 suffix(yyyymm) 규칙을 일관되게 유지한다.
        return Objects.requireNonNull(targetMonth, "targetMonth must not be null").format(YYYYMM_FORMATTER);
    }

    /**
     * Compute the expiration instant for a given date using the "end of day plus 8 hours" rule in Asia/Seoul.
     *
     * @param targetDate the date for which to compute the expiration; must not be null
     * @return the Instant representing targetDate's end of day plus 8 hours in the Asia/Seoul time zone
     * @throws NullPointerException if {@code targetDate} is null
     */
    public Instant resolveDailyExpireAt(LocalDate targetDate) {
        // "일말 + 8h" 규칙:
        // 1) targetDate의 일말(23:59:59.999...) 계산
        // 2) +8시간 적용
        // 3) Asia/Seoul 기준 Instant로 변환
        LocalDateTime dayEnd = Objects.requireNonNull(targetDate, "targetDate must not be null").atTime(LocalTime.MAX);
        return dayEnd.plusHours(8).atZone(ASIA_SEOUL_ZONE_ID).toInstant();
    }

    /**
     * Compute the epoch-second timestamp for the daily expiration instant, defined as the end of the given date plus 8 hours in the Asia/Seoul time zone.
     *
     * @param targetDate the date whose daily expiration to compute; must not be null
     * @return the epoch second of the expiration instant (end of targetDate plus 8 hours in Asia/Seoul)
     */
    public long resolveDailyExpireAtEpochSeconds(LocalDate targetDate) {
        // Redis EXPIREAT는 epoch seconds를 사용하므로 변환값을 제공한다.
        return resolveDailyExpireAt(targetDate).getEpochSecond();
    }

    /**
     * Compute the expiration Instant for a month according to the "end of month plus 10 days" policy in Asia/Seoul time zone.
     *
     * @param targetMonth the YearMonth to base the expiration on
     * @return the Instant representing the expiration moment (end of the target month plus 10 days, in Asia/Seoul)
     * @throws NullPointerException if targetMonth is null
     */
    public Instant resolveMonthlyExpireAt(YearMonth targetMonth) {
        // "월말 + 10d" 규칙:
        // 1) targetMonth의 월말(23:59:59.999...) 계산
        // 2) +10일 적용
        // 3) Asia/Seoul 기준 Instant로 변환
        LocalDateTime monthEnd = Objects.requireNonNull(targetMonth, "targetMonth must not be null")
                .atEndOfMonth()
                .atTime(LocalTime.MAX);
        return monthEnd.plusDays(10).atZone(ASIA_SEOUL_ZONE_ID).toInstant();
    }

    /**
     * Compute the epoch-second timestamp for the monthly expiration according to the policy.
     *
     * @param targetMonth the target YearMonth to compute the expiration for
     * @return the epoch second of the monthly expiration instant (suitable for Redis EXPIREAT)
     */
    public long resolveMonthlyExpireAtEpochSeconds(YearMonth targetMonth) {
        // Redis EXPIREAT는 epoch seconds를 사용하므로 변환값을 제공한다.
        return resolveMonthlyExpireAt(targetMonth).getEpochSecond();
    }
}
