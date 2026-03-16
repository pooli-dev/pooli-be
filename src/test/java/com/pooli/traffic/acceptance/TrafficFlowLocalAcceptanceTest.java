package com.pooli.traffic.acceptance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.nio.charset.StandardCharsets;
import java.sql.Time;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.YearMonth;
import java.time.ZonedDateTime;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataAccessException;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pooli.traffic.domain.dto.response.TrafficGenerateResDto;
import com.pooli.traffic.domain.entity.TrafficDeductDoneLog;
import com.pooli.traffic.service.policy.TrafficPolicyBootstrapService;
import com.pooli.traffic.service.runtime.TrafficRedisKeyFactory;
import com.pooli.traffic.service.runtime.TrafficRedisRuntimePolicy;

/**
 * 트래픽 처리 파이프라인의 로컬 실행 전용 인수 테스트입니다. CI 및 운영 환경에서 절대 실행되면 안 됩니다.
 *
 * <p>중요 제약:
 * - 반드시 local 프로파일에서만 실행합니다.
 * - CI 환경에서는 실행하지 않습니다.
 * - 각 테스트 시작 전 Redis flushall + DB 초기화(가족/회선 잔량)를 강제합니다.
 * - 테스트 데이터 식별자는 family_id=1, line_id=1~4만 사용합니다.
 */
@Tag("local-only")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
@AutoConfigureMockMvc
@ActiveProfiles("local")
@DisabledIfEnvironmentVariable(named = "CI", matches = "(?i)true")
class TrafficFlowLocalAcceptanceTest {

    private static final long FAMILY_ID = 1L;
    private static final long LINE_ID = 1L;
    private static final long LINE_ID_2 = 2L;
    private static final long LINE_ID_3 = 3L;
    private static final long LINE_ID_4 = 4L;

    private static final int APP_ID = 1;
    private static final int APP_ID_NON_WHITELIST = 2;

    private static final int POLICY_REPEAT_BLOCK = 1;
    private static final int POLICY_IMMEDIATE_BLOCK = 2;
    private static final int POLICY_LINE_LIMIT_SHARED = 3;
    private static final int POLICY_LINE_LIMIT_DAILY = 4;
    private static final int POLICY_APP_DATA = 5;
    private static final int POLICY_APP_SPEED = 6;
    private static final int POLICY_APP_WHITELIST = 7;

    private static final long RESET_FAMILY_REMAINING = 100L;
    private static final long RESET_LINE_REMAINING = 200L;
    private static final String TARGET_LINE_IDS = "1,2,3,4";

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private MongoTemplate mongoTemplate;

    @Autowired
    @Qualifier("cacheStringRedisTemplate")
    private StringRedisTemplate cacheStringRedisTemplate;

    @Autowired
    private TrafficRedisKeyFactory trafficRedisKeyFactory;

    @Autowired
    private TrafficRedisRuntimePolicy trafficRedisRuntimePolicy;

    @Autowired
    private TrafficPolicyBootstrapService trafficPolicyBootstrapService;

    @BeforeEach
    void setUp() {
        // 테스트 독립성 보장을 위해 cache Redis를 매번 비운다.
        // streams Redis를 flushall 하면 stream key/group/PEL이 함께 사라져
        // local consumer 루프가 NOGROUP 상태에 빠질 수 있으므로 여기서는 건드리지 않는다.
        flushAll(cacheStringRedisTemplate);

        // 시나리오 기준 대상 데이터(가족 1, 회선 1~4)가 존재하는지 먼저 검증한다.
        Integer familyExists = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM FAMILY WHERE family_id = ? AND deleted_at IS NULL",
                Integer.class,
                FAMILY_ID
        );
        assertThat(familyExists).isNotNull();
        assertThat(familyExists).isEqualTo(1);

        Integer lineCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM LINE WHERE line_id IN (1,2,3,4) AND deleted_at IS NULL",
                Integer.class
        );
        assertThat(lineCount).isNotNull();
        assertThat(lineCount).isEqualTo(4);

        // 공통 초기값: family 잔량 100, line(1~4) 잔량 200.
        jdbcTemplate.update(
                "UPDATE FAMILY SET pool_remaining_data = ?, pool_total_data = ?, updated_at = NOW(6) WHERE family_id = ?",
                RESET_FAMILY_REMAINING,
                RESET_FAMILY_REMAINING,
                FAMILY_ID
        );
        jdbcTemplate.update(
                "UPDATE LINE SET remaining_data = ?, updated_at = NOW(6) WHERE line_id IN (" + TARGET_LINE_IDS + ")",
                RESET_LINE_REMAINING
        );

        // lineId 1~4에 대해 인수테스트가 생성할 정책 데이터를 모두 원복한다.
        jdbcTemplate.update("UPDATE LINE SET block_end_at = NULL, updated_at = NOW(6) WHERE line_id IN (" + TARGET_LINE_IDS + ")");
        jdbcTemplate.update(
                "DELETE rbd FROM REPEAT_BLOCK_DAY rbd JOIN REPEAT_BLOCK rb ON rb.repeat_block_id = rbd.repeat_block_id WHERE rb.line_id IN (" + TARGET_LINE_IDS + ")"
        );
        jdbcTemplate.update("DELETE FROM REPEAT_BLOCK WHERE line_id IN (" + TARGET_LINE_IDS + ")");
        jdbcTemplate.update("DELETE FROM APP_POLICY WHERE line_id IN (" + TARGET_LINE_IDS + ")");
        jdbcTemplate.update("DELETE FROM LINE_LIMIT WHERE line_id IN (" + TARGET_LINE_IDS + ")");

        // 전역 정책 레코드는 soft-delete 흔적을 제거하고 기본 활성 상태로 맞춘다.
        jdbcTemplate.update("UPDATE POLICY SET deleted_at = NULL, is_active = true, updated_at = NOW(6)");

        // 전역 정책 1~7은 기본 활성 상태로 복구한다.
        setAllGlobalPolicies(true);
        syncPolicySnapshot();
    }

    // ---------------------------------------------------------------------
    // 기존 회귀 테스트(4개) 유지
    // ---------------------------------------------------------------------

    @Test
    @DisplayName("[REG-01] 기본 흐름: 개인풀 차감 요청은 DB 잔량에서 refill 후 정상 차감된다")
    void shouldDeductIndividualBalanceAfterHydrateAndRefill() throws Exception {
        long before = readLineRemaining(LINE_ID);

        String traceId = enqueueTrafficRequest(LINE_ID, FAMILY_ID, APP_ID, 50L);
        TrafficDeductDoneLog doneLog = assertDoneLog(traceId, 50L, 0L, "SUCCESS", "OK");

        long after = readLineRemaining(LINE_ID);
        assertThat(after).isEqualTo(before - 50L);
        assertThat(doneLog.getLineId()).isEqualTo(LINE_ID);
        assertThat(doneLog.getFamilyId()).isEqualTo(FAMILY_ID);
        assertThat(doneLog.getAppId()).isEqualTo(APP_ID);
        assertThat(doneLog.getApiTotalData()).isEqualTo(50L);
    }

    @Test
    @DisplayName("[REG-02] 즉시 차단 정책 활성 + line block 설정 시 차감이 발생하지 않는다")
    void shouldBlockWhenImmediatePolicyIsActive() throws Exception {
        setImmediateBlock(LINE_ID, 10);
        syncPolicySnapshot();

        long before = readLineRemaining(LINE_ID);
        String traceId = enqueueTrafficRequest(LINE_ID, FAMILY_ID, APP_ID, 50L);
        assertDoneLog(traceId, 0L, 50L, "PARTIAL_SUCCESS", "BLOCKED_IMMEDIATE");

        long after = readLineRemaining(LINE_ID);
        assertThat(after).isEqualTo(before);
    }

    @Test
    @DisplayName("[REG-03] 즉시 차단 정책 비활성 시 line block이 있어도 차감이 진행된다")
    void shouldBypassImmediateBlockWhenPolicyIsDisabled() throws Exception {
        setImmediateBlock(LINE_ID, 10);
        setGlobalPolicy(POLICY_IMMEDIATE_BLOCK, false);
        syncPolicySnapshot();

        long before = readLineRemaining(LINE_ID);
        String traceId = enqueueTrafficRequest(LINE_ID, FAMILY_ID, APP_ID, 50L);
        assertDoneLog(traceId, 50L, 0L, "SUCCESS", "OK");

        long after = readLineRemaining(LINE_ID);
        assertThat(after).isEqualTo(before - 50L);
    }

    @Test
    @DisplayName("[REG-04] 일일 한도 정책 활성 시 한도까지만 차감되고 정책 비활성 시 전체 차감된다")
    void shouldRespectDailyLimitOnlyWhenPolicyIsActive() throws Exception {
        upsertLineLimit(LINE_ID, 30L, true, -1L, false);
        syncPolicySnapshot();

        LocalDate today = LocalDate.now(trafficRedisRuntimePolicy.zoneId());
        String dailyUsageKey = trafficRedisKeyFactory.dailyTotalUsageKey(LINE_ID, today);

        String firstTraceId = enqueueTrafficRequest(LINE_ID, FAMILY_ID, APP_ID, 50L);
        assertDoneLog(firstTraceId, 30L, 20L, "PARTIAL_SUCCESS", "HIT_DAILY_LIMIT");
        await("daily usage reflects active daily limit", () -> readLongValue(dailyUsageKey) == 30L);

        setGlobalPolicy(POLICY_LINE_LIMIT_DAILY, false);
        syncPolicySnapshot();

        String secondTraceId = enqueueTrafficRequest(LINE_ID, FAMILY_ID, APP_ID, 50L);
        assertDoneLog(secondTraceId, 50L, 0L, "SUCCESS", "OK");
        await("daily usage adds full request when daily policy disabled", () -> readLongValue(dailyUsageKey) == 80L);
    }

    // ---------------------------------------------------------------------
    // A. 단일 정책 검증 (1~9)
    // ---------------------------------------------------------------------

    @Test
    @DisplayName("[A-01] 현재 차단 구간에서는 반복 차단 정책이 차감을 막는다")
    void shouldBlockWhenRepeatBlockPolicyIsActive() throws Exception {
        insertRepeatBlockForNow(LINE_ID);
        syncPolicySnapshot();

        long before = readLineRemaining(LINE_ID);
        String traceId = enqueueTrafficRequest(LINE_ID, FAMILY_ID, APP_ID, 50L);
        assertDoneLog(traceId, 0L, 50L, "PARTIAL_SUCCESS", "BLOCKED_REPEAT");
        assertThat(readLineRemaining(LINE_ID)).isEqualTo(before);
    }

    @Test
    @DisplayName("[A-02] 반복 차단 전역 정책을 끄면 차단 구간이어도 차감된다")
    void shouldBypassRepeatBlockWhenPolicyIsDisabled() throws Exception {
        insertRepeatBlockForNow(LINE_ID);
        setGlobalPolicy(POLICY_REPEAT_BLOCK, false);
        syncPolicySnapshot();

        String traceId = enqueueTrafficRequest(LINE_ID, FAMILY_ID, APP_ID, 50L);
        assertDoneLog(traceId, 50L, 0L, "SUCCESS", "OK");
    }

    @Test
    @DisplayName("[A-03] 반복 차단 시간이 아니면 정책이 있어도 차감된다")
    void shouldNotBlockWhenOutsideRepeatBlockTimeRange() throws Exception {
        insertRepeatBlockOutsideNow(LINE_ID);
        syncPolicySnapshot();

        String traceId = enqueueTrafficRequest(LINE_ID, FAMILY_ID, APP_ID, 50L);
        assertDoneLog(traceId, 50L, 0L, "SUCCESS", "OK");
    }

    @Test
    @DisplayName("[A-04] 앱 일일 데이터 한도 정책 활성 시 한도까지만 차감된다")
    void shouldRespectAppDailyDataLimitWhenPolicyIsActive() throws Exception {
        upsertAppPolicy(LINE_ID, APP_ID, 30L, -1, true, false);
        syncPolicySnapshot();

        String traceId = enqueueTrafficRequest(LINE_ID, FAMILY_ID, APP_ID, 50L);
        assertDoneLog(traceId, 30L, 20L, "PARTIAL_SUCCESS", "HIT_APP_DAILY_LIMIT");
    }

    @Test
    @DisplayName("[A-05] 앱 데이터 전역 정책 비활성 시 앱 한도 설정이 있어도 전체 차감된다")
    void shouldBypassAppDailyDataLimitWhenPolicyIsDisabled() throws Exception {
        upsertAppPolicy(LINE_ID, APP_ID, 30L, -1, true, false);
        setGlobalPolicy(POLICY_APP_DATA, false);
        syncPolicySnapshot();

        String traceId = enqueueTrafficRequest(LINE_ID, FAMILY_ID, APP_ID, 50L);
        assertDoneLog(traceId, 50L, 0L, "SUCCESS", "OK");
    }

    @Test
    @DisplayName("[A-06] 앱 속도 제한 활성 시 버킷 초과 요청은 차감되지 않는다")
    void shouldRespectAppSpeedLimitWhenPolicyIsActive() throws Exception {
        upsertAppPolicy(LINE_ID, APP_ID, -1L, 1, true, false);
        primeSpeedBucket(LINE_ID, 10_000L);
        syncPolicySnapshot();

        String traceId = enqueueTrafficRequest(LINE_ID, FAMILY_ID, APP_ID, 50L);
        assertDoneLog(traceId, 0L, 50L, "PARTIAL_SUCCESS", "HIT_APP_SPEED");
    }

    @Test
    @DisplayName("[A-07] 앱 속도 전역 정책 비활성 시 버킷 초과 상태여도 차감된다")
    void shouldBypassAppSpeedLimitWhenPolicyIsDisabled() throws Exception {
        upsertAppPolicy(LINE_ID, APP_ID, -1L, 1, true, false);
        primeSpeedBucket(LINE_ID, 10_000L);
        setGlobalPolicy(POLICY_APP_SPEED, false);
        syncPolicySnapshot();

        String traceId = enqueueTrafficRequest(LINE_ID, FAMILY_ID, APP_ID, 50L);
        assertDoneLog(traceId, 50L, 0L, "SUCCESS", "OK");
    }

    @Test
    @DisplayName("[A-08] 월 공유 한도 정책 활성 시 공유풀 차감은 한도까지만 허용된다")
    void shouldRespectMonthlySharedLimitWhenPolicyIsActive() throws Exception {
        setLineRemaining(LINE_ID, 0L);
        setFamilyRemaining(FAMILY_ID, 100L);
        upsertLineLimit(LINE_ID, -1L, false, 40L, true);
        syncPolicySnapshot();

        String traceId = enqueueTrafficRequest(LINE_ID, FAMILY_ID, APP_ID, 50L);
        assertDoneLog(traceId, 40L, 10L, "PARTIAL_SUCCESS", "HIT_MONTHLY_SHARED_LIMIT");
    }

    @Test
    @DisplayName("[A-09] 월 공유 전역 정책 비활성 시 공유풀 한도 설정을 우회한다")
    void shouldBypassMonthlySharedLimitWhenPolicyIsDisabled() throws Exception {
        setLineRemaining(LINE_ID, 0L);
        setFamilyRemaining(FAMILY_ID, 100L);
        upsertLineLimit(LINE_ID, -1L, false, 40L, true);
        setGlobalPolicy(POLICY_LINE_LIMIT_SHARED, false);
        syncPolicySnapshot();

        long familyBefore = readFamilyRemaining(FAMILY_ID);
        String traceId = enqueueTrafficRequest(LINE_ID, FAMILY_ID, APP_ID, 50L);
        assertDoneLog(traceId, 50L, 0L, "SUCCESS", "OK");
        await("family remaining deducted by 50", () -> readFamilyRemaining(FAMILY_ID) == familyBefore - 50L);
    }

    // ---------------------------------------------------------------------
    // B. 복합 정책 검증 (10~14)
    // ---------------------------------------------------------------------

    @Test
    @DisplayName("[B-10] 앱 데이터 제한이 앱 속도 제한보다 먼저 적용된다")
    void shouldHitAppDailyLimitBeforeAppSpeed() throws Exception {
        upsertAppPolicy(LINE_ID, APP_ID, 30L, 1, true, false);
        primeSpeedBucket(LINE_ID, 10L);
        syncPolicySnapshot();

        String traceId = enqueueTrafficRequest(LINE_ID, FAMILY_ID, APP_ID, 50L);
        assertDoneLog(traceId, 30L, 20L, "PARTIAL_SUCCESS", "HIT_APP_DAILY_LIMIT");
    }

    @Test
    @DisplayName("[B-11] 앱 데이터 제한이 충분하면 앱 속도 제한이 결과를 결정한다")
    void shouldHitAppSpeedWhenDataLimitSufficient() throws Exception {
        upsertAppPolicy(LINE_ID, APP_ID, 200L, 1, true, false);
        primeSpeedBucket(LINE_ID, 10_000L);
        syncPolicySnapshot();

        String traceId = enqueueTrafficRequest(LINE_ID, FAMILY_ID, APP_ID, 50L);
        assertDoneLog(traceId, 0L, 50L, "PARTIAL_SUCCESS", "HIT_APP_SPEED");
    }

    @Test
    @DisplayName("[B-12] 일일 총량 제한이 앱 일일 제한보다 먼저 적용된다")
    void shouldHitDailyLimitBeforeAppDailyLimit() throws Exception {
        upsertLineLimit(LINE_ID, 20L, true, -1L, false);
        upsertAppPolicy(LINE_ID, APP_ID, 40L, -1, true, false);
        syncPolicySnapshot();

        String traceId = enqueueTrafficRequest(LINE_ID, FAMILY_ID, APP_ID, 50L);
        assertDoneLog(traceId, 20L, 30L, "PARTIAL_SUCCESS", "HIT_DAILY_LIMIT");
    }

    @Test
    @DisplayName("[B-13] 일일 총량이 충분하면 앱 일일 제한이 결과를 결정한다")
    void shouldHitAppDailyLimitWhenDailyLimitSufficient() throws Exception {
        upsertLineLimit(LINE_ID, 100L, true, -1L, false);
        upsertAppPolicy(LINE_ID, APP_ID, 25L, -1, true, false);
        syncPolicySnapshot();

        String traceId = enqueueTrafficRequest(LINE_ID, FAMILY_ID, APP_ID, 50L);
        assertDoneLog(traceId, 25L, 25L, "PARTIAL_SUCCESS", "HIT_APP_DAILY_LIMIT");
    }

    @Test
    @DisplayName("[B-14] 반복 차단은 일일 한도보다 우선적으로 차감을 막는다")
    void shouldBlockRepeatBeforeDailyLimit() throws Exception {
        insertRepeatBlockForNow(LINE_ID);
        upsertLineLimit(LINE_ID, 20L, true, -1L, false);
        syncPolicySnapshot();

        String traceId = enqueueTrafficRequest(LINE_ID, FAMILY_ID, APP_ID, 50L);
        assertDoneLog(traceId, 0L, 50L, "PARTIAL_SUCCESS", "BLOCKED_REPEAT");
    }

    // ---------------------------------------------------------------------
    // C. 화이트리스트 우회 검증 (15~18)
    // ---------------------------------------------------------------------

    @Test
    @DisplayName("[C-15] 화이트리스트 앱은 즉시/반복 차단을 모두 우회한다")
    void shouldBypassAllBlockPoliciesWhenWhitelisted() throws Exception {
        setImmediateBlock(LINE_ID, 10);
        insertRepeatBlockForNow(LINE_ID);
        upsertAppPolicy(LINE_ID, APP_ID, -1L, -1, true, true);
        syncPolicySnapshot();

        String traceId = enqueueTrafficRequest(LINE_ID, FAMILY_ID, APP_ID, 50L);
        assertDoneLog(traceId, 50L, 0L, "SUCCESS", "OK");
    }

    @Test
    @DisplayName("[C-16] 화이트리스트 앱은 일일/앱 제한도 우회한다")
    void shouldBypassDailyAndAppLimitsWhenWhitelisted() throws Exception {
        upsertLineLimit(LINE_ID, 0L, true, -1L, false);
        upsertAppPolicy(LINE_ID, APP_ID, 0L, 1, true, true);
        primeSpeedBucket(LINE_ID, 10_000L);
        syncPolicySnapshot();

        String traceId = enqueueTrafficRequest(LINE_ID, FAMILY_ID, APP_ID, 50L);
        assertDoneLog(traceId, 50L, 0L, "SUCCESS", "OK");
    }

    @Test
    @DisplayName("[C-17] 화이트리스트 전역 정책이 꺼지면 우회가 비활성화된다")
    void shouldNotBypassWhenWhitelistPolicyIsDisabled() throws Exception {
        upsertLineLimit(LINE_ID, 0L, true, -1L, false);
        upsertAppPolicy(LINE_ID, APP_ID, 0L, 1, true, true);
        primeSpeedBucket(LINE_ID, 10_000L);
        setGlobalPolicy(POLICY_APP_WHITELIST, false);
        syncPolicySnapshot();

        String traceId = enqueueTrafficRequest(LINE_ID, FAMILY_ID, APP_ID, 50L);
        assertDoneLog(traceId, 0L, 50L, "PARTIAL_SUCCESS", "HIT_DAILY_LIMIT");
    }

    @Test
    @DisplayName("[C-18] 화이트리스트 미등록 앱에는 기존 정책이 그대로 적용된다")
    void shouldApplyPoliciesForNonWhitelistedApp() throws Exception {
        upsertLineLimit(LINE_ID, 20L, true, -1L, false);
        upsertAppPolicy(LINE_ID, APP_ID, -1L, -1, true, true);
        syncPolicySnapshot();

        String traceId = enqueueTrafficRequest(LINE_ID, FAMILY_ID, APP_ID_NON_WHITELIST, 50L);
        assertDoneLog(traceId, 20L, 30L, "PARTIAL_SUCCESS", "HIT_DAILY_LIMIT");
    }

    // ---------------------------------------------------------------------
    // D. 개인풀 + 공유풀 연계 차감 검증 (19~25)
    // ---------------------------------------------------------------------

    @Test
    @DisplayName("[D-19] 개인풀 잔량이 충분하면 개인풀에서만 차감된다")
    void shouldDeductOnlyFromIndividualPool() throws Exception {
        long lineBefore = readLineRemaining(LINE_ID);
        long familyBefore = readFamilyRemaining(FAMILY_ID);

        String traceId = enqueueTrafficRequest(LINE_ID, FAMILY_ID, APP_ID, 50L);
        assertDoneLog(traceId, 50L, 0L, "SUCCESS", "OK");

        assertThat(readLineRemaining(LINE_ID)).isEqualTo(lineBefore - 50L);
        assertThat(readFamilyRemaining(FAMILY_ID)).isEqualTo(familyBefore);
    }

    @Test
    @DisplayName("[D-20] 개인풀에서 요청량만큼 차감되면 공유풀은 사용되지 않는다")
    void shouldDeductPartialFromIndividualPool() throws Exception {
        long lineBefore = readLineRemaining(LINE_ID);
        long familyBefore = readFamilyRemaining(FAMILY_ID);

        String traceId = enqueueTrafficRequest(LINE_ID, FAMILY_ID, APP_ID, 100L);
        assertDoneLog(traceId, 100L, 0L, "SUCCESS", "OK");

        assertThat(readLineRemaining(LINE_ID)).isEqualTo(lineBefore - 100L);
        assertThat(readFamilyRemaining(FAMILY_ID)).isEqualTo(familyBefore);
    }

    @Test
    @DisplayName("[D-21] 개인풀 부족 시 부족분만 공유풀에서 보완 차감된다")
    void shouldDeductAllIndivAndPartialShared() throws Exception {
        setLineRemaining(LINE_ID, 30L);
        setFamilyRemaining(FAMILY_ID, 100L);

        long lineBefore = readLineRemaining(LINE_ID);

        String traceId = enqueueTrafficRequest(LINE_ID, FAMILY_ID, APP_ID, 50L);
        assertDoneLog(traceId, 50L, 0L, "SUCCESS", "OK");

        assertThat(readLineRemaining(LINE_ID)).isEqualTo(lineBefore - 30L);
    }

    @Test
    @DisplayName("[D-22] line=30/shared=20/request=50은 코드 기준으로 SUCCESS가 된다")
    void shouldDeductAllIndivAndAllShared() throws Exception {
        // 가이드 문구와 달리, 현재 오케스트레이터 로직은 apiRemainingData=0이면 SUCCESS를 반환한다.
        setLineRemaining(LINE_ID, 30L);
        setFamilyRemaining(FAMILY_ID, 20L);

        long lineBefore = readLineRemaining(LINE_ID);
        long familyBefore = readFamilyRemaining(FAMILY_ID);

        String traceId = enqueueTrafficRequest(LINE_ID, FAMILY_ID, APP_ID, 50L);
        assertDoneLog(traceId, 50L, 0L, "SUCCESS", "OK");

        assertThat(readLineRemaining(LINE_ID)).isEqualTo(lineBefore - 30L);
        assertThat(readFamilyRemaining(FAMILY_ID)).isEqualTo(familyBefore - 20L);
    }

    @Test
    @DisplayName("[D-23] 두 풀 모두 부족하면 부분 성공으로 종료된다")
    void shouldReturnPartialWhenBothPoolsExhausted() throws Exception {
        setLineRemaining(LINE_ID, 10L);
        setFamilyRemaining(FAMILY_ID, 10L);

        String traceId = enqueueTrafficRequest(LINE_ID, FAMILY_ID, APP_ID, 50L);
        assertDoneLog(traceId, 20L, 30L, "PARTIAL_SUCCESS", "NO_BALANCE");
    }

    @Test
    @DisplayName("[D-24] 공유풀 차감 단계에서도 일일 한도 정책이 적용된다")
    void shouldApplyDailyLimitOnSharedPool() throws Exception {
        setLineRemaining(LINE_ID, 0L);
        setFamilyRemaining(FAMILY_ID, 100L);
        upsertLineLimit(LINE_ID, 20L, true, -1L, false);
        syncPolicySnapshot();

        String traceId = enqueueTrafficRequest(LINE_ID, FAMILY_ID, APP_ID, 50L);
        assertDoneLog(traceId, 20L, 30L, "PARTIAL_SUCCESS", "HIT_DAILY_LIMIT");
    }

    @Test
    @DisplayName("[D-25] 반복 차단 활성 시 개인풀 단계에서 BLOCKED_REPEAT로 종료된다")
    void shouldBlockSharedPoolByRepeatBlock() throws Exception {
        // 현재 로직에서는 개인풀 결과가 NO_BALANCE일 때만 공유풀 보완 차감이 실행된다.
        // 반복 차단이 먼저 걸리면 shared fallback 자체가 실행되지 않는다.
        setLineRemaining(LINE_ID, 0L);
        setFamilyRemaining(FAMILY_ID, 100L);
        insertRepeatBlockForNow(LINE_ID);
        syncPolicySnapshot();

        long familyBefore = readFamilyRemaining(FAMILY_ID);
        String traceId = enqueueTrafficRequest(LINE_ID, FAMILY_ID, APP_ID, 50L);
        assertDoneLog(traceId, 0L, 50L, "PARTIAL_SUCCESS", "BLOCKED_REPEAT");
        assertThat(readFamilyRemaining(FAMILY_ID)).isEqualTo(familyBefore);
    }

    // ---------------------------------------------------------------------
    // E. 일일 사용량 누적 검증 (26~27)
    // ---------------------------------------------------------------------

    @Test
    @DisplayName("[E-26] 연속 요청에서도 일일 사용량 누적이 정확히 계산된다")
    void shouldAccumulateDailyUsageAcrossRequests() throws Exception {
        upsertLineLimit(LINE_ID, 100L, true, -1L, false);
        syncPolicySnapshot();

        LocalDate today = LocalDate.now(trafficRedisRuntimePolicy.zoneId());
        String dailyUsageKey = trafficRedisKeyFactory.dailyTotalUsageKey(LINE_ID, today);

        String traceId1 = enqueueTrafficRequest(LINE_ID, FAMILY_ID, APP_ID, 30L);
        assertDoneLog(traceId1, 30L, 0L, "SUCCESS", "OK");

        String traceId2 = enqueueTrafficRequest(LINE_ID, FAMILY_ID, APP_ID, 30L);
        assertDoneLog(traceId2, 30L, 0L, "SUCCESS", "OK");

        String traceId3 = enqueueTrafficRequest(LINE_ID, FAMILY_ID, APP_ID, 50L);
        assertDoneLog(traceId3, 40L, 10L, "PARTIAL_SUCCESS", "HIT_DAILY_LIMIT");

        await("daily usage should be capped at 100", () -> readLongValue(dailyUsageKey) == 100L);
    }

    @Test
    @DisplayName("[E-27] 개인풀+공유풀 전환 시에도 일일 사용량은 합산 누적된다")
    void shouldTrackDailyUsageSeparatelyPerPool() throws Exception {
        upsertLineLimit(LINE_ID, 100L, true, -1L, false);
        setLineRemaining(LINE_ID, 30L);
        setFamilyRemaining(FAMILY_ID, 100L);
        syncPolicySnapshot();

        LocalDate today = LocalDate.now(trafficRedisRuntimePolicy.zoneId());
        String dailyUsageKey = trafficRedisKeyFactory.dailyTotalUsageKey(LINE_ID, today);

        String traceId1 = enqueueTrafficRequest(LINE_ID, FAMILY_ID, APP_ID, 50L);
        assertDoneLog(traceId1, 50L, 0L, "SUCCESS", "OK");

        String traceId2 = enqueueTrafficRequest(LINE_ID, FAMILY_ID, APP_ID, 70L);
        assertDoneLog(traceId2, 50L, 20L, "PARTIAL_SUCCESS", "HIT_DAILY_LIMIT");

        await("daily usage should sum individual+shared", () -> readLongValue(dailyUsageKey) == 100L);
    }

    // ---------------------------------------------------------------------
    // F. 엣지 케이스 및 방어 검증 (28~30)
    // ---------------------------------------------------------------------

    @Test
    @DisplayName("[F-28] 요청 데이터가 0이면 차감 없이 SUCCESS로 종료된다")
    void shouldHandleZeroDataRequest() throws Exception {
        long lineBefore = readLineRemaining(LINE_ID);
        long familyBefore = readFamilyRemaining(FAMILY_ID);

        String traceId = enqueueTrafficRequest(LINE_ID, FAMILY_ID, APP_ID, 0L);
        assertDoneLog(traceId, 0L, 0L, "SUCCESS", null);

        assertThat(readLineRemaining(LINE_ID)).isEqualTo(lineBefore);
        assertThat(readFamilyRemaining(FAMILY_ID)).isEqualTo(familyBefore);
    }

    @Test
    @DisplayName("[F-29] 잔량과 요청량이 정확히 같으면 전량 차감 후 SUCCESS가 된다")
    void shouldHandleExactBalanceRequest() throws Exception {
        setLineRemaining(LINE_ID, 50L);

        String traceId = enqueueTrafficRequest(LINE_ID, FAMILY_ID, APP_ID, 50L);
        assertDoneLog(traceId, 50L, 0L, "SUCCESS", "OK");
        assertThat(readLineRemaining(LINE_ID)).isEqualTo(0L);
    }

    @Test
    @DisplayName("[F-30] 전역 정책 1~7을 모두 비활성화하면 정책 레코드가 있어도 우회된다")
    void shouldHandleMultiplePoliciesAllDisabled() throws Exception {
        setImmediateBlock(LINE_ID, 10);
        insertRepeatBlockForNow(LINE_ID);
        upsertLineLimit(LINE_ID, 20L, true, 20L, true);
        upsertAppPolicy(LINE_ID, APP_ID, 10L, 1, true, false);
        setAllGlobalPolicies(false);
        primeSpeedBucket(LINE_ID, 10_000L);
        syncPolicySnapshot();

        String traceId = enqueueTrafficRequest(LINE_ID, FAMILY_ID, APP_ID, 50L);
        assertDoneLog(traceId, 50L, 0L, "SUCCESS", "OK");
    }

    // ---------------------------------------------------------------------
    // 테스트 보조 헬퍼
    // ---------------------------------------------------------------------

    private void setGlobalPolicy(int policyId, boolean active) {
        jdbcTemplate.update(
                "UPDATE POLICY SET is_active = ?, updated_at = NOW(6) WHERE policy_id = ? AND deleted_at IS NULL",
                active ? 1 : 0,
                policyId
        );
    }

    private void setAllGlobalPolicies(boolean active) {
        jdbcTemplate.update(
                "UPDATE POLICY SET is_active = ?, updated_at = NOW(6) WHERE policy_id BETWEEN 1 AND 7 AND deleted_at IS NULL",
                active ? 1 : 0
        );
    }

    private void upsertLineLimit(long lineId, long dailyLimit, boolean dailyActive, long sharedLimit, boolean sharedActive) {
        jdbcTemplate.update(
                """
                INSERT INTO LINE_LIMIT (
                    line_id,
                    daily_data_limit,
                    is_daily_limit_active,
                    shared_data_limit,
                    is_shared_limit_active,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, NOW(6), NOW(6))
                ON DUPLICATE KEY UPDATE
                    daily_data_limit = VALUES(daily_data_limit),
                    is_daily_limit_active = VALUES(is_daily_limit_active),
                    shared_data_limit = VALUES(shared_data_limit),
                    is_shared_limit_active = VALUES(is_shared_limit_active),
                    deleted_at = NULL,
                    updated_at = NOW(6)
                """,
                lineId,
                dailyLimit,
                dailyActive ? 1 : 0,
                sharedLimit,
                sharedActive ? 1 : 0
        );
    }

    private void upsertAppPolicy(
            long lineId,
            int appId,
            long dataLimit,
            int speedLimit,
            boolean isActive,
            boolean isWhitelist
    ) {
        jdbcTemplate.update(
                """
                INSERT INTO APP_POLICY (
                    line_id,
                    application_id,
                    data_limit,
                    speed_limit,
                    is_active,
                    is_whitelist,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, NOW(6), NOW(6))
                ON DUPLICATE KEY UPDATE
                    data_limit = VALUES(data_limit),
                    speed_limit = VALUES(speed_limit),
                    is_active = VALUES(is_active),
                    is_whitelist = VALUES(is_whitelist),
                    deleted_at = NULL,
                    updated_at = NOW(6)
                """,
                lineId,
                appId,
                dataLimit,
                speedLimit,
                isActive ? 1 : 0,
                isWhitelist ? 1 : 0
        );
    }

    private void insertRepeatBlockForNow(long lineId) {
        int nowSec = LocalTime.now(trafficRedisRuntimePolicy.zoneId()).toSecondOfDay();
        int startSec = Math.max(0, nowSec - 120);
        int endSec = Math.min(86_399, nowSec + 120);
        insertRepeatBlock(lineId, startSec, endSec);
    }

    private void insertRepeatBlockOutsideNow(long lineId) {
        int nowSec = LocalTime.now(trafficRedisRuntimePolicy.zoneId()).toSecondOfDay();
        int startSec = nowSec < 43_200 ? 50_000 : 1_000;
        int endSec = Math.min(86_399, startSec + 600);
        if (startSec <= nowSec && nowSec <= endSec) {
            startSec = 70_000;
            endSec = 70_600;
        }
        insertRepeatBlock(lineId, startSec, endSec);
    }

    private void insertRepeatBlock(long lineId, int startSec, int endSec) {
        jdbcTemplate.update(
                "INSERT INTO REPEAT_BLOCK (line_id, is_active, created_at, updated_at) VALUES (?, 1, NOW(6), NOW(6))",
                lineId
        );

        Long repeatBlockId = jdbcTemplate.queryForObject(
                "SELECT MAX(repeat_block_id) FROM REPEAT_BLOCK WHERE line_id = ? AND deleted_at IS NULL",
                Long.class,
                lineId
        );
        assertThat(repeatBlockId).isNotNull();

        ZonedDateTime now = ZonedDateTime.now(trafficRedisRuntimePolicy.zoneId());
        String dayOfWeek = toPolicyDayOfWeek(now.getDayOfWeek().name());

        jdbcTemplate.update(
                """
                INSERT INTO REPEAT_BLOCK_DAY (
                    repeat_block_id,
                    day_of_week,
                    start_at,
                    end_at,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, NOW(6), NOW(6))
                """,
                repeatBlockId,
                dayOfWeek,
                Time.valueOf(LocalTime.ofSecondOfDay(startSec)),
                Time.valueOf(LocalTime.ofSecondOfDay(endSec))
        );
    }

    private void setImmediateBlock(long lineId, int minutesFromNow) {
        jdbcTemplate.update(
                "UPDATE LINE SET block_end_at = DATE_ADD(NOW(6), INTERVAL ? MINUTE), updated_at = NOW(6) WHERE line_id = ?",
                minutesFromNow,
                lineId
        );
    }

    private void setLineRemaining(long lineId, long amount) {
        jdbcTemplate.update(
                "UPDATE LINE SET remaining_data = ?, updated_at = NOW(6) WHERE line_id = ?",
                amount,
                lineId
        );
    }

    private void setFamilyRemaining(long familyId, long amount) {
        jdbcTemplate.update(
                "UPDATE FAMILY SET pool_remaining_data = ?, pool_total_data = ?, updated_at = NOW(6) WHERE family_id = ?",
                amount,
                amount,
                familyId
        );
    }

    private void primeSpeedBucket(long lineId, long usedBytes) {
        long nowEpochSec = Instant.now().getEpochSecond();
        for (long offset = -2L; offset <= 5L; offset++) {
            String key = trafficRedisKeyFactory.speedBucketIndividualKey(lineId, nowEpochSec + offset);
            cacheStringRedisTemplate.opsForValue().set(key, String.valueOf(usedBytes), Duration.ofSeconds(30));
        }
    }

    private void syncPolicySnapshot() {
        trafficPolicyBootstrapService.reconcilePolicyActivationSnapshot();
    }

    private String toPolicyDayOfWeek(String javaDayName) {
        return switch (javaDayName) {
            case "MONDAY" -> "MON";
            case "TUESDAY" -> "TUE";
            case "WEDNESDAY" -> "WED";
            case "THURSDAY" -> "THU";
            case "FRIDAY" -> "FRI";
            case "SATURDAY" -> "SAT";
            case "SUNDAY" -> "SUN";
            default -> throw new IllegalArgumentException("Unsupported day name: " + javaDayName);
        };
    }

    private TrafficDeductDoneLog assertDoneLog(
            String traceId,
            long expectedDeducted,
            long expectedRemaining,
            String expectedFinalStatus,
            String expectedLastLuaStatus
    ) throws Exception {
        TrafficDeductDoneLog doneLog = awaitDoneLog(traceId);
        assertThat(doneLog.getTraceId()).isEqualTo(traceId);
        assertThat(doneLog.getDeductedTotalBytes()).isEqualTo(expectedDeducted);
        assertThat(doneLog.getApiRemainingData()).isEqualTo(expectedRemaining);
        assertThat(doneLog.getFinalStatus()).isEqualTo(expectedFinalStatus);

        if (expectedLastLuaStatus == null) {
            assertThat(doneLog.getLastLuaStatus()).isNull();
        } else {
            assertThat(doneLog.getLastLuaStatus()).isEqualTo(expectedLastLuaStatus);
        }

        return doneLog;
    }

    private String enqueueTrafficRequest(long lineId, long familyId, int appId, long apiTotalData) throws Exception {
        String requestBody = """
                {
                  "lineId": %d,
                  "familyId": %d,
                  "appId": %d,
                  "apiTotalData": %d
                }
                """.formatted(lineId, familyId, appId, apiTotalData);

        MvcResult mvcResult = mockMvc.perform(
                        post("/api/traffic/requests")
                                .contentType("application/json")
                                .content(requestBody.getBytes(StandardCharsets.UTF_8))
                )
                .andExpect(status().isOk())
                .andReturn();

        String responseBody = mvcResult.getResponse().getContentAsString(StandardCharsets.UTF_8);
        TrafficGenerateResDto response = objectMapper.readValue(responseBody, TrafficGenerateResDto.class);
        assertThat(response.getTraceId()).isNotBlank();
        return response.getTraceId();
    }

    private long readLineRemaining(long lineId) {
        Long value = jdbcTemplate.queryForObject(
                "SELECT remaining_data FROM LINE WHERE line_id = ? AND deleted_at IS NULL",
                Long.class,
                lineId
        );
        assertThat(value).isNotNull();
        return value;
    }

    private long readFamilyRemaining(long familyId) {
        Long value = jdbcTemplate.queryForObject(
                "SELECT pool_remaining_data FROM FAMILY WHERE family_id = ? AND deleted_at IS NULL",
                Long.class,
                familyId
        );
        assertThat(value).isNotNull();
        return value;
    }

    private String readHashField(String key, String field) {
        Object value = cacheStringRedisTemplate.opsForHash().get(key, field);
        return value == null ? "" : String.valueOf(value);
    }

    private long readLongValue(String key) {
        String value = cacheStringRedisTemplate.opsForValue().get(key);
        if (value == null || value.isBlank()) {
            return 0L;
        }
        return Long.parseLong(value);
    }

    private TrafficDeductDoneLog findDoneLog(String traceId) {
        Query query = Query.query(Criteria.where("trace_id").is(traceId));
        return mongoTemplate.findOne(query, TrafficDeductDoneLog.class);
    }

    private TrafficDeductDoneLog awaitDoneLog(String traceId) throws Exception {
        long startedAt = System.currentTimeMillis();
        long timeoutMs = 5_000L;
        while (System.currentTimeMillis() - startedAt < timeoutMs) {
            TrafficDeductDoneLog doneLog = findDoneLog(traceId);
            if (doneLog != null) {
                return doneLog;
            }
            TimeUnit.MILLISECONDS.sleep(100);
        }
        throw new AssertionError("Timeout while waiting done log: traceId=" + traceId);
    }

    private void await(String description, BooleanSupplier condition) throws Exception {
        long startedAt = System.currentTimeMillis();
        long timeoutMs = 5_000L;
        while (System.currentTimeMillis() - startedAt < timeoutMs) {
            if (condition.getAsBoolean()) {
                return;
            }
            TimeUnit.MILLISECONDS.sleep(100);
        }
        throw new AssertionError("Timeout while waiting: " + description);
    }

    private void flushAll(StringRedisTemplate redisTemplate) {
        redisTemplate.execute((RedisCallback<String>) connection -> {
            try {
                connection.serverCommands().flushAll();
            } catch (DataAccessException e) {
                throw e;
            }
            return "OK";
        });
    }
}
