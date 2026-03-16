package com.pooli.traffic.service.runtime;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Lua 정책 스크립트의 실행 순서와 가드 조건을 검증하는 테스트입니다.
 */
class TrafficLuaPolicyContractTest {

    private static final Path INDIVIDUAL_SCRIPT =
            Path.of("src/main/resources/lua/traffic/deduct_indiv.lua");
    private static final Path SHARED_SCRIPT =
            Path.of("src/main/resources/lua/traffic/deduct_shared.lua");
    private static final Path REFILL_GATE_SCRIPT =
            Path.of("src/main/resources/lua/traffic/refill_gate.lua");

    @Test
    void keepsIndividualPolicyPriorityOrder() throws IOException {
        String script = Files.readString(INDIVIDUAL_SCRIPT, StandardCharsets.UTF_8);

        assertAppearsInOrder(
                script,
                "local whitelist_bypass = false",
                "if not whitelist_bypass then",
                "if is_policy_enabled(policy_immediate_key)",
                "if is_policy_enabled(policy_repeat_key)",
                "if is_policy_enabled(policy_daily_key)",
                "if is_policy_enabled(policy_app_data_key)",
                "if is_policy_enabled(policy_app_speed_key)"
        );
    }

    @Test
    void keepsSharedPolicyPriorityOrder() throws IOException {
        String script = Files.readString(SHARED_SCRIPT, StandardCharsets.UTF_8);

        assertAppearsInOrder(
                script,
                "local whitelist_bypass = false",
                "if not whitelist_bypass then",
                "if is_policy_enabled(policy_immediate_key)",
                "if is_policy_enabled(policy_repeat_key)",
                "if is_policy_enabled(policy_daily_key)",
                "if is_policy_enabled(policy_shared_key)",
                "if is_policy_enabled(policy_app_data_key)",
                "if is_policy_enabled(policy_app_speed_key)"
        );
    }

    @Test
    void evaluatesIndividualPolicyChecksInsideWhitelistGuard() throws IOException {
        String script = Files.readString(INDIVIDUAL_SCRIPT, StandardCharsets.UTF_8);

        int bypassGuardIndex = script.indexOf("if not whitelist_bypass then");
        int immediateIndex = script.indexOf("if is_policy_enabled(policy_immediate_key)");
        int speedIndex = script.indexOf("if is_policy_enabled(policy_app_speed_key)");
        int writeIndex = script.indexOf("redis.call(\"HINCRBY\", remaining_key, \"amount\", -answer)");

        assertTrue(bypassGuardIndex >= 0, "Whitelist bypass guard must exist.");
        assertTrue(immediateIndex > bypassGuardIndex, "Immediate policy check must run after whitelist guard.");
        assertTrue(speedIndex > immediateIndex, "App speed policy check must run after immediate policy check.");
        assertTrue(writeIndex > speedIndex, "Redis write must run after policy checks.");
    }

    @Test
    void evaluatesSharedPolicyChecksInsideWhitelistGuard() throws IOException {
        String script = Files.readString(SHARED_SCRIPT, StandardCharsets.UTF_8);

        int bypassGuardIndex = script.indexOf("if not whitelist_bypass then");
        int immediateIndex = script.indexOf("if is_policy_enabled(policy_immediate_key)");
        int monthlySharedIndex = script.indexOf("if is_policy_enabled(policy_shared_key)");
        int speedIndex = script.indexOf("if is_policy_enabled(policy_app_speed_key)");
        int writeIndex = script.indexOf("redis.call(\"HINCRBY\", remaining_key, \"amount\", -answer)");

        assertTrue(bypassGuardIndex >= 0, "Whitelist bypass guard must exist.");
        assertTrue(immediateIndex > bypassGuardIndex, "Immediate policy check must run after whitelist guard.");
        assertTrue(monthlySharedIndex > immediateIndex, "Shared monthly policy check must run after immediate policy check.");
        assertTrue(speedIndex > monthlySharedIndex, "App speed policy check must run after shared monthly policy check.");
        assertTrue(writeIndex > speedIndex, "Redis write must run after policy checks.");
    }

    @Test
    void checksIndividualBlockPolicyBeforeZeroBalanceBranch() throws IOException {
        String script = Files.readString(INDIVIDUAL_SCRIPT, StandardCharsets.UTF_8);

        assertAppearsInOrder(
                script,
                "if is_policy_enabled(policy_immediate_key)",
                "if is_policy_enabled(policy_repeat_key)",
                "if answer <= 0 then"
        );
    }

    @Test
    void checksSharedBlockPolicyBeforeZeroBalanceBranch() throws IOException {
        String script = Files.readString(SHARED_SCRIPT, StandardCharsets.UTF_8);

        assertAppearsInOrder(
                script,
                "if is_policy_enabled(policy_immediate_key)",
                "if is_policy_enabled(policy_repeat_key)",
                "if answer <= 0 then"
        );
    }

    @Test
    @DisplayName("전역 정책 키 누락 가드는 개인풀 정책 판정보다 먼저 수행된다")
    void checksGlobalPolicyGuardBeforeIndividualPolicyChecks() throws IOException {
        String script = Files.readString(INDIVIDUAL_SCRIPT, StandardCharsets.UTF_8);

        assertAppearsInOrder(
                script,
                "if has_missing_global_policy_key(",
                "return as_json(0, \"GLOBAL_POLICY_HYDRATE\")",
                "local whitelist_bypass = false"
        );
    }

    @Test
    @DisplayName("전역 정책 키 누락 가드는 공유풀 정책 판정보다 먼저 수행된다")
    void checksGlobalPolicyGuardBeforeSharedPolicyChecks() throws IOException {
        String script = Files.readString(SHARED_SCRIPT, StandardCharsets.UTF_8);

        assertAppearsInOrder(
                script,
                "if has_missing_global_policy_key(",
                "return as_json(0, \"GLOBAL_POLICY_HYDRATE\")",
                "local whitelist_bypass = false"
        );
    }

    @Test
    void doesNotWriteDbEmptyFlagInDeductScripts() throws IOException {
        String individualScript = Files.readString(INDIVIDUAL_SCRIPT, StandardCharsets.UTF_8);
        String sharedScript = Files.readString(SHARED_SCRIPT, StandardCharsets.UTF_8);

        assertTrue(!individualScript.contains("is_empty"), "Individual deduct script must not write is_empty flag.");
        assertTrue(!sharedScript.contains("is_empty"), "Shared deduct script must not write is_empty flag.");
    }

    @Test
    void refillGateChecksDbEmptyBeforeThreshold() throws IOException {
        String script = Files.readString(REFILL_GATE_SCRIPT, StandardCharsets.UTF_8);

        assertAppearsInOrder(
                script,
                "if is_empty == 1 then",
                "if current_amount >= threshold then"
        );
    }

    private void assertAppearsInOrder(String script, String... fragments) {
        int previousIndex = -1;
        for (String fragment : fragments) {
            int currentIndex = script.indexOf(fragment);
            assertTrue(currentIndex >= 0, "Required fragment not found. fragment=" + fragment);
            assertTrue(
                    currentIndex > previousIndex,
                    "Fragments must appear in order. fragment=" + fragment + ", previousIndex=" + previousIndex + ", currentIndex=" + currentIndex
            );
            previousIndex = currentIndex;
        }
    }
}
