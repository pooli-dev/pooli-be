package com.pooli.traffic.service.outbox;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Profile;
import org.springframework.core.io.ClassPathResource;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;

/**
 * 정책 키를 version CAS 규칙으로 Redis에 반영하는 서비스입니다.
 */
@Service
@Profile({"local", "api", "traffic"})
@RequiredArgsConstructor
public class TrafficPolicyVersionedRedisService {

    private static final String VALUE_CAS_SCRIPT_RESOURCE = "lua/traffic/policy_value_cas.lua";
    private static final String REPEAT_BLOCK_CAS_SCRIPT_RESOURCE = "lua/traffic/repeat_block_snapshot_cas.lua";
    private static final String APP_POLICY_SINGLE_CAS_SCRIPT_RESOURCE = "lua/traffic/app_policy_single_cas.lua";
    private static final String APP_POLICY_SNAPSHOT_CAS_SCRIPT_RESOURCE = "lua/traffic/app_policy_snapshot_cas.lua";

    @Qualifier("cacheStringRedisTemplate")
    private final StringRedisTemplate cacheStringRedisTemplate;
    private final ObjectMapper objectMapper;

    private RedisScript<Long> valueCasScript;
    private RedisScript<Long> repeatBlockCasScript;
    private RedisScript<Long> appPolicySingleCasScript;
    private RedisScript<Long> appPolicySnapshotCasScript;

    /**
     * 애플리케이션 시작 시 CAS Lua 스크립트를 classpath에서 읽어 RedisScript로 준비합니다.
     */
    @PostConstruct
    public void initializeScripts() {
        valueCasScript = buildLongScript(loadScriptText(VALUE_CAS_SCRIPT_RESOURCE));
        repeatBlockCasScript = buildLongScript(loadScriptText(REPEAT_BLOCK_CAS_SCRIPT_RESOURCE));
        appPolicySingleCasScript = buildLongScript(loadScriptText(APP_POLICY_SINGLE_CAS_SCRIPT_RESOURCE));
        appPolicySnapshotCasScript = buildLongScript(loadScriptText(APP_POLICY_SNAPSHOT_CAS_SCRIPT_RESOURCE));
    }

    /**
     * value/version 구조 Hash를 CAS 규칙으로 갱신합니다.
     */
    public PolicySyncResult syncVersionedValue(String key, String value, long version) {
        return executeCas(
                requireScript(valueCasScript, "valueCasScript"),
                List.of(key),
                String.valueOf(version),
                value
        );
    }

    /**
     * 반복 차단 Hash 전체 스냅샷을 CAS 규칙으로 재작성합니다.
     */
    public PolicySyncResult syncRepeatBlockSnapshot(String repeatBlockKey, Map<String, String> repeatHash, long version) {
        String payloadJson = toJson(repeatHash == null ? Map.of() : repeatHash);
        return executeCas(
                requireScript(repeatBlockCasScript, "repeatBlockCasScript"),
                List.of(repeatBlockKey),
                String.valueOf(version),
                payloadJson
        );
    }

    /**
     * 앱 정책 단건을 app_data/app_speed/whitelist 키에 CAS 규칙으로 반영합니다.
     */
    public PolicySyncResult syncAppPolicySingle(
            String appDataKey,
            String appSpeedKey,
            String appWhitelistKey,
            int appId,
            boolean isActive,
            long dataLimit,
            int speedLimit,
            boolean isWhitelist,
            long version
    ) {
        return executeCas(
                requireScript(appPolicySingleCasScript, "appPolicySingleCasScript"),
                List.of(appDataKey, appSpeedKey, appWhitelistKey),
                String.valueOf(version),
                String.valueOf(appId),
                isActive ? "1" : "0",
                String.valueOf(dataLimit),
                String.valueOf(speedLimit),
                isWhitelist ? "1" : "0"
        );
    }

    /**
     * 앱 정책 스냅샷을 CAS 규칙으로 전체 재작성합니다.
     */
    public PolicySyncResult syncAppPolicySnapshot(
            String appDataKey,
            String appSpeedKey,
            String appWhitelistKey,
            Map<String, String> dataLimitHash,
            Map<String, String> speedLimitHash,
            Set<String> whitelistMembers,
            long version
    ) {
        return executeCas(
                requireScript(appPolicySnapshotCasScript, "appPolicySnapshotCasScript"),
                List.of(appDataKey, appSpeedKey, appWhitelistKey),
                String.valueOf(version),
                toJson(dataLimitHash == null ? Map.of() : dataLimitHash),
                toJson(speedLimitHash == null ? Map.of() : speedLimitHash),
                toJson(whitelistMembers == null ? List.of() : whitelistMembers)
        );
    }

    /**
     * 공통 CAS Lua 실행 래퍼입니다.
     */
    private PolicySyncResult executeCas(RedisScript<Long> script, List<String> keys, String... args) {
        try {
            Long rawResult = cacheStringRedisTemplate.execute(script, keys, (Object[]) args);
            if (rawResult == null) {
                return PolicySyncResult.RETRYABLE_FAILURE;
            }
            if (rawResult == 1L) {
                return PolicySyncResult.SUCCESS;
            }
            if (rawResult == 0L) {
                return PolicySyncResult.STALE_REJECTED;
            }
            return PolicySyncResult.RETRYABLE_FAILURE;
        } catch (DataAccessException e) {
            if (isConnectionFailure(e)) {
                return PolicySyncResult.CONNECTION_FAILURE;
            }
            return PolicySyncResult.RETRYABLE_FAILURE;
        }
    }

    /**
     * 연결 실패 계열 예외인지 판별합니다.
     */
    private boolean isConnectionFailure(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            if (current instanceof RedisConnectionFailureException) {
                return true;
            }

            String className = current.getClass().getSimpleName();
            String message = current.getMessage();
            if (className != null && className.toLowerCase().contains("connection")) {
                return true;
            }
            if (message != null) {
                String normalized = message.toLowerCase();
                if (normalized.contains("connection")
                        || normalized.contains("connect timed out")
                        || normalized.contains("connection refused")
                        || normalized.contains("unable to connect")) {
                    return true;
                }
            }
            current = current.getCause();
        }
        return false;
    }

    /**
     * Lua payload 전달을 위한 JSON 직렬화를 수행합니다.
     */
    private String toJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize Lua payload.", e);
        }
    }

    /**
     * classpath에서 Lua 스크립트 본문을 읽어옵니다.
     */
    private String loadScriptText(String resourcePath) {
        ClassPathResource resource = new ClassPathResource(resourcePath);
        try {
            return new String(resource.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to load Lua script text. resource=" + resourcePath, e);
        }
    }

    /**
     * 초기화 누락을 빠르게 감지하기 위해 스크립트 존재를 검증합니다.
     */
    private RedisScript<Long> requireScript(RedisScript<Long> script, String scriptName) {
        if (script == null) {
            throw new IllegalStateException("Lua script is not initialized. script=" + scriptName);
        }
        return script;
    }

    /**
     * Long 반환 스크립트 객체를 생성합니다.
     */
    private static RedisScript<Long> buildLongScript(String scriptText) {
        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        script.setScriptText(scriptText);
        script.setResultType(Long.class);
        return script;
    }
}
