package com.pooli.traffic.service;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Profile;
import org.springframework.core.io.ClassPathResource;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pooli.common.exception.ApplicationException;
import com.pooli.common.exception.CommonErrorCode;
import com.pooli.traffic.domain.TrafficLuaExecutionResult;
import com.pooli.traffic.domain.dto.response.TrafficLuaDeductResDto;
import com.pooli.traffic.domain.enums.TrafficLuaScriptType;
import com.pooli.traffic.domain.enums.TrafficRefillGateStatus;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * 트래픽 차감 Lua 스크립트의 preload(SHA)와 실행을 담당하는 인프라 서비스입니다.
 * 스크립트별 실행 메서드를 제공해 오케스트레이터가 일관된 계약으로 Lua를 호출할 수 있게 합니다.
 */
@Slf4j
@Service
@Profile({"local", "traffic"})
@RequiredArgsConstructor
public class TrafficLuaScriptInfraService {

    // Lua를 실행할 Redis 인스턴스(트래픽 정책/카운터 저장소)
    @Qualifier("cacheStringRedisTemplate")
    private final StringRedisTemplate cacheStringRedisTemplate;
    // 차감 결과 JSON을 DTO로 역직렬화할 때 사용하는 매퍼
    private final ObjectMapper objectMapper;

    // preload 결과로 받은 SHA1을 기록해 운영 로그/점검에 활용한다.
    private final Map<TrafficLuaScriptType, String> scriptShaRegistry =
            new EnumMap<>(TrafficLuaScriptType.class);

    // 단일 문자열 결과를 반환하는 스크립트 레지스트리
    private final Map<TrafficLuaScriptType, RedisScript<String>> stringScriptRegistry =
            new EnumMap<>(TrafficLuaScriptType.class);

    // 단일 숫자 결과를 반환하는 스크립트 레지스트리
    private final Map<TrafficLuaScriptType, RedisScript<Long>> longScriptRegistry =
            new EnumMap<>(TrafficLuaScriptType.class);

    /**
     * Preloads all configured Lua scripts: loads script text, registers RedisScript instances,
     * and loads each script into Redis to store its SHA for subsequent EVALSHA execution.
     *
     * <p>This runs at startup to minimize first-call latency by ensuring scripts are cached
     * both in the JVM and in Redis.</p>
     *
     * @throws ApplicationException if loading, registering, or preloading any script fails
     */
    @PostConstruct
    /**
      * `preloadScripts` 처리 목적에 맞는 핵심 로직을 수행합니다.
     */
    public void preloadScripts() {
        // 애플리케이션 시작 시 모든 Lua를 메모리에 올리고 Redis에 SCRIPT LOAD 해둔다.
        // 이후 실행 시 EVALSHA 경로를 우선 사용해 첫 호출 지연을 줄인다.
        for (TrafficLuaScriptType scriptType : TrafficLuaScriptType.values()) {
            String scriptText = loadScriptText(scriptType);
            registerScript(scriptType, scriptText);
            String sha = preloadScriptSha(scriptType, scriptText);

            log.info("traffic_lua_script_preloaded script={} sha={}", scriptType.getScriptName(), sha);
        }
    }

    /**
     * Execute the individual-pool deduction Lua script.
     *
     * The caller must supply `keys` and `args` in the specific order expected by the script.
     *
     * @param keys the Redis KEYS array for the script, in the order defined by the caller
     * @param args the script ARGV array, in the order defined by the caller
     * @return a TrafficLuaExecutionResult containing the script's parsed answer and status
     */
    public TrafficLuaExecutionResult executeDeductIndivTick(List<String> keys, List<String> args) {
        String rawJson = executeStringSingle(TrafficLuaScriptType.DEDUCT_INDIV_TICK, keys, args);
        return parseDeductResult(rawJson, TrafficLuaScriptType.DEDUCT_INDIV_TICK);
    }

    /**
     * Execute the shared-pool deduction Lua script and return its parsed result.
     *
     * @param keys redis KEYS for the script, provided in the order required by the script
     * @param args redis ARGV values for the script, provided in the order required by the script
     * @return the script execution result parsed into a TrafficLuaExecutionResult (contains the answer and status)
     */
    public TrafficLuaExecutionResult executeDeductSharedTick(List<String> keys, List<String> args) {
        String rawJson = executeStringSingle(TrafficLuaScriptType.DEDUCT_SHARED_TICK, keys, args);
        return parseDeductResult(rawJson, TrafficLuaScriptType.DEDUCT_SHARED_TICK);
    }

    /**
     * Determine the refill gate status for a lock based on current amount and threshold.
     *
     * @param lockKey      the Redis key identifying the lock
     * @param traceId      tracing identifier passed to the Lua script for observability
     * @param lockTtlMs    lock time-to-live in milliseconds used by the script
     * @param currentAmount current token/amount observed when evaluating the gate
     * @param threshold    threshold value that determines refill behavior
     * @return             the refill gate status (`TrafficRefillGateStatus`) such as FAIL, SKIP, OK, or WAIT
     * @throws ApplicationException if the script returns an unrecognized status or script execution fails
     */
    public TrafficRefillGateStatus executeRefillGate(
            String lockKey,
            String traceId,
            long lockTtlMs,
            long currentAmount,
            long threshold
    ) {
        // refill gate는 단일 문자열 상태(FAIL/SKIP/OK/WAIT)를 반환한다.
        String statusText = executeStringSingle(
                TrafficLuaScriptType.REFILL_GATE,
                List.of(lockKey),
                List.of(
                        traceId,
                        String.valueOf(lockTtlMs),
                        String.valueOf(currentAmount),
                        String.valueOf(threshold)
                )
        );

        try {
            return TrafficRefillGateStatus.valueOf(statusText);
        } catch (IllegalArgumentException e) {
            throw new ApplicationException(
                    CommonErrorCode.INTERNAL_SERVER_ERROR,
                    "refill gate 상태 파싱에 실패했습니다. status=" + statusText
            );
        }
    }

    /**
     * Send a heartbeat to the lock Lua script and indicate whether the heartbeat succeeded.
     *
     * @param lockKey   the Redis key identifying the lock
     * @param traceId   a trace identifier forwarded to the script for logging/tracing
     * @param lockTtlMs the lock time-to-live in milliseconds to refresh
     * @return          `true` if the script returned `1` indicating success, `false` otherwise
     */
    public boolean executeLockHeartbeat(String lockKey, String traceId, long lockTtlMs) {
        // lock heartbeat는 1/0을 반환하므로 1이면 성공(true)으로 변환한다.
        Long rawResult = executeLongSingle(
                TrafficLuaScriptType.LOCK_HEARTBEAT,
                List.of(lockKey),
                List.of(traceId, String.valueOf(lockTtlMs))
        );

        return rawResult == 1L;
    }

    /**
     * Attempt to release a distributed lock and indicate whether the release succeeded.
     *
     * @param lockKey the Redis key identifying the lock
     * @param traceId identifier used for tracing/log correlation passed to the script
     * @return `true` if the lock was released (script returned 1), `false` otherwise
     */
    public boolean executeLockRelease(String lockKey, String traceId) {
        // lock release 역시 1/0 계약이므로 1이면 실제 해제 성공으로 판단한다.
        Long rawResult = executeLongSingle(
                TrafficLuaScriptType.LOCK_RELEASE,
                List.of(lockKey),
                List.of(traceId)
        );

        return rawResult == 1L;
    }

    /**
     * Retrieve the preloaded SHA1 fingerprint associated with the given Lua script type.
     *
     * @param scriptType the Lua script type to look up
     * @return the SHA1 string for the script if preloaded, or {@code null} if not present
     */
    public String getPreloadedSha(TrafficLuaScriptType scriptType) {
        // 운영 점검/로그 목적의 조회 메서드다.
        return scriptShaRegistry.get(scriptType);
    }

    /**
     * Execute the given Lua script (expected to produce a single string) and return its result.
     *
     * @param scriptType the type of Lua script to execute
     * @param keys       Redis keys to pass to the script
     * @param args       arguments to pass to the script
     * @return           the string result produced by the script
     * @throws ApplicationException if the script returns a null/empty result or if execution fails due to an external/Redis error
     */
    private String executeStringSingle(TrafficLuaScriptType scriptType, List<String> keys, List<String> args) {
        RedisScript<String> script = requireStringScript(scriptType);

        try {
            // 문자열 단일값 계약을 그대로 반환한다.
            String result = cacheStringRedisTemplate.execute(script, keys, args.toArray());
            if (result == null) {
                throw new ApplicationException(CommonErrorCode.INTERNAL_SERVER_ERROR, "Lua 실행 결과가 비어 있습니다.");
            }

            return result;
        } catch (DataAccessException e) {
            log.error("traffic_lua_execute_failed script={}", scriptType.getScriptName(), e);
            throw new ApplicationException(CommonErrorCode.EXTERNAL_SYSTEM_ERROR, "Lua 실행에 실패했습니다.");
        }
    }

    /**
     * Execute a registered Lua script that returns a single numeric result.
     *
     * @param scriptType the type of Lua script to execute
     * @param keys       the Redis keys to pass to the script
     * @param args       the script arguments
     * @return           the numeric result returned by the script (typically 1 or 0 for boolean-like outcomes)
     * @throws ApplicationException if the script result is null or if execution fails due to Redis/data-access errors
     */
    private Long executeLongSingle(TrafficLuaScriptType scriptType, List<String> keys, List<String> args) {
        RedisScript<Long> script = requireLongScript(scriptType);

        try {
            // 숫자 단일값 계약(주로 1/0)을 Long으로 받아 처리한다.
            Long result = cacheStringRedisTemplate.execute(script, keys, args.toArray());
            if (result == null) {
                throw new ApplicationException(CommonErrorCode.INTERNAL_SERVER_ERROR, "Lua 실행 결과가 비어 있습니다.");
            }

            return result;
        } catch (DataAccessException e) {
            log.error("traffic_lua_execute_failed script={}", scriptType.getScriptName(), e);
            throw new ApplicationException(CommonErrorCode.EXTERNAL_SYSTEM_ERROR, "Lua 실행에 실패했습니다.");
        }
    }

    /**
         * Validate and convert raw Lua deduction JSON into a TrafficLuaExecutionResult.
         *
         * @param rawJson    the raw JSON string produced by the Lua script
         * @param scriptType the script type (used in error messages for context)
         * @return           a TrafficLuaExecutionResult containing the parsed `answer` and `status`
         * @throws ApplicationException if `rawJson` is null or blank, if JSON parsing fails, or if the parsed `status` is null
         */
    private TrafficLuaExecutionResult parseDeductResult(String rawJson, TrafficLuaScriptType scriptType) {
        // 반환값이 비어 있으면 차감 결과를 신뢰할 수 없으므로 즉시 실패 처리한다.
        if (rawJson == null || rawJson.isBlank()) {
            throw new ApplicationException(
                    CommonErrorCode.INTERNAL_SERVER_ERROR,
                    "Lua 차감 결과 파싱에 실패했습니다. script=" + scriptType.getScriptName()
            );
        }

        try {
            // 스크립트 JSON을 전용 DTO로 역직렬화해 answer/status 타입 안전성을 확보한다.
            TrafficLuaDeductResDto parsedResult = objectMapper.readValue(rawJson, TrafficLuaDeductResDto.class);
            if (parsedResult.getStatus() == null) {
                throw new ApplicationException(
                        CommonErrorCode.INTERNAL_SERVER_ERROR,
                        "Lua 상태 값이 비어 있습니다. script=" + scriptType.getScriptName()
                );
            }

            return TrafficLuaExecutionResult.builder()
                    .answer(parsedResult.getAnswer())
                    .status(parsedResult.getStatus())
                    .build();
        } catch (JsonProcessingException e) {
            throw new ApplicationException(
                    CommonErrorCode.INTERNAL_SERVER_ERROR,
                    "Lua JSON 파싱에 실패했습니다. script=" + scriptType.getScriptName()
            );
        }
    }

    /**
     * Retrieve the registered Redis script (returning a `String`) for the specified Lua script type.
     *
     * @param scriptType the Lua script type to retrieve
     * @return the registered {@code RedisScript<String>} for the given script type
     * @throws ApplicationException if no script is registered for the provided script type
     */
    private RedisScript<String> requireStringScript(TrafficLuaScriptType scriptType) {
        RedisScript<String> script = stringScriptRegistry.get(scriptType);
        if (script == null) {
            throw new ApplicationException(
                    CommonErrorCode.INTERNAL_SERVER_ERROR,
                    "등록되지 않은 Lua 스크립트입니다. script=" + scriptType.getScriptName()
            );
        }
        return script;
    }

    /**
     * Retrieve the registered Redis script that returns a `Long` for the given traffic Lua script type.
     *
     * @param scriptType the TrafficLuaScriptType to lookup
     * @return the registered RedisScript<Long> for the provided script type
     * @throws ApplicationException if no script is registered for the provided script type
     */
    private RedisScript<Long> requireLongScript(TrafficLuaScriptType scriptType) {
        RedisScript<Long> script = longScriptRegistry.get(scriptType);
        if (script == null) {
            throw new ApplicationException(
                    CommonErrorCode.INTERNAL_SERVER_ERROR,
                    "등록되지 않은 Lua 스크립트입니다. script=" + scriptType.getScriptName()
            );
        }
        return script;
    }

    /**
     * Register a Lua script into the appropriate registry so it can be executed with the correct return type.
     *
     * @param scriptType the enum value identifying which Lua script is being registered; determines whether the script
     *                   is stored as a string-returning or long-returning RedisScript
     * @param scriptText the Lua source code to register
     */
    private void registerScript(TrafficLuaScriptType scriptType, String scriptText) {
        // 스크립트 반환 계약에 따라 결과 타입을 분리 등록한다.
        switch (scriptType) {
            case DEDUCT_INDIV_TICK, DEDUCT_SHARED_TICK -> {
                DefaultRedisScript<String> redisScript = new DefaultRedisScript<>();
                redisScript.setScriptText(scriptText);
                redisScript.setResultType(String.class);
                stringScriptRegistry.put(scriptType, redisScript);
            }
            case REFILL_GATE -> {
                DefaultRedisScript<String> redisScript = new DefaultRedisScript<>();
                redisScript.setScriptText(scriptText);
                redisScript.setResultType(String.class);
                stringScriptRegistry.put(scriptType, redisScript);
            }
            case LOCK_HEARTBEAT, LOCK_RELEASE -> {
                DefaultRedisScript<Long> redisScript = new DefaultRedisScript<>();
                redisScript.setScriptText(scriptText);
                redisScript.setResultType(Long.class);
                longScriptRegistry.put(scriptType, redisScript);
            }
        }
    }

    /**
     * Preloads the given Lua script into Redis, stores its SHA1 in the registry, and returns the SHA1.
     *
     * @param scriptType the enum value identifying the script and the registry key to store the SHA
     * @param scriptText the Lua script source to load into Redis
     * @return the SHA1 string returned by Redis for the loaded script
     * @throws ApplicationException if Redis returns an empty SHA or a data access error occurs during preload
     */
    private String preloadScriptSha(TrafficLuaScriptType scriptType, String scriptText) {
        try {
            // execute 오버로드 모호성을 피하기 위해 RedisCallback 타입을 명시한다.
            // scriptLoad는 SHA1 문자열을 반환하므로 그대로 보관한다.
            String sha = cacheStringRedisTemplate.execute((RedisCallback<String>) connection ->
                    connection.scriptingCommands().scriptLoad(scriptText.getBytes(StandardCharsets.UTF_8))
            );

            if (sha == null || sha.isBlank()) {
                throw new ApplicationException(
                        CommonErrorCode.EXTERNAL_SYSTEM_ERROR,
                        "Lua SHA preload 결과가 비어 있습니다. script=" + scriptType.getScriptName()
                );
            }

            scriptShaRegistry.put(scriptType, sha);
            return sha;
        } catch (DataAccessException e) {
            log.error("traffic_lua_preload_failed script={}", scriptType.getScriptName(), e);
            throw new ApplicationException(CommonErrorCode.EXTERNAL_SYSTEM_ERROR, "Lua SHA preload에 실패했습니다.");
        }
    }

    /**
     * Load and return the Lua script source text for the given script type.
     *
     * @param scriptType identifies which Lua script resource to load
     * @return the UTF-8 text contents of the Lua script resource
     * @throws ApplicationException if the script resource cannot be read
     */
    private String loadScriptText(TrafficLuaScriptType scriptType) {
        ClassPathResource resource = new ClassPathResource(scriptType.getResourcePath());

        try {
            // classpath에 등록된 Lua 원문을 문자열로 로드한다.
            return new String(resource.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new ApplicationException(
                    CommonErrorCode.INTERNAL_SERVER_ERROR,
                    "Lua 스크립트 로드에 실패했습니다. script=" + scriptType.getScriptName()
            );
        }
    }

}
