package com.pooli.traffic.service;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Profile;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pooli.common.config.AppStreamsProperties;
import com.pooli.traffic.domain.dto.request.TrafficPayloadReqDto;
import com.pooli.traffic.domain.dto.response.TrafficDeductResultResDto;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Streams BLOCK 소비 루프를 실행하는 러너입니다.
 * poller 스레드는 레코드를 읽고, worker 풀은 레코드 처리 로직을 병렬 수행합니다.
 * worker는 payload 역직렬화 후 10-tick 오케스트레이터를 호출합니다.
 */
@Slf4j
@Component
@Profile({"local", "traffic"})
@RequiredArgsConstructor
public class TrafficStreamConsumerRunner implements SmartLifecycle {

    // Streams read/ack/DLQ 인프라 유틸
    private final TrafficStreamInfraService trafficStreamInfraService;
    // app.streams.* 설정값
    private final AppStreamsProperties appStreamsProperties;
    // payload JSON 역직렬화 도구
    private final ObjectMapper objectMapper;
    // 10-tick 차감 오케스트레이션 서비스
    private final TrafficDeductOrchestratorService trafficDeductOrchestratorService;
    // in-flight dedupe 선점 서비스(traceId 기준)
    private final TrafficInFlightDedupeService trafficInFlightDedupeService;
    // DONE 영속화 서비스(traceId UNIQUE idempotency)
    private final TrafficDeductDonePersistenceService trafficDeductDonePersistenceService;
    // pending reclaim/retry/DLQ 분기 서비스
    private final TrafficStreamReclaimService trafficStreamReclaimService;

    // 전역적인 소비 루프 동작 여부 플래그(start/stop 간 공유)
    private final AtomicBoolean running = new AtomicBoolean(false);
    // BLOCK read 전용 단일 poller 실행기
    private ExecutorService pollerExecutor;
    // 레코드 처리 병렬 수행용 worker 실행기
    private ExecutorService workerExecutor;
    // pending reclaim 주기 실행기
    private ScheduledExecutorService reclaimExecutor;

    /**
     * Initialize and start the traffic stream consumer when consumption is enabled.
     *
     * Ensures the consumer group exists, creates the poller and worker executors, sets the running flag,
     * submits the blocking consume loop, starts the reclaim loop, and logs startup details.
     */
    @Override
    /**
     * 애플리케이션 시작 시점에 필요한 초기화 작업을 수행합니다.
     */
    public void start() {
        // 오케스트레이터가 아직 연결되지 않은 환경에서 소비기를 켜면
        // 메시지가 불완전 처리될 수 있으므로, 설정값으로 기동 여부를 먼저 확인한다.
        if (!appStreamsProperties.isConsumerEnabled()) {
            log.info("traffic_stream_consumer_disabled enabled=false");
            return;
        }

        // 읽기 전 Consumer Group 존재를 보장한다.
        // 이미 생성된 그룹이면 infra 서비스에서 안전하게 무시 처리한다.
        trafficStreamInfraService.ensureConsumerGroup();

        int workerThreadCount = resolveWorkerThreadCount();

        // poller는 BLOCK read 전용이므로 단일 스레드면 충분하다.
        pollerExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread thread = new Thread(r, "traffic-stream-poller");
            // traffic 프로파일(web-application-type=none)에서는
            // non-daemon 워커가 최소 1개는 있어야 JVM이 즉시 종료되지 않는다.
            thread.setDaemon(false);
            return thread;
        });

        // 레코드 처리는 worker 풀에서 병렬 수행한다.
        workerExecutor = Executors.newFixedThreadPool(workerThreadCount, r -> {
            Thread thread = new Thread(r, "traffic-stream-worker");
            thread.setDaemon(false);
            return thread;
        });

        // running 플래그를 먼저 켠 뒤 루프를 제출해
        // 제출 직후 루프가 즉시 종료되는 경쟁 상태를 피한다.
        running.set(true);
        pollerExecutor.submit(this::consumeLoop);
        startReclaimLoop();
        log.info(
                "traffic_stream_consumer_started group={} consumer={} workerThreads={}",
                appStreamsProperties.getGroupTraffic(),
                appStreamsProperties.getConsumerName(),
                workerThreadCount
        );
    }

    /**
     * Stops the consumer, signals shutdown, and terminates the poller, worker, and reclaim executors.
     *
     * <p>Sets the running flag to false and shuts down active executors to interrupt blocking reads
     * and halt in-progress or queued tasks.</p>
     */
    @Override
    /**
     * 애플리케이션 종료 시점에 실행 중인 리소스를 안전하게 정리합니다.
     */
    public void stop() {
        // 루프가 다음 사이클에서 종료되도록 먼저 상태를 내린다.
        running.set(false);

        // BLOCK read 대기를 빠르게 종료하기 위해 poller를 먼저 interrupt한다.
        if (pollerExecutor != null) {
            pollerExecutor.shutdownNow();
        }

        // 진행 중/대기 중인 처리 태스크를 함께 종료시킨다.
        if (workerExecutor != null) {
            workerExecutor.shutdownNow();
        }

        // reclaim 보조 루프도 함께 중단한다.
        if (reclaimExecutor != null) {
            reclaimExecutor.shutdownNow();
        }
        log.info("traffic_stream_consumer_stopped");
    }

    /**
     * Indicates whether the consumer runner is currently running.
     *
     * @return true if the runner is running, false otherwise.
     */
    @Override
    /**
     * 현재 상태를 불리언 값으로 확인해 호출 측의 분기 판단을 돕습니다.
     */
    public boolean isRunning() {
        return running.get();
    }

    /**
     * Indicates that this lifecycle component should start automatically.
     *
     * @return true if the component should start automatically, false otherwise.
     */
    @Override
    /**
     * 현재 상태를 불리언 값으로 확인해 호출 측의 분기 판단을 돕습니다.
     */
    public boolean isAutoStartup() {
        return true;
    }

    /**
     * Defines the lifecycle phase used to order startup and shutdown of this component.
     *
     * @return the phase value; Integer.MAX_VALUE so the component starts last and stops first
     */
    @Override
    /**
     * 현재 설정/상태 값을 반환합니다.
     */
    public int getPhase() {
        return Integer.MAX_VALUE;
    }

    /**
     * Continuously performs blocking reads from the traffic stream and dispatches received records to the worker pool.
     *
     * The loop runs while the internal running flag is true; on each cycle it performs a blocking read and submits
     * any retrieved records for parallel processing. Exceptions are logged and the loop continues, except when the
     * runner is stopping in which case the loop exits quietly.
     */
    private void consumeLoop() {
        // SmartLifecycle stop() 호출 전까지 BLOCK read -> 레코드 처리 과정을 반복한다.
        while (running.get()) {
            try {
                List<MapRecord<String, String, String>> records = trafficStreamInfraService.readBlocking();

                // BLOCK read 특성상 대부분의 루프는 0건 또는 소수 레코드를 받는다.
                // 수신된 레코드는 worker 풀로 분배해 병렬 처리한다.
                for (MapRecord<String, String, String> record : records) {
                    dispatchRecord(record);
                }
            } catch (Exception e) {
                // shutdownNow()로 BLOCK read를 깨운 종료 경로에서는 오류 로그를 남기지 않는다.
                if (!running.get()) {
                    break;
                }
                // 루프를 중단하지 않고 다음 read 사이클로 복구를 시도한다.
                log.error("traffic_stream_consume_loop_failed", e);
            }
        }
    }

    /**
      * 수신 레코드를 적절한 처리 경로로 분배합니다.
     */
    private void dispatchRecord(MapRecord<String, String, String> record) {
        if (workerExecutor == null) {
            log.warn("traffic_stream_worker_not_ready recordId={}", record.getId().getValue());
            return;
        }

        try {
            // poller는 I/O(read)에 집중하고, 처리 비용이 있는 역직렬화/분기는 worker로 위임한다.
            workerExecutor.submit(() -> handleRecord(record));
        } catch (RejectedExecutionException e) {
            // 종료 과정에서 발생 가능한 submit 실패는 로그만 남기고 무해하게 처리한다.
            log.warn(
                    "traffic_stream_record_dispatch_rejected recordId={} running={}",
                    record.getId().getValue(),
                    running.get()
            );
        }
    }

    /**
     * Starts a single-threaded scheduled loop that periodically invokes {@code runReclaimCycle()}.
     *
     * The loop interval is taken from {@code appStreamsProperties.getReclaimIntervalMs()} with a minimum
     * of 1 millisecond and is scheduled with a fixed delay between executions.
     */
    private void startReclaimLoop() {
        long reclaimIntervalMs = Math.max(1L, appStreamsProperties.getReclaimIntervalMs());

        // reclaim은 주기적으로 pending 목록을 점검하는 보조 작업이므로 단일 스레드면 충분하다.
        reclaimExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "traffic-stream-reclaim");
            thread.setDaemon(false);
            return thread;
        });

        reclaimExecutor.scheduleWithFixedDelay(
                this::runReclaimCycle,
                reclaimIntervalMs,
                reclaimIntervalMs,
                TimeUnit.MILLISECONDS
        );
    }

    /**
     * Runs a single reclaim cycle that requeues reclaimed records for processing.
     *
     * <p>Invokes the reclaim service to obtain records eligible for reprocessing (e.g., exceeded retries)
     * and dispatches each reclaimed record to the worker queue. Any exception is logged and suppressed
     * so the main consumer loop is not interrupted.</p>
     */
    private void runReclaimCycle() {
        if (!running.get()) {
            return;
        }

        try {
            // reclaim 서비스가 idle/retry 기준으로 필터링한 레코드를 반환하면
            // 메인 소비 경로와 동일하게 worker 큐로 분배해 처리한다.
            List<MapRecord<String, String, String>> reclaimedRecords =
                    trafficStreamReclaimService.reclaimAndRouteExceededRetries();

            for (MapRecord<String, String, String> reclaimedRecord : reclaimedRecords) {
                dispatchRecord(reclaimedRecord);
            }
        } catch (Exception e) {
            // reclaim 실패가 메인 소비 루프를 멈추게 해서는 안 되므로 로그 후 다음 주기를 기다린다.
            log.error("traffic_stream_reclaim_cycle_failed", e);
        }
    }

    /**
     * Process a Redis stream record: validate payload and traceId, perform idempotency and in‑flight deduplication,
     * run the deduction orchestration, persist a DONE marker, acknowledge the record, or route invalid payloads to DLQ.
     *
     * <p>The method will write unrecoverable messages (missing/invalid payload or missing traceId) to the DLQ and acknowledge them.
     * If a record is already completed or cannot be claimed for in‑flight processing, it will be acknowledged or skipped as appropriate.
     * On successful orchestration the DONE state is persisted before acknowledging and releasing the in‑flight claim.
     * Deserialization errors are sent to DLQ and acknowledged; other runtime exceptions are logged and left unacknowledged for retry/reclaim.
     *
     * @param record the Redis stream record to process
     */
    private void handleRecord(MapRecord<String, String, String> record) {
        // DLQ/로그 추적을 위해 레코드 ID를 초기에 추출해 둔다.
        String recordId = record.getId().getValue();

        // 명세(field=payload)에 맞춰 payload 문자열을 가져온다.
        String payloadJson = trafficStreamInfraService.extractPayload(record);

        if (payloadJson == null || payloadJson.isBlank()) {
            // payload 자체가 없으면 이후 처리 불가능하므로 DLQ로 우회 후 ACK한다.
            trafficStreamInfraService.writeDlq(payloadJson, "payload 필드가 비어 있습니다.", recordId);
            trafficStreamInfraService.acknowledge(record.getId());
            return;
        }

        try {
            // JSON payload를 DTO로 역직렬화해 이후 오케스트레이터가 바로 사용할 수 있게 한다.
            TrafficPayloadReqDto payload = objectMapper.readValue(payloadJson, TrafficPayloadReqDto.class);

            // traceId가 없으면 dedupe 키를 만들 수 없고 재처리 제어가 불가능하므로
            // 복구 불가능한 메시지로 판단해 DLQ + ACK 처리한다.
            if (payload.getTraceId() == null || payload.getTraceId().isBlank()) {
                trafficStreamInfraService.writeDlq(payloadJson, "traceId가 비어 있습니다.", recordId);
                trafficStreamInfraService.acknowledge(record.getId());
                return;
            }

            // 이미 DONE 저장된 traceId면 재차감 없이 즉시 ACK해 재전달을 종료한다.
            if (trafficDeductDonePersistenceService.existsByTraceId(payload.getTraceId())) {
                log.info(
                        "traffic_stream_record_already_done recordId={} traceId={}",
                        recordId,
                        payload.getTraceId()
                );
                trafficStreamInfraService.acknowledge(record.getId());
                return;
            }

            // 동일 traceId에 대한 동시/중복 처리 경쟁을 막기 위해 in-flight 선점을 시도한다.
            // 선점 실패는 이미 다른 워커(또는 다른 인스턴스)가 처리 중이라는 의미이므로
            // 현재 레코드는 실행을 생략한다.
            boolean claimed = trafficInFlightDedupeService.tryClaim(payload.getTraceId());
            if (!claimed) {
                // 선점 실패 직후 DONE이 이미 저장됐는지 한 번 더 확인한다.
                // 다른 워커가 처리 완료했다면 ACK로 정리하고, 아니면 재전달에 맡긴다.
                if (trafficDeductDonePersistenceService.existsByTraceId(payload.getTraceId())) {
                    trafficStreamInfraService.acknowledge(record.getId());
                }
                log.info(
                        "traffic_stream_record_deduped recordId={} traceId={}",
                        recordId,
                        payload.getTraceId()
                );
                return;
            }

            // 10-tick 오케스트레이터를 실행해 개인풀/공유풀 차감 결과를 계산한다.
            TrafficDeductResultResDto result = trafficDeductOrchestratorService.orchestrate(payload);

            // DONE 저장을 먼저 수행한다.
            // - 신규 저장(1건) 또는 중복 저장(0건)은 모두 idempotent 성공으로 간주한다.
            // - 저장 예외가 발생하면 ACK를 하지 않아 재전달로 복구한다.
            boolean saved = trafficDeductDonePersistenceService.saveIfAbsent(payload, result);

            // "저장 성공 후 ACK" 규칙을 보장하기 위해 영속화 이후에만 ACK한다.
            trafficStreamInfraService.acknowledge(record.getId());

            // ACK까지 성공한 뒤 in-flight 키를 정리한다.
            trafficInFlightDedupeService.release(payload.getTraceId());

            log.info(
                    "traffic_stream_record_done recordId={} traceId={} persistResult={} finalStatus={} deducted={} remaining={} lastLuaStatus={}",
                    recordId,
                    result.getTraceId(),
                    saved ? "SAVED" : "DUPLICATE",
                    result.getFinalStatus(),
                    result.getDeductedTotalBytes(),
                    result.getApiRemainingData(),
                    result.getLastLuaStatus()
            );
        } catch (JsonProcessingException e) {
            // 스키마 불일치/JSON 파손은 재처리해도 복구가 어려우므로 DLQ로 분기한다.
            trafficStreamInfraService.writeDlq(payloadJson, "payload 역직렬화 실패", recordId);
            trafficStreamInfraService.acknowledge(record.getId());
        } catch (Exception e) {
            // 처리 중 시스템 예외는 ACK하지 않고 남겨 재전달/reclaim 경로에서 복구한다.
            log.error("traffic_stream_record_handle_failed recordId={}", recordId, e);
        }
    }

    /**
     * Determine the number of worker threads to use based on configuration.
     *
     * @return the configured worker thread count when greater than zero; otherwise 1
     */
    private int resolveWorkerThreadCount() {
        int configuredCount = appStreamsProperties.getWorkerThreadCount();
        if (configuredCount > 0) {
            return configuredCount;
        }

        // 잘못된 설정값(0 이하)이 들어오면 최소 1개 스레드로 안전하게 보정한다.
        return 1;
    }
}
