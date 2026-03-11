package com.pooli.traffic.controller;

import org.springframework.context.annotation.Profile;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.pooli.traffic.domain.dto.request.TrafficGenerateReqDto;
import com.pooli.traffic.domain.dto.response.TrafficGenerateResDto;
import com.pooli.traffic.service.TrafficRequestEnqueueService;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;

/**
 * 트래픽 발생 요청을 수신하는 API 엔드포인트입니다.
 * 본 컨트롤러는 API 서버 역할(local/api profile)에서만 활성화됩니다.
 */
@Tag(name = "Traffic", description = "트래픽 발생 API")
@Validated
@RestController
@Profile({"local", "api"})
@RequiredArgsConstructor
@RequestMapping("/api/traffic")
public class TrafficController {

    private final TrafficRequestEnqueueService trafficRequestEnqueueService;

    /**
     * Enqueues a traffic generation request into Redis Streams and returns the generated trace identifier.
     *
     * @param request the traffic generation request payload containing parameters for the traffic to produce
     * @return the response DTO containing the generated `traceId`
     */
    @Operation(
            summary = "트래픽 발생 요청 적재",
            description = "요청 정보를 Redis Streams에 적재하고 traceId를 반환합니다."
    )
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "적재 성공"),
            @ApiResponse(responseCode = "500", description = "Streams 적재 실패")
    })
    @PostMapping("/requests")
    public ResponseEntity<TrafficGenerateResDto> enqueueTraffic(@RequestBody TrafficGenerateReqDto request) {
        return ResponseEntity.ok(trafficRequestEnqueueService.enqueue(request));
    }
}
