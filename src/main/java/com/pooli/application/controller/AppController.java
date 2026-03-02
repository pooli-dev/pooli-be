package com.pooli.application.controller;

import java.util.List;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.pooli.application.domain.dto.response.AppResDto;
import com.pooli.application.domain.entity.ApplicationCategory;
import com.pooli.application.service.AppService;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;

@Tag(name = "application", description = "애플리케이션 관련 API")
@RestController
@RequestMapping("/api/apps")
@RequiredArgsConstructor
public class AppController {

      private final AppService appService;
	  @Operation(
          summary = "애플리케이션 목록 조회",
          description = "애플리케이션 ID 기준 등록된 애플리케이션 목록을 모두 조회한다."
      )
      @ApiResponses({
          @ApiResponse(responseCode = "200", description = "조회 성공"),
          @ApiResponse(responseCode = "404", description = "앱 정보가 존재하지 않음"),
          @ApiResponse(responseCode = "500", description = "서버 오류"),
            
      })
      @GetMapping
      public ResponseEntity<List<AppResDto>> getApps(
              @Parameter(description = "앱 카테고리", example = "SNS")
              @RequestParam(required = false) ApplicationCategory category,
              @Parameter(description = "앱 이름 검색 키워드", example = "유튜")
              @RequestParam(required = false) String keyword) {
            return ResponseEntity.ok(appService.getApps(category, keyword));
      }
}
