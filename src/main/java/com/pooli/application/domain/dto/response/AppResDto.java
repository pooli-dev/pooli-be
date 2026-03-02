package com.pooli.application.domain.dto.response;

import com.pooli.application.domain.entity.ApplicationCategory;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AccessLevel;
import lombok.*;

@Builder
@Getter
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@Schema(description = "애플리케이션 조회 응답 DTO")
public class AppResDto {

	 @Schema(description = "애플리케이션 ID", example = "1")
     private Integer appId;

     @Schema(description = "애플리케이션 이름", example = "유튜브")
     private String appName;

     @Schema(description = "애플리케이션 카테고리", example = "SNS")
     private ApplicationCategory category;
}
