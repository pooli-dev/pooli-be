package com.pooli.traffic.mapper;

import java.util.List;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import com.pooli.traffic.domain.outbox.RedisOutboxRecord;

/**
 * Redis Outbox 테이블 접근을 담당하는 MyBatis Mapper입니다.
 */
@Mapper
public interface RedisOutboxMapper {

    /**
     * 신규 Outbox 레코드를 삽입합니다.
     */
    int insert(RedisOutboxRecord record);

    /**
     * 재시도 대상 레코드를 SKIP LOCK으로 잠금 조회합니다.
     */
    List<RedisOutboxRecord> selectRetryCandidatesForUpdate(
            @Param("limit") int limit,
            @Param("pendingDelaySeconds") int pendingDelaySeconds,
            @Param("processingStuckSeconds") int processingStuckSeconds
    );

    /**
     * 레코드를 PROCESSING 상태로 전이합니다.
     */
    int markProcessing(@Param("id") Long id);

    /**
     * 레코드를 SUCCESS 상태로 전이합니다.
     */
    int markSuccess(@Param("id") Long id);

    /**
     * 레코드를 FAIL 상태로 전이합니다(재시도 횟수 유지).
     */
    int markFail(@Param("id") Long id);

    /**
     * 레코드를 FAIL 상태로 전이하고 재시도 횟수를 1 증가시킵니다.
     */
    int markFailWithRetryIncrement(@Param("id") Long id);

    /**
     * 레코드를 FAIL 상태로 전이하고 재시도 횟수를 지정 값으로 설정합니다.
     */
    int markFailWithRetryCount(
            @Param("id") Long id,
            @Param("retryCount") int retryCount
    );

    /**
     * REFILL 레코드를 REVERT로 CAS 전이합니다.
     * 이미 터미널 상태(SUCCESS/REVERT)면 0을 반환합니다.
     */
    int markRevertIfCompensable(@Param("id") Long id);
}
