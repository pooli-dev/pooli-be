package com.pooli.traffic.service.outbox.strategy;

import com.pooli.traffic.domain.outbox.OutboxRetryResult;
import com.pooli.traffic.service.outbox.PolicySyncResult;

/**
 * 정책 동기화 결과를 Outbox 재시도 결과로 변환하는 공통 베이스 클래스입니다.
 */
abstract class AbstractPolicyOutboxRetryStrategy implements OutboxEventRetryStrategy {

    protected OutboxRetryResult mapPolicySyncResult(PolicySyncResult syncResult) {
        if (syncResult == PolicySyncResult.SUCCESS
                || syncResult == PolicySyncResult.STALE_REJECTED) {
            return OutboxRetryResult.SUCCESS;
        }
        return OutboxRetryResult.FAIL;
    }
}
