# TrafficFlowLocalAcceptanceTest 추가 테스트 케이스 설계서

## 1. 개요
본 문서는 TrafficFlow 시스템의 안정성 및 정책 준수 여부를 검증하기 위한 상세 테스트 케이스를 정의합니다. 단일 정책 검증부터 복합 정책, 풀(Pool) 연계 차감, 엣지 케이스까지 총 **30개의 테스트 케이스**를 통해 시스템의 신뢰성을 확보하는 것을 목적으로 합니다.

---

## 2. 정책 및 우선순위 기준 (참조)

### 2.1 정책 ID 매핑
| ID | 상수명 | 설명 |
| :--- | :--- | :--- |
| 1 | `POLICY_REPEAT_BLOCK` | 반복 차단 정책 |
| 2 | `POLICY_IMMEDIATE_BLOCK` | 즉시 차단 정책 |
| 3 | `POLICY_LINE_LIMIT_SHARED` | 월별 공유풀 사용 한도 |
| 4 | `POLICY_LINE_LIMIT_DAILY` | 일별 데이터 사용 한도 |
| 5 | `POLICY_APP_DATA` | 앱별 일일 데이터 한도 |
| 6 | `POLICY_APP_SPEED` | 앱별 일일 속도 제한 |
| 7 | `POLICY_APP_WHITELIST` | 화이트리스트 정책 |

### 2.2 Lua 정책 평가 우선순위
시스템은 아래의 순서에 따라 정책을 평가하며, 상위 정책에서 결과가 결정될 경우 하위 정책은 평가되지 않습니다.
> **Whitelist** → **Immediate** → **Repeat** → **Daily** → **Monthly Shared** (공유풀 전용) → **App Daily** → **App Speed**

### 2.3 데이터 차감 흐름 (Orchestrator)
1. **개인풀 차감**: 우선적으로 개인 잔량에서 차감을 시도합니다.
2. **공유풀 보완**: 개인풀 잔량이 부족할 경우, 부족분(residual)만큼 공유풀에서 추가 차감을 수행합니다.

---

## 3. 상세 테스트 케이스 설계

### A. 단일 정책 검증 (Single Policy)
각 정책의 활성화 상태에 따른 기본적인 차단 및 허용 동작을 검증합니다.

| ID | 테스트 항목 | 시나리오 | 기대 결과 |
| :-- | :--- | :--- | :--- |
| 1 | `shouldBlockWhenRepeatBlockPolicyIsActive` | 현재 요일/시각이 차단 구간에 포함된 상태에서 차감 요청 | `deductedTotalBytes=0`, `lastLuaStatus=BLOCKED_REPEAT` |
| 2 | `shouldBypassRepeatBlockWhenPolicyIsDisabled` | 차단 구간 설정 중이나 전역 정책(ID=1) 비활성화 시 | 정상 차감 (`SUCCESS`) |
| 3 | `shouldNotBlockWhenOutsideRepeatBlockTimeRange` | 차단 구간 이외의 시각에 요청 시 | 정상 차감 (`SUCCESS`) |
| 4 | `shouldRespectAppDailyDataLimitWhenPolicyIsActive` | 앱(ID=1) 한도 30 설정 후 50 요청 | `deductedTotalBytes=30`, `lastLuaStatus=HIT_APP_DAILY_LIMIT` |
| 5 | `shouldBypassAppDailyDataLimitWhenPolicyIsDisabled` | 앱 한도 설정 중이나 전역 정책(ID=5) 비활성화 시 | `deductedTotalBytes=50`, `SUCCESS` |
| 6 | `shouldRespectAppSpeedLimitWhenPolicyIsActive` | 앱(ID=1) 속도 한도 초과 상태에서 요청 | `deductedTotalBytes=0`, `lastLuaStatus=HIT_APP_SPEED` |
| 7 | `shouldBypassAppSpeedLimitWhenPolicyIsDisabled` | 속도 한도 초과 상태이나 전역 정책(ID=6) 비활성화 시 | 정상 차감 (`SUCCESS`) |
| 8 | `shouldRespectMonthlySharedLimitWhenPolicyIsActive` | 공유풀 한도 40 설정, 개인풀 0 상태에서 50 요청 | 공유풀 40 차감, `lastLuaStatus=HIT_MONTHLY_SHARED_LIMIT` |
| 9 | `shouldBypassMonthlySharedLimitWhenPolicyIsDisabled` | 공유풀 한도 설정 중이나 전역 정책(ID=3) 비활성화 시 | 공유풀 50 전체 차감 (`SUCCESS`) |

> **비고**: 반복 차단 테스트는 `REPEAT_BLOCK_DAY`를 현재 시각 기준으로 동적 설정해야 하며, 속도 제한 테스트는 Redis 버킷 값을 사전에 임계치 이상으로 설정해야 합니다.

### B. 복합 정책 검증 (Composite Policy)
복수의 정책이 중첩될 때 Lua 엔진의 우선순위가 올바르게 적용되는지 검증합니다.

| ID | 테스트 항목 | 시나리오 | 기대 결과 |
| :-- | :--- | :--- | :--- |
| 10 | `shouldHitAppDailyLimitBeforeAppSpeed` | 앱 데이터 한도(30) 및 속도 한도 동시 설정 후 50 요청 | 데이터 한도 우선 적용, `30` 차감, `HIT_APP_DAILY_LIMIT` |
| 11 | `shouldHitAppSpeedWhenDataLimitSufficient` | 데이터 한도 충분, 속도 한도 초과 상태에서 50 요청 | 속도 한도 적용, `0` 차감, `HIT_APP_SPEED` |
| 12 | `shouldHitDailyLimitBeforeAppDailyLimit` | 일일 한도(20), 앱별 한도(40) 설정 후 50 요청 | 일일 한도 우선 적용, `20` 차감, `HIT_DAILY_LIMIT` |
| 13 | `shouldHitAppDailyLimitWhenDailyLimitSufficient` | 일일 한도 충분, 앱별 한도(25) 초과 요청 | 앱별 한도 적용, `25` 차감, `HIT_APP_DAILY_LIMIT` |
| 14 | `shouldBlockRepeatBeforeDailyLimit` | 반복 차단 구간 내 + 일일 한도 설정 상태에서 요청 | 반복 차단 우선 적용, `BLOCKED_REPEAT`, 사용량 미변경 |

### C. 화이트리스트 우회 검증 (Whitelist Bypass)
화이트리스트 정책이 모든 차단 및 한도 제한을 우회하는지 검증합니다.

| ID | 테스트 항목 | 시나리오 | 기대 결과 |
| :-- | :--- | :--- | :--- |
| 15 | `shouldBypassAllBlockPoliciesWhenWhitelisted` | 즉시/반복 차단 설정된 앱이 화이트리스트 등록됨 | 모든 차단 우회, 정상 차감 |
| 16 | `shouldBypassDailyAndAppLimitsWhenWhitelisted` | 일일/앱 한도 소진(0) 상태에서 화이트리스트 등록됨 | 모든 한도 우회, 정상 차감 |
| 17 | `shouldNotBypassWhenWhitelistPolicyIsDisabled` | 화이트리스트 등록 상태이나 전역 정책(ID=7) 비활성화 | 화이트리스트 무시, 기존 정책 적용 |
| 18 | `shouldApplyPoliciesForNonWhitelistedApp` | 앱 A(등록), 앱 B(미등록) 중 앱 B로 요청 시 | 앱 B는 기존 정책(일일 한도 등) 모두 적용 |

### D. 개인풀 + 공유풀 연계 차감 검증 (Pool Linkage)
오케스트레이터의 풀 전환 로직 및 공유풀 전용 정책 적용 여부를 검증합니다.

| ID | 테스트 항목 | 시나리오 | 기대 결과                                             |
| :-- | :--- | :--- |:--------------------------------------------------|
| 19 | `shouldDeductOnlyFromIndividualPool` | 개인풀 잔량 200 상태에서 50 요청 | 개인풀 50 차감, 공유풀 미사용, `SUCCESS`                     |
| 20 | `shouldDeductPartialFromIndividualPool` | 개인풀 잔량 200 상태에서 100 요청 | 개인풀 100 차감, `SUCCESS`                             |
| 21 | `shouldDeductAllIndivAndPartialShared` | 개인풀 30, 공유풀 100 상태에서 50 요청 | 개인 30 + 공유 20 차감, `SUCCESS`                       |
| 22 | `shouldDeductAllIndivAndAllShared` | 개인풀 30, 공유풀 20 상태에서 50 요청 | 개인 30 + 공유 20 차감, `SUCCESS`                       |
| 23 | `shouldReturnPartialWhenBothPoolsExhausted` | 개인풀 10, 공유풀 10 상태에서 50 요청 | 총 20 차감, `apiRemainingData=30`, `PARTIAL_SUCCESS` |
| 24 | `shouldApplyDailyLimitOnSharedPool` | 개인풀 0, 공유풀 전환 시 일일 한도 20 적용 중 50 요청 | 공유풀 20만 차감, `HIT_DAILY_LIMIT`                     |
| 25 | `shouldBlockSharedPoolByRepeatBlock` | 개인풀 0, 반복 차단 활성 상태에서 공유풀 차감 시도 | 공유풀 차단 차단, `BLOCKED_REPEAT`                       |

### E. 일일 사용량 누적 검증 (Cumulative Usage)
다중 요청 시퀀스에 따른 데이터 누적 계산의 정확성을 검증합니다.

| ID | 테스트 항목 | 시나리오 | 기대 결과 |
| :-- | :--- | :--- | :--- |
| 26 | `shouldAccumulateDailyUsageAcrossRequests` | 일일 한도 100, 요청 1(30) -> 2(30) -> 3(50) | 요청 3에서 잔여 40만 차감, `HIT_DAILY_LIMIT` |
| 27 | `shouldTrackDailyUsageSeparatelyPerPool` | 개인풀 차감 후 공유풀로 넘어가는 연속 요청 | 일일 사용량이 개인+공유 합산으로 정상 누적 확인 |

### F. 엣지 케이스 및 방어 검증 (Edge Case)
비정상 입력 또는 전역 정책 비활성화 상황에서의 시스템 거동을 확인합니다.

| ID | 테스트 항목 | 시나리오 | 기대 결과 |
| :-- | :--- | :--- | :--- |
| 28 | `shouldHandleZeroDataRequest` | `apiTotalData = 0`으로 차감 요청 시 | 차감 없이 `SUCCESS`, `deductedTotalBytes=0` |
| 29 | `shouldHandleExactBalanceRequest` | 개인풀 잔량 50 상태에서 50 요청 | 정확히 50 차감, 잔량 0, `SUCCESS` |
| 30 | `shouldHandleMultiplePoliciesAllDisabled` | 정책 설정 존재하나 전역 정책(1~7) 모두 비활성화 | 모든 정책 무시하고 정상 차감 수행 |

---

## 4. 테스트 요약
| 카테고리 | 테스트 수 | 주요 검증 포인트 |
| :--- | :---: | :--- |
| **A. 단일 정책** | 9개 | 각 정책의 활성/비활성 시 기본 동작 |
| **B. 복합 정책** | 5개 | 정책 간 우선순위 준수 여부 |
| **C. 화이트리스트** | 4개 | 특수 권한 부여 및 전역 스위치 동작 |
| **D. 풀 연계 차감** | 7개 | 개인풀 소진 시 공유풀 전환 및 정책 적용 |
| **E. 누적 검증** | 2개 | 연속 요청 시 일일 사용량 계산 정확성 |
| **F. 엣지 케이스** | 3개 | 경계값 및 비정상 상황 처리 능력 |
| **합계** | **30개** | |