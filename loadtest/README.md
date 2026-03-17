# 부하 테스트 가이드

이 폴더는 `POST /api/traffic/requests` 엔드포인트에 대한 k6 부하 테스트 스크립트와 샘플 데이터, 결과 저장 위치를 포함합니다.

## 구성

- `loadtest/k6/traffic-mixed.js`
  - 메인 k6 스크립트
  - `normal`, `hot`, `refill`, `policy`, `mixed` 모드 지원
- `loadtest/k6/data/traffic-targets.sample.json`
  - 샘플 대상 데이터
  - 실제 AWS 테스트 전용 DB에 맞는 `lineId`, `familyId`, `appId`로 교체해서 사용
- `loadtest/results/`
  - `--summary-export` 결과 파일 저장 위치

## 어디서 실행해야 하나

- 로컬 PC
  - 스모크 테스트만 권장
  - 요청 형식, 연결, 응답 형태 확인용
- 별도 AWS runner EC2
  - 실제 부하 테스트 실행 위치
  - 테스트용 ALB와 같은 VPC 내부에서 실행 권장

주의:

- API 서버 EC2나 traffic worker EC2에서 k6를 같이 실행하면 안 됩니다.
- 생성기 CPU와 서버 CPU가 섞여서 TPS 결과가 왜곡됩니다.

## 빠른 시작

1. `loadtest/k6/data/traffic-targets.sample.json`의 ID를 실제 테스트 환경에 맞게 수정합니다.
2. 먼저 낮은 RPS로 스모크 테스트를 돌립니다.
3. 그 다음 AWS runner에서 단계적으로 RPS를 올립니다.

## 로컬 스모크 테스트

```bash
k6 run loadtest/k6/traffic-mixed.js -e BASE_URL=http://localhost:8080 -e FLOW_MODE=normal -e STAGES=5:1m,10:2m --summary-export loadtest/results/local-smoke.json
```

## AWS runner 실행 예시

```bash
k6 run loadtest/k6/traffic-mixed.js -e BASE_URL=http://test-api-alb.example.internal -e DATA_FILE=loadtest/k6/data/traffic-targets.sample.json -e FLOW_MODE=mixed -e STAGES=20:2m,50:5m,100:10m,150:10m -e PRE_ALLOCATED_VUS=200 -e MAX_VUS=1000 --summary-export loadtest/results/aws-mixed.json
```

## 환경 변수

- `BASE_URL`
  - 필수
  - 예: `http://localhost:8080`, `http://test-api-alb.example.internal`
- `ENDPOINT`
  - 기본값: `/api/traffic/requests`
- `DATA_FILE`
  - 기본값: `./data/traffic-targets.sample.json`
- `FLOW_MODE`
  - `mixed`, `normal`, `hot`, `refill`, `policy`
- `STAGES`
  - `목표RPS:지속시간` 형식
  - 예: `20:2m,50:5m,100:10m`
- `START_RATE`
  - 시작 RPS
  - 기본값: `20`
- `PRE_ALLOCATED_VUS`
  - 기본값: `100`
- `MAX_VUS`
  - 기본값: `500`
- `AUTH_TOKEN`
  - 추후 인증이 필요해질 경우 bearer token 전달용
- `HTTP_TIMEOUT`
  - 기본값: `30s`
- `WEIGHT_NORMAL`
  - mixed 모드에서 normal 비중
  - 기본값: `70`
- `WEIGHT_HOT`
  - mixed 모드에서 hot 비중
  - 기본값: `15`
- `WEIGHT_REFILL`
  - mixed 모드에서 refill 비중
  - 기본값: `10`
- `WEIGHT_POLICY`
  - mixed 모드에서 policy 비중
  - 기본값: `5`

## 샘플 데이터 설명

현재 샘플 파일은 이 레포의 로컬 인수 테스트 기준값을 사용합니다.

- `familyId=1`
- `lineId=1~4`
- `appId=1`, `appId=2`

실제 AWS 테스트에서는 반드시 테스트 DB에 존재하는 값으로 바꿔야 합니다.

- `normal`
  - 여러 회선을 넓게 분산하는 일반 트래픽
- `hot`
  - 일부 회선에 집중시켜 lock 경합과 hot key 상황을 만드는 트래픽
- `refill`
  - 잔액 부족 상태를 미리 세팅한 뒤 refill 경로를 자주 타게 하는 트래픽
- `policyHit`
  - 차단, 제한, whitelist 미일치 같은 정책 경로를 자주 타게 하는 트래픽

주의:

- 이 스크립트는 HTTP 요청만 보냅니다.
- DB 데이터 생성, Redis flush, 정책 활성화, 잔액 조정은 미리 별도 작업으로 준비해야 합니다.

## 어떤 지표를 봐야 하나

- API enqueue TPS
- API 응답 지연 시간
- `traffic_event_result_total` 기준 실제 완료 TPS
- `traffic_stream_pending_messages`
- `traffic_stream_length`
- RDS CPU, DB Load, Write Latency, lock wait
- ElastiCache CPU, eviction, latency

중요:

- 최종 TPS는 HTTP 200 TPS만으로 판단하면 안 됩니다.
- 이 서비스는 enqueue 후 worker가 실제 처리를 완료하므로, 최종 결론은 완료 이벤트 TPS 기준으로 봐야 합니다.

## 권장 실행 순서

1. 로컬 스모크
2. AWS에서 `normal` 단독 테스트
3. AWS에서 `hot`, `refill`, `policy` 단독 테스트
4. AWS에서 `mixed` 통합 테스트
5. 최대 후보 TPS에서 30분 이상 soak 테스트

## 참고

- 스크립트 문법 확인:

```bash
k6 inspect loadtest/k6/traffic-mixed.js -e BASE_URL=http://localhost:8080
```

- 예시 결과 파일:
  - `loadtest/results/local-smoke.json`
  - `loadtest/results/aws-mixed.json`
