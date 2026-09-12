# Kalshi 암호화폐 가격 산정 플랫폼

[English documentation](README.md)

실시간 BTC 예측 시장을 위한 프로덕션 시장 데이터 및 변동성 가격 산정
시스템입니다. 암호화폐 거래소와 Kalshi 피드를 수집하고 Redis에서 실시간
상태를 유지하며, BigQuery에 오프라인 학습 데이터를 구축합니다. 하이브리드
변동성 기간 구조를 제공하고 Dash에서 모델 확률과 호가 대비 엣지를
표시합니다.

이 시스템은 의사결정 지원 전용입니다. 주문을 제출하거나 포지션을 관리하지
않습니다.

## 데이터 흐름

```text
암호화폐 거래소 + Kalshi API
        |
        v
    ingestion ---------------------------> gcs_exporter -> GCS
        |                                      |
        v                                      v
      Redis -> aggregator -> live_feature_service     batch_etl -> BigQuery
        |                         |                         |
        |                         +-> feast-live-bridge     v
        |                                                   ml_pipeline
        |                                                        |
        |                                                  Vertex AI + GCS
        |                                                        |
        |            analytics -> model-serving <----------------+
        |                |
        +<---------------+-- 변동성 및 Kalshi 가격
        |
        v
     dashboard -> https://crypto-dashboard.kairos-trading.com
```

Redis Streams와 최신 상태 키는 실시간 통합 경계입니다. 동기 방식의 비즈니스
API는 내부 `POST /v1/forecast` 엔드포인트를 사용하는
`analytics -> model-serving` 호출뿐입니다. 전체 서비스 및 인터페이스 목록은
[ARCHITECTURE.md](ARCHITECTURE.md)를 참고하십시오.

## 서비스

| 디렉터리 | 역할 | 런타임 |
|---|---|---|
| `services/ingestion` | 거래소 및 Kalshi 이벤트를 정규화해 Redis Streams에 기록합니다. 생략된 Kalshi 오더북 한쪽 면은 빈 면으로 처리하고, 잘못된 값은 계속 거부합니다. | GKE Deployment `ingestion-service` |
| `services/aggregator` | 합성 BTC 현물 상태, 거래소별 표준 오더북 및 완료된 10초 거래소 프리미티브를 생성합니다. | GKE Deployment `aggregator` |
| `services/live_feature_service` | 프리미티브를 실시간 `market_features/v2_10s` 계약과 EWMA 상태로 변환합니다. | GKE Deployment `live-feature-service` |
| `services/gcs_exporter` | 정규화된 Redis Stream 5개를 파티션된 Parquet으로 GCS에 보관합니다. | GKE Deployment `gcs-exporter` |
| `services/batch_etl` | 원천 데이터를 적재하고 BigQuery에서 10초 바, 피처 및 미래 변동성 라벨을 생성합니다. | GKE CronJobs |
| `services/feast_store` | Feast 정의, 레지스트리, 온라인 쓰기, 선택적 피처 서빙 및 동등성 검증을 담당합니다. | GKE `feast-live-bridge` 및 Jobs/CronJobs. `feast-server`는 0개 replica로 유지됩니다. |
| `services/ml_pipeline` | 4개 주기의 변동성 모델 후보를 학습, 평가 및 등록합니다. | Vertex AI Pipelines 태스크 이미지 |
| `services/model_serving` | 5분/15분/30분 EWMA 예측과 이미지에 패키징된 프로모션 1시간 XGBoost 모델을 제공합니다. | GKE Deployment 및 Service `model-serving` |
| `services/analytics` | 실시간 현물, 피처, 예측 및 Kalshi 메타데이터로 활성 KXBTCD 계약 가격을 계산합니다. | GKE Deployment 및 Service `analytics` |
| `services/dashboard` | 실시간 BTC 상태, Kalshi 시장/모델 곡선, 공정가치, 미드포인트 엣지 및 체결 인식 활동 나이를 표시합니다. | GKE Deployment, Service 및 공개 Ingress `dashboard` |

각 서비스 README에는 정확한 입력, 출력, 설정, 개발 명령 및 배포 경계가
설명되어 있습니다.

## 표준 v2_10s 계약

현재 모델 계약은 `v2_10s`로 명확하게 버전 관리되며 레거시 v1 피처 정의를
재사용하지 않습니다.

| 계층 | 계약 |
|---|---|
| 원천 아카이브 | GCS에 파티션된 암호화폐 및 Kalshi Parquet |
| 표준 바 | BigQuery `market_data.bars`, 주기 `10s` |
| 오프라인 피처 | BigQuery `feature_store.realized_volatility_v2_10s` |
| 오프라인 타깃 | BigQuery `training_labels.future_realized_volatility_v2_10s` |
| 실시간 프리미티브 | Redis `stream:primitives:v1` |
| 실시간 피처 | Redis `market:features:v2_10s:BTCUSD:latest` |
| Feast 피처 뷰 | `market_features`, 버전 `v2_10s` |
| 변동성 출력 | Redis `market:volatility:v2_10s:BTCUSD:latest` |
| 계약 가격 | Redis `market:pricing:v1:<KXBTCD market ticker>` |

오프라인과 온라인 피처는 동일한 이벤트 시간 의미, 10초 주기, 이름 및 모델
입력값을 공유합니다. 동등성 검증 작업은 최근 BigQuery 행을 실시간 계산 결과
및 Feast 온라인 값과 비교합니다. 오프라인 라벨 테이블은 호환성을 위해
`target_rv_1m`을 유지하지만, 1분은 현재 학습 또는 서빙 대상 주기가 아닙니다.

추가형 `v3_10s` HAR 연구 계약은 오프라인, 실시간, Feast 및 ML 코드에
구현되어 있지만, 프로덕션 매니페스트와 서빙은 계속 `v2_10s`를 사용합니다.

## 예측 및 Kalshi 가격 산정

지원하는 예측 주기는 `5m`, `15m`, `30m`, `1h`입니다. 앞의 세 주기는 실시간
10초 분산 상태에서 계산한 결정론적 EWMA를 사용합니다. 1시간 예측은 CI가
메타데이터와 체크섬을 검증한 뒤 model-serving 이미지에 패키징한 변경 불가능
프로모션 XGBoost 아티팩트를 사용합니다. 따라서 실행 중인 model-serving
Pod는 Vertex AI, GCS, Redis, Feast 또는 Kalshi 접근 권한이 필요하지 않습니다.

각 활성 KXBTCD 계약에 대해 analytics는 만기까지 남은 시간을 분 단위로
계산하고 `[0,5)`, `[5,15)`, `[15,30)`, `[30,60)` 구간을 찾습니다. 이후
연환산 변동성을 선형 보간하고 그 값을 Gaussian 행사가 초과 확률 프라이서에
전달합니다. 확률, 공정가치 및 호가 엣지를 게시하지만 주문은 제출하지
않습니다.

대시보드는 Kalshi 모니터를 1초마다, 계약 테이블을 2초마다 갱신하며 모델
곡선을 노란색으로 표시합니다. `EDGE MID`는 항상
`Kalshi YES 미드포인트 - 모델 확률`입니다. 계약에 아직 체결이 없으면
`AGE`는 티커 또는 오더북 활동을 사용하고, 체결 이후에는 가장 최근의 서로
다른 가격/수량/방향 체결 조합 이후 경과 시간을 표시합니다. 따라서 반복
스냅샷은 `AGE`를 초기화하지 않습니다.

Analytics는 ATM 인근 KXBTCD 계약 5~7개의 미드포인트로 단일 내재변동성도
추정합니다. 선택된 계약의 가중치는 `(미결제약정 + 1)`을 합계로 정규화하며,
변동성 콘에서는 이 값을 모델 예측과 함께 노란색 점선으로 표시합니다.

## 로컬 개발

저장소 루트에서 공용 편집 가능 개발 환경을 설치합니다.

```bash
uv sync
source .venv/bin/activate
```

서비스 lockfile이 존재하는 경우 배포의 기준입니다. CI와 동일한 방식으로 개별
서비스를 검증하려면 다음 명령을 사용합니다.

```bash
uv sync --directory services/<service> --locked
uv run --directory services/<service> --locked pytest
```

현재 `live_feature_service`에는 lockfile이 없으므로 이 서비스에서는
`--locked` 없이 `uv sync`와 `uv run pytest`를 사용합니다.

대부분의 장기 실행 서비스에는 로컬 Redis 인스턴스가 필요합니다. 클라우드
작업에는 Application Default Credentials와 각 README에 명시된 프로젝트
리소스 접근 권한도 필요합니다.

## 배포

- `.github/workflows/ci.yml`은 배포 대상 모든 서비스를 테스트하고 빌드합니다.
- `.github/workflows/integration.yml`은 격리된
  ingestion/aggregator/live-feature 경로와 재시작 복구를 검증합니다.
- `.github/workflows/cd.yml`은 변경 불가능한 커밋 SHA 이미지를 생성하고 승인된
  1시간 모델을 `model-serving`에 패키징하며, GKE 리소스를 배포하고 롤아웃을
  검증한 뒤 오프라인/온라인 동등성 게이트와 실시간 가격 스모크 테스트를
  실행합니다.
- `.github/workflows/ml-pipeline.yml`은 Feast/ML 코드를 테스트하고 Vertex
  파이프라인 태스크 이미지 4개와 컴파일된 파이프라인 템플릿을 게시합니다.

프로덕션 manifest는 `k8s/`에 있습니다. `latest`를 배포하지 마십시오. CD는 Git
커밋 SHA를 이미지 태그로 사용합니다.

## 현재 프로덕션 계약

- 피처 계약: `market_features/v2_10s`
- 예측 주기: `5m`, `15m`, `30m`, `1h`
- 오프라인 전용 `target_rv_1m` 열은 호환성을 위해 유지하지만 1분 예측은
  서빙하지 않습니다.
- 실시간 피처 키: `market:features:v2_10s:BTCUSD:latest`
- 변동성 키: `market:volatility:v2_10s:BTCUSD:latest`
- Kalshi IV 키: `market:implied_volatility:v1:BTCUSD:latest`
- 가격 키: `market:pricing:v1:<KXBTCD market ticker>`
- 대시보드: 노란색 모델 곡선, `EDGE MID = 미드포인트 - 모델`, 체결 전에는
  오더북 활동을 포함하고 체결 후에는 서로 다른 체결을 따르는 활동 나이
- 리전: `asia-northeast3`

상위 데이터나 모델 응답이 누락되거나 오래됐거나 호환되지 않거나 유한하지
않으면 변동성 및 가격 출력은 fail-closed 방식으로 중단됩니다.
