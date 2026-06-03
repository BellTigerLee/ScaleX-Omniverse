# CHANGELOG

이 익스텐션의 모든 주목할 변경을 기록한다.

## [1.0.2]

### Removed
- 카드(plane) 기능 제거, `Under_Front` under-panel만 유지.

## [1.0.1]

### Fixed
- `ASSETS_LOADED` 반복 이벤트에서 매번 오버레이를 `Clear()` 후 재author 하던 동작을 차단.
  스테이지당 1회만 빌드하고 `OPENED` / `CLOSING` / `CLOSED` 에서만 가드를 리셋한다.

### Added
- `SCALEX_DYNAMIC_PROPS_ENABLED` 환경변수 추가. 기본값은 켜짐이며,
  `0` / `false` / `off` / `no` 설정 시 동적 prop author 를 중단하고 기존 세션 오버레이를 정리한다.

## [1.0.0]

### Added
- 세션 레이어 오버레이 기반 동적 prop 생성 익스텐션 최초 구현 (원본 USD 오염 없음).
- 클러스터별 색상 FloorPanel under-panel — DataX/TwinX/MobileX/AutoX 랙 바닥에
  소스 mesh 복사로 `FloorPanel_Bottom_Under_Front` 생성 + `Darker_Chassis_Metal` base_color 바인딩.
- `ASSETS_LOADED` 트리거 + 멱등 rebuild + `on_shutdown` 정리.
