# ScaleX Dynamic Props

원본 `ScaleX_Twin.usd` 를 **오염시키지 않고**, 런타임에 **세션 레이어**로만
클러스터별 색상 `FloorPanel_Bottom_Under_Front` under-panel 을 동적 생성하는
**독립 Kit 익스텐션**이다. (기존 `datacenter_monitor` 와 합치지 않음)

## 왜 별도 익스텐션인가

원본 USD 에 직접 author 하면 같은 USD 를 쓰는 다른 곳이 오염된다. 이 익스텐션은
모든 prim/material 을 스테이지의 **세션 레이어(의 익명 서브레이어)** 에만 author 하므로
디스크의 `.usd` 는 절대 바뀌지 않는다("Save" 해도 원본에 안 남음).

## 무엇을 만드나

각 대상 랙의 `FloorPanel_Bottom_Front` 아래에
`FloorPanel_Bottom_Under_Front` mesh 를 소스 mesh 복사로 생성하고,
`Darker_Chassis_Metal.mdl` 의 `base_color` 를 클러스터 색으로 바인딩한다.

- DataX `#9cc2e5`
- TwinX `#a8d08c`
- MobileX `#ffd766`
- AutoX `#bfbfbf`

상세 사양과 모든 수치는 [`SPEC.md`](SPEC.md) 참고.

## 실행 / 연동

`--ext-folder` 가 이미 `ScaleX-Omniverse/extensions` 를 가리키므로 폴더만 있으면 발견된다.
`~/workspace/start_datacenter.sh` 가 `--enable scalex_dynamic_props` 를 함께 넘긴다.

```bash
~/workspace/start_datacenter.sh --composer    # 윈도우 Composer 로 확인
```

### 환경변수

- `SCALEX_DYNAMIC_PROPS_ENABLED` 는 기본값이 켜짐(`1`)이다.
- `0`, `false`, `off`, `no` 로 설정하면 동적 prop 을 author 하지 않고 기존 세션 오버레이도 분리한다.
  원본 USD 를 동적 prop 없는 상태로 저장해야 할 때 사용한다.

## 에셋 / 튜닝

- MDL 은 복제하지 않고 `datacenter_monitor/assets/ScaleX_POD_Project` 를 경로로 참조한다.
- 모든 경로/색상/파일명은 `scalex_dynamic_props_python/global_variables.py` 한 곳에 있다.
  under-panel 대상 랙이나 색상 변경은 그 파일만 고치면 된다(`[수정]` 마커 참고).

## 라이프사이클 (멱등)

- 스테이지의 `ASSETS_LOADED` 이벤트에서 `rebuild()` 를 스테이지당 1회만 실행.
- MDL 로드가 다시 `ASSETS_LOADED` 를 발생시켜도 이미 빌드된 스테이지에서는 건너뜀.
- `OPENED` / `CLOSING` / `CLOSED` 이벤트에서만 빌드 가드를 리셋.
- 스테이지를 다시 열어도 중복 생성 없이 동일 결과.
- `on_shutdown` 에서 오버레이를 비우고 세션 레이어에서 분리.
