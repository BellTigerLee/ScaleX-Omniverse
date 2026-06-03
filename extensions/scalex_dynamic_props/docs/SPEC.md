# ScaleX Dynamic Props — 구현 스펙 (SPEC)

> 원본 `ScaleX_Twin.usd` 를 **오염시키지 않고**, 런타임에 **세션 레이어**로만
> 클러스터별 색상 `FloorPanel_Bottom_Under_Front` under-panel 을 동적 생성하는
> **별도 Kit 익스텐션**의 사양. (기존 `datacenter_monitor` 에 합치지 않음)
>
> 모든 수치는 저장본 `ScaleX_Twin.usd` 를 `usd-core` 로 직접 읽어 추출한 실제 값이다.

---

## 0. 목적 / 제약

- **오염 금지**: 디스크의 `.usd` 파일은 절대 수정되지 않아야 한다 → 모든 prim/material 은
  스테이지의 **세션 레이어**(의 익명 서브레이어)에만 author 한다. "Save" 해도 원본에 안 남는다.
- **별도 익스텐션**: 새 폴더 `extensions/scalex_dynamic_props/` 에 독립 구현. 기존 코드 수정 없음.
- **에셋 재사용**: MDL 은 복제하지 않고 `datacenter_monitor` 의 assets 를 경로로 참조한다.
- **멱등(idempotent)**: 스테이지가 열릴 때마다 깨끗이 지우고 다시 만든다.
- **USD Viewer 기반**: Isaac Sim / `omni.physx` 의존성 없이 USD authoring 만 사용한다.

---

## 1. 폴더 / 파일 구조

```
extensions/scalex_dynamic_props/
├── config/
│   └── extension.toml                       # 익스텐션 메타데이터
├── scalex_dynamic_props_python/
│   ├── __init__.py                          # from .extension import Extension
│   ├── extension.py                         # omni.ext.IExt — 진입점/스테이지 이벤트
│   ├── global_variables.py                  # 모든 상수(아래 §4 데이터 테이블)
│   └── scene_builder.py                     # 세션 레이어 author 로직(아래 §5)
└── docs/
    ├── SPEC.md                              # 이 문서
    ├── README.md
    └── CHANGELOG.md
```

명명 규칙은 `datacenter_monitor` 와 동일: 폴더명 = ext id, 패키지명 = `<폴더>_python`,
`config/extension.toml`, 한국어 주석 + `[수정]` 마커, `print("[ScaleX Dynamic Props] ...")` 로깅.

---

## 2. extension.toml

```toml
[package]
version = "1.0.0"
category = "Visualization"
title = "ScaleX Dynamic Props"
description = "런타임 세션 레이어로 클러스터별 색상 under-panel 동적 생성"
writeTarget.kit = true

[dependencies]
"omni.usd" = {}
"omni.kit.viewport.utility" = {}

[[python.module]]
name = "scalex_dynamic_props_python"
```

---

## 3. 런처 연동

`~/workspace/start_datacenter.sh` 의 `--enable datacenter_monitor` 다음 줄에 추가:

```bash
         --enable datacenter_monitor \
         --enable scalex_dynamic_props \
```

`--ext-folder` 가 이미 `ScaleX-Omniverse/extensions` 를 가리키므로 폴더만 있으면 발견된다.

---

## 4. 클러스터별 색상 FloorPanel under-panel

각 대상 랙의 `FloorPanel_Bottom_Front`(Xform) **자식**으로
`FloorPanel_Bottom_Under_Front`(Mesh) 를 생성하고, 클러스터 색 MDL 을 바인딩한다.

### 4.1 대상 클러스터 / 랙 / 색상

| 클러스터 prim | 랙 | 색 hex | base_color (sRGB, 0~1) |
|---|---|---|---|
| `DataX_Cluster`  | `Rack_42U_A3` | `#9cc2e5` | `(0.611765, 0.760784, 0.898039)` |
| `TwinX_Cluster`  | `Rack_42U_A4` | `#a8d08c` | `(0.658824, 0.815686, 0.549020)` |
| `MobileX_Cluster`| `Rack_42U_A1` | `#ffd766` | `(1.000000, 0.843137, 0.400000)` |
| `AutoX_Cluster`  | `Rack_42U_A5`, `Rack_42U_A6` | `#bfbfbf` | `(0.749020, 0.749020, 0.749020)` |

- 베이스 prim: `/World/SCENT_Multi_POD_Module/ScaleX_POD/<Cluster>/<Rack>`
- 색공간: **sRGB 값 그대로**(0~255 → /255). linear 변환하지 않음.
- (참고) 색을 안 준 `EdgeX`/`Anonymous` 클러스터는 대상 아님. 실제 씬에 EdgeX_Cluster 자체가 없음.

### 4.2 소스 메쉬 / under-panel 생성

- 내부 경로(모든 대상 랙에서 동일함을 확인):
  `<Rack>/Rack_42U/Body/Core/Rack_Core/FloorPanel_Bottom_Front` (Xform)
  - 자식 mesh: `…/FloorPanel_Bottom_Front` (768 pts) ← **소스**
- under-panel 경로: `…/FloorPanel_Bottom_Front/FloorPanel_Bottom_Under_Front`
- 생성 방식: 소스 mesh 의 `points / faceVertexCounts / faceVertexIndices / normals(+interpolation) /
  extent / subdivisionScheme / primvars:st` 를 **런타임에 그대로 복사**(768 pt 하드코딩 금지).
- Transform: `xformOpOrder = [xformOp:transform]`,
  matrix = identity + translate **`(0, 0, -5)`** (저장본과 동일; 월드 기준 Y −5 로 내려감).
- `DataX_Cluster/Rack_42U_A3` 에는 사용자가 만든 under-panel 이 이미 있음 → 세션 레이어 override
  로 동일 경로에 author 하면 됨(원본은 추후 사용자가 삭제 예정).

### 4.3 under-panel 머티리얼 — MDL (저장본과 동일 형식)

클러스터당 머티리얼 1개를 만들어 그 클러스터의 모든 under-panel 에 공유 바인딩.

- 위치(권장): `/World/ScaleX_Dynamic/Looks/<Cluster>_Color_Chassis_Metal`
  (예: `DataX_Color_Chassis_Metal`, `TwinX_…`, `MobileX_…`, `AutoX_…`)
- 셰이더(`<mat>/Shader`):
  - `info:implementationSource = sourceAsset`
  - `info:mdl:sourceAsset = @<…>/materials/Darker_Chassis_Metal.mdl@` (디스크 존재 확인됨)
  - `info:mdl:sourceAsset:subIdentifier = Darker_Chassis_Metal`
  - `inputs:base_color` (`color3f`) = 위 표의 sRGB 값
  - 출력 `outputs:out` (`token`)
- 머티리얼 출력(저장본과 동일 배선):
  `outputs:mdl:surface`, `outputs:mdl:displacement`, `outputs:mdl:volume` → 각각 `Shader.outputs:out`
- 바인딩: `UsdShade.MaterialBindingAPI.Apply(under).Bind(mat)`

---

## 5. 빌드 로직 / 라이프사이클 (`scene_builder.py` + `extension.py`)

### 5.1 세션 레이어 격리 (오염 방지의 핵심)

```python
session = stage.GetSessionLayer()
overlay = Sdf.Layer.CreateAnonymous("scalex_dynamic_props")   # 익명 레이어
if overlay.identifier not in session.subLayerPaths:
    session.subLayerPaths.insert(0, overlay.identifier)
# 모든 author 는 이 edit target 안에서만 수행
with Usd.EditContext(stage, Usd.EditTarget(overlay)):
    _build_floor_under_panels(stage)
```

세션 레이어(및 그 서브레이어)는 root `.usd` 저장 대상이 아니므로 원본 오염 없음.

### 5.2 트리거 / 멱등

- `extension.py.on_startup` 에서 `omni.usd.get_context()` 의 stage event 구독.
- `StageEventType.ASSETS_LOADED` (references/payload 로드 완료 → 소스 mesh 읽기 안전) 에서
  `rebuild()` 호출. 스테이지가 이미 떠 있으면 startup 시 1회 시도.
- `rebuild()` 는 먼저 이전 overlay 내용을 비우고(`overlay.Clear()` 또는 생성 경로 제거) 재생성 → 멱등.

### 5.3 정리

- `on_shutdown`: 생성 prim 제거 + `session.subLayerPaths` 에서 overlay 분리 + 구독 해제.

### 5.4 공용 헬퍼(설계)

```python
def hex_to_srgb(hex_str) -> Gf.Vec3f          # "#9cc2e5" → (0.6118,0.7608,0.8980)
def make_mdl_material(stage, path, mdl_asset, sub_id, inputs: dict) -> UsdShade.Material
def copy_mesh_geom(src_mesh, dst_mesh)         # points/topology/normals/extent/st/subdiv 복사
def add_under_panel(stage, front_xform_path, material)  # §4.2
```

### 5.5 경로 resolve

```python
EXT_ROOT = Path(__file__).resolve().parent.parent                 # …/scalex_dynamic_props
DC_ASSETS = EXT_ROOT.parent / "datacenter_monitor" / "assets" / "ScaleX_POD_Project"
MDL_DARKER = DC_ASSETS / "materials" / "Darker_Chassis_Metal.mdl"
```

---

## 6. 검증 시나리오

1. `start_datacenter.sh --composer` 실행 → 스테이지 로드 후 DataX/TwinX/MobileX/AutoX 랙 바닥에
   색상 under-panel(파랑/초록/노랑/회색)이 보임.
2. Composer 에서 **Save** 후 `ScaleX_Twin.usd` 가 git/디스크상 변경 없음(오염 0) 확인.
3. 스테이지 재오픈 시 중복 생성 없이 동일 결과(멱등) 확인.

---

## 7. 미해결/주의

- under-panel 의 `(0,0,-5)` 는 **부모 Xform 로컬 기준**이므로, 동일 부모 아래에 author 하면 랙마다 자동 정렬됨.
- 추후 사용자가 원본 under-panel 을 USD 에서 삭제해도 이 익스텐션이 세션 레이어에서 동일 경로를 다시 author 한다.
