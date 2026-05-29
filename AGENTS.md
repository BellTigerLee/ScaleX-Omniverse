# Repository Guidelines

## Project Structure & Module Organization

This repository contains the ScaleX Omniverse extension under `extensions/datacenter_monitor/`.
Core extension code lives in `datacenter_monitor_python/`, with the Kit entry point in
`extension.py`, Kafka handling in `kafka_subscriber.py`, configuration loading in
`config_loader.py`, and USD scene helpers under `datacenter_monitor_python/scene/`.
Python tests for the extension are in `datacenter_monitor_python/tests/`.

Local demo infrastructure is in `extensions/datacenter_monitor/kafka-dummy/`, including
Docker Compose services, producers, the FastAPI `query-server/`, and its tests. Runtime
endpoint profiles are stored in `extensions/datacenter_monitor/config/`; use committed
`env.*` profiles or create a local `active` symlink.

## Build, Test, and Development Commands

Run extension tests from the extension root:

```bash
cd extensions/datacenter_monitor
python -m pytest
```

Run the query-server tests when changing demo API/cache/replay code:

```bash
cd extensions/datacenter_monitor/kafka-dummy/query-server
python -m pytest
```

Start the local Kafka/Iceberg/Trino/FastAPI demo stack:

```bash
cd extensions/datacenter_monitor/kafka-dummy
docker compose up -d --build
```

For Omniverse, install Kafka dependencies into the Kit Python environment, then load
`extensions/datacenter_monitor/` through the Extension Manager.

## Coding Style & Naming Conventions

Use Python 3 style with 4-space indentation, type hints where they clarify interfaces,
and short module-level docstrings for non-obvious modules. Keep filenames and functions
in `snake_case`; test files should be named `test_*.py`. Prefer small, focused helpers in
`datacenter_monitor_python/scene/` over expanding `extension.py` with scene logic. No
project-wide formatter or linter is configured, so match nearby code and keep imports
simple and explicit.

## Testing Guidelines

The main framework is `pytest`; `extensions/datacenter_monitor/pytest.ini` sets
`--import-mode=importlib` and targets `datacenter_monitor_python/tests`. Add tests beside
the behavior being changed, especially for config parsing, message parsing, topology
mapping, cache updates, and replay state. Use fixtures such as `tmp_path` and
`monkeypatch` for filesystem and environment profile cases.

## Commit & Pull Request Guidelines

The current history uses very short commit subjects such as `UPDATE`; prefer improving on
that with concise imperative messages, for example `Add node-state replay tests`.
Pull requests should describe the user-facing behavior, list test commands run, link any
related issue or design note, and include screenshots or recordings for Omniverse scene or
dashboard-visible changes.

## Security & Configuration Tips

Do not hard-code cluster endpoints, credentials, or local paths in Python modules. Add new
cluster profiles as `config/env.<name>` and activate them with `DC_PROFILE=<name>` or an
`active` symlink. Keep generated data, local secrets, and machine-specific Omniverse files
out of version control.
