"""Endpoint profile loader for the datacenter_monitor Omniverse extension.

Reads a flat KEY=VALUE profile file whose location is selected by:
  1) env var DC_PROFILE (looks for <config_dir>/env.<DC_PROFILE>)
  2) symlink <config_dir>/active
  3) <config_dir>/env.default
"""
from __future__ import annotations

import os
from pathlib import Path


def resolve_profile_path(config_dir: Path) -> Path:
    """Return the profile file to load, or raise FileNotFoundError with guidance."""
    profile_name = os.environ.get("DC_PROFILE")
    if profile_name:
        candidate = config_dir / f"env.{profile_name}"
        if not candidate.exists():
            raise FileNotFoundError(
                f"DC_PROFILE={profile_name!r} but {candidate} does not exist."
            )
        return candidate

    active = config_dir / "active"
    if active.is_symlink() or active.exists():
        return active

    default = config_dir / "env.default"
    if default.exists():
        return default

    available = sorted(p.name for p in config_dir.glob("env.*") if p.name != "env.example")
    raise FileNotFoundError(
        "No endpoint profile found. Fix by either:\n"
        "  1) export DC_PROFILE=<name>  (looks for config/env.<name>)\n"
        "  2) cd config && ln -sfn env.<name> active\n"
        f"Available profiles in {config_dir}: {available or '(none)'}"
    )


REQUIRED_KEYS = ("CLUSTER_HOST", "KAFKA_NODEPORT")


def parse_profile(path: Path) -> dict:
    """Parse a KEY=VALUE profile file and return a validated dict.

    - Blank lines and '#' comments are ignored.
    - Whitespace around '=' and at line edges is trimmed.
    - Duplicate keys: last value wins.
    - KAFKA_NODEPORT is converted to int.
    - Missing required keys raise KeyError; malformed lines raise ValueError.
    """
    parsed: dict[str, str] = {}
    for lineno, raw in enumerate(path.read_text().splitlines(), start=1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            raise ValueError(f"{path}:{lineno}: malformed line: {raw!r}")
        key, _, value = line.partition("=")
        parsed[key.strip()] = value.strip()

    for key in REQUIRED_KEYS:
        if key not in parsed:
            raise KeyError(f"Profile {path} missing required key: {key}")

    try:
        parsed["KAFKA_NODEPORT"] = int(parsed["KAFKA_NODEPORT"])
    except ValueError as exc:
        raise ValueError(
            f"Profile {path} KAFKA_NODEPORT must be an integer, got {parsed['KAFKA_NODEPORT']!r}"
        ) from exc

    return parsed


# The config/ directory sits next to datacenter_monitor_python/ at the extension root.
_DEFAULT_CONFIG_DIR = Path(__file__).resolve().parent.parent / "config"


def load_profile(config_dir: Path | None = None) -> dict:
    """Resolve the active profile file and return its parsed contents."""
    directory = config_dir if config_dir is not None else _DEFAULT_CONFIG_DIR
    return parse_profile(resolve_profile_path(directory))
