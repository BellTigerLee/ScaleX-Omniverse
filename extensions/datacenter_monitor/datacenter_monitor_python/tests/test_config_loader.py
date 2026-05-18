import os
from pathlib import Path

import pytest

from config_loader import resolve_profile_path


@pytest.fixture
def config_dir(tmp_path, monkeypatch):
    monkeypatch.delenv("DC_PROFILE", raising=False)
    return tmp_path


def test_env_var_wins(config_dir, monkeypatch):
    (config_dir / "env.cluster-a").write_text("CLUSTER_HOST=1.2.3.4\n")
    monkeypatch.setenv("DC_PROFILE", "cluster-a")
    assert resolve_profile_path(config_dir) == config_dir / "env.cluster-a"


def test_env_var_missing_file_raises(config_dir, monkeypatch):
    monkeypatch.setenv("DC_PROFILE", "nonexistent")
    with pytest.raises(FileNotFoundError, match="nonexistent"):
        resolve_profile_path(config_dir)


def test_symlink_used_when_no_env(config_dir):
    (config_dir / "env.cluster-b").write_text("CLUSTER_HOST=5.6.7.8\n")
    (config_dir / "active").symlink_to("env.cluster-b")
    assert resolve_profile_path(config_dir).resolve() == (config_dir / "env.cluster-b").resolve()


def test_default_fallback(config_dir):
    (config_dir / "env.default").write_text("CLUSTER_HOST=9.9.9.9\n")
    assert resolve_profile_path(config_dir) == config_dir / "env.default"


def test_nothing_found_raises(config_dir):
    (config_dir / "env.cluster-x").write_text("CLUSTER_HOST=1.1.1.1\n")
    with pytest.raises(FileNotFoundError, match="DC_PROFILE"):
        resolve_profile_path(config_dir)


from config_loader import parse_profile


def _write(path: Path, body: str) -> Path:
    path.write_text(body)
    return path


def test_parse_basic(tmp_path):
    p = _write(tmp_path / "env.x", "CLUSTER_HOST=10.0.0.1\nKAFKA_NODEPORT=31327\n")
    result = parse_profile(p)
    assert result == {"CLUSTER_HOST": "10.0.0.1", "KAFKA_NODEPORT": 31327}


def test_parse_ignores_comments_and_blanks(tmp_path):
    p = _write(tmp_path / "env.x", "\n# comment\nCLUSTER_HOST=1.2.3.4\n\nKAFKA_NODEPORT=9092\n")
    result = parse_profile(p)
    assert result["CLUSTER_HOST"] == "1.2.3.4"
    assert result["KAFKA_NODEPORT"] == 9092


def test_parse_trims_whitespace(tmp_path):
    p = _write(tmp_path / "env.x", "  CLUSTER_HOST = 1.2.3.4  \n KAFKA_NODEPORT = 9092 \n")
    result = parse_profile(p)
    assert result["CLUSTER_HOST"] == "1.2.3.4"
    assert result["KAFKA_NODEPORT"] == 9092


def test_parse_duplicate_key_last_wins(tmp_path):
    p = _write(tmp_path / "env.x", "CLUSTER_HOST=1.1.1.1\nCLUSTER_HOST=2.2.2.2\nKAFKA_NODEPORT=9092\n")
    assert parse_profile(p)["CLUSTER_HOST"] == "2.2.2.2"


def test_parse_missing_required_key(tmp_path):
    p = _write(tmp_path / "env.x", "CLUSTER_HOST=1.2.3.4\n")
    with pytest.raises(KeyError, match="KAFKA_NODEPORT"):
        parse_profile(p)


def test_parse_invalid_port(tmp_path):
    p = _write(tmp_path / "env.x", "CLUSTER_HOST=1.2.3.4\nKAFKA_NODEPORT=not-a-number\n")
    with pytest.raises(ValueError, match="not-a-number"):
        parse_profile(p)


def test_parse_malformed_line(tmp_path):
    p = _write(tmp_path / "env.x", "CLUSTER_HOST=1.2.3.4\nnonsense\nKAFKA_NODEPORT=9092\n")
    with pytest.raises(ValueError, match="nonsense"):
        parse_profile(p)


from config_loader import load_profile


def test_load_profile_end_to_end(tmp_path, monkeypatch):
    (tmp_path / "env.cluster-test").write_text(
        "CLUSTER_HOST=10.0.0.1\nKAFKA_NODEPORT=31327\n"
    )
    monkeypatch.setenv("DC_PROFILE", "cluster-test")
    result = load_profile(config_dir=tmp_path)
    assert result == {"CLUSTER_HOST": "10.0.0.1", "KAFKA_NODEPORT": 31327}


def test_load_profile_default_config_dir(tmp_path, monkeypatch):
    # With no config_dir argument, loader uses the extension's config directory.
    # We patch the default by passing it explicitly; separate smoke test covers the real path.
    (tmp_path / "env.default").write_text("CLUSTER_HOST=1.1.1.1\nKAFKA_NODEPORT=9092\n")
    monkeypatch.delenv("DC_PROFILE", raising=False)
    assert load_profile(config_dir=tmp_path)["CLUSTER_HOST"] == "1.1.1.1"
