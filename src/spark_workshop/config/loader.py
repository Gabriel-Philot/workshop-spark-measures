"""Load and merge named experiment configuration."""

import os
import re
from copy import deepcopy
from pathlib import Path
from typing import Any

import yaml

from spark_workshop.config.models import ExperimentConfig


DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "experiments.yaml"
_ENV_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(:-([^}]*))?\}")


def load_experiment_config(
    experiment_name: str,
    config_path: str | Path | None = None,
) -> ExperimentConfig:
    path = Path(config_path) if config_path else DEFAULT_CONFIG_PATH
    raw = _load_yaml(path)

    if path.resolve() == DEFAULT_CONFIG_PATH.resolve():
        defaults = raw.get("defaults") or {}
        experiments = raw.get("experiments") or {}
    else:
        global_raw = _load_yaml(DEFAULT_CONFIG_PATH)
        defaults = _deep_merge(global_raw.get("defaults") or {}, raw.get("defaults") or {})
        experiments = raw.get("experiments") or {}

    if experiment_name not in experiments:
        available = sorted(experiments)
        raise KeyError(
            f"Unknown experiment '{experiment_name}'. Available experiments: {available}"
        )

    merged = _deep_merge(defaults, experiments[experiment_name])
    return ExperimentConfig.from_mapping(experiment_name, _expand_env(merged))


def _load_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as config_file:
        return yaml.safe_load(config_file) or {}


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = deepcopy(base)
    for key, value in (override or {}).items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = deepcopy(value)
    return merged


def _expand_env(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _expand_env(child) for key, child in value.items()}
    if isinstance(value, list):
        return [_expand_env(child) for child in value]
    if isinstance(value, str):
        return _ENV_PATTERN.sub(
            lambda match: os.environ.get(match.group(1), match.group(3) or ""), value
        )
    return value
