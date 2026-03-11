from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

import yaml 

from nirmir_pipeline.pipeline.utils.validate import (Level, Channel, _require_mapping, _require_bool, _require_list_of_str, _require_str, 
                                                     _resolve_path, _resolve_optional_path, _validate_levels, _validate_channels)

from nirmir_pipeline.pipeline.utils.errors import ConfigError
from nirmir_pipeline.pipeline.utils.classes import Config, RunConfig, DataConfig, PipelineConfig


DEFAULT_CANDIDATES = [
    Path("configs/pipeline.yaml"),
    Path("configs/pipeline.example.yaml"),
]


# Public API
def load_config(config_path: str | Path | None) -> Config:
    """
    Load, parse and validate the pipeline configurations
    Called by pipeline/run.py
    
    :param config_path: path to the configuration file
    :type config_path: str | Path | None
    :return: Config object containing pipeline configurations
    :rtype: Config
    """

    path = _resolve_config_path(config_path=config_path)
    raw = _read_yaml(path)
    config_object = _parse_config_dict(raw=raw, config_path=path)
    return config_object

# Internals
def _resolve_config_path(config_path: str | Path | None) -> Path:
    """
    Resolve the config file path
    - If config_path is provided: must exist and be a file.
    - If None: try DEFAULT_CANIDATES, return that first exists.
    
    :param config_path: path to the configuration file
    :type config_path: str | Path | None
    :return: Path object to the configuration
    :rtype: Path
    """
    if config_path is not None:
        p = Path(config_path).expanduser()
        if not p.exists():
            raise ConfigError(f"Config not found {p}")
        if p.is_dir():
            raise ConfigError(f"Config path is a directory, expected a file: {p}")
        return p.resolve()
    
    for p in DEFAULT_CANDIDATES:
        if p.exists() and p.is_file():
            return p.resolve()
    
    tried = ", ".join(str(p) for p in DEFAULT_CANDIDATES)
    raise ConfigError(
        "No config provided and no default config found. "
        f"Tried: {tried}."
        "Run from project root or pass --config"
    )


def _read_yaml(path: Path) -> dict[str, Any]:
    """
    Read YAML file and return a dict. 
    
    :param path: Path object to the config YAML file
    :type path: Path
    :return: dictionary of the top-level config fields
    :rtype: dict[str, Any]
    """

    try: 
        text = path.read_text(encoding="utf-8")
    except OSError as e:
        raise ConfigError(f"Failed to read config file: {path}") from e
    
    try:
        obj = yaml.safe_load(text) or {}
    except Exception as e:
        raise ConfigError(f"Failed to parse YAML in config file {path}") from e
    
    if not isinstance(obj, dict):
        raise ConfigError(f"top-level YAML must be a mapping/object in {path}")
    
    return obj

def _parse_config_dict(raw: dict[str, Any], *, config_path: Path) -> Config:
    """
    Convert the raw YAML dict into dataclasses.
    
    :param raw: top-level config dictionary
    :type raw: dict[str, Any]
    :param config_path: Path to the config YAML file
    :type config_path: Path
    :return: Config object
    :rtype: Config
    """

    base_dir = config_path.parent
    run_raw = _require_mapping(raw, "run")
    data_raw = _require_mapping(raw, "data")
    pipeline_raw = _require_mapping(raw, "pipeline")

    run = RunConfig(
        input_dir=_resolve_path(_require_str(run_raw, "input_dir"), base_dir),
        output_dir=_resolve_optional_path(run_raw.get("output_dir", ""), base_dir),
        spice_dir=_resolve_optional_path(run_raw.get("spice_dir", ""), base_dir),
        calibration_dir=_resolve_path(_require_str(run_raw, "calibration_dir"), base_dir),
        overwrite=_require_bool(run_raw, "overwrite"),
    )

    data = DataConfig(
        instrume=_require_str(data_raw, "instrume"),
        origin=_require_str(data_raw, "origin"),
        swcreate=_require_str(data_raw, "swcreate"),
        missphas=_require_str(data_raw, "missphas"),
        observ=_require_str(data_raw, "observ"),
        object=_require_str(data_raw, "object"),
        target=_require_str(data_raw, "target"),
    )

    levels = _require_list_of_str(pipeline_raw, "levels")
    channels = _require_list_of_str(pipeline_raw, "channels")

    _validate_levels(levels)
    _validate_channels(channels)

    pipeline = PipelineConfig(
        levels=tuple(levels),
        channels=tuple(channels),
    )

    return Config(run=run, data=data, pipeline=pipeline, config_path=config_path)


