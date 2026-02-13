
from __future__ import annotations

from pathlib import Path
from typing import Any, Literal, Sequence, get_args

from nirmir_pipeline.pipeline.utils.error import ValidationError

Level = Literal["0", "1", "2", "3"]
Channel = Literal["nir", "mir"]
ALLOWED_LEVELS = set(get_args(Level))
ALLOWED_CHANNELS = set(get_args(Channel))

def _require_mapping(d: dict[str, Any], key: str) -> dict[str, Any]:
    v = d.get(key)
    if not isinstance(v, dict):
        raise ValidationError(f"Missing or invalid section '{key}:' Must be a mapping/object")
    return v

def _require_str(d: dict[str, Any], key: str) -> str:
    v = d.get(key)
    if not isinstance(v, str) or v.strip() == "":
        raise ValidationError(f"Missing or invalid string: {key}")
    return v.strip()

def _require_bool(d: dict[str, Any], key: str) -> bool:
    v = d.get(key)
    if not isinstance(v, bool):
        raise ValidationError(f"Missing or invalid boolean: {key}")
    return v

def _require_list_of_str(d: dict[str, Any], key:str) -> list[str]:
    v = d.get(key)
    if not isinstance(v, list) or len(v) == 0:
        raise ValidationError(f"Missing or invalid list: {key}, must be non-empty list")
    out: list[str] = []
    for i, item in enumerate(v):
        if not isinstance(item, str) or item.strip() == "":
            raise ValidationError(f"Invalid item in {key}[{i}], must be non-empty string")
        out.append(item.strip())
    return out

def _resolve_path(base_dir: Path, raw_path: str, *, key: str) -> Path:
    p = Path(raw_path).expanduser()
    if not p.is_absolute():
        p = base_dir / p
    return p.resolve()

def _resolve_optional_path(base_dir: Path, raw_value: Any, *, key: str) -> Path | None:
    if raw_value is None:
        return None
    if isinstance(raw_value, str):
        if raw_value.strip() == "":
            return None
        return _resolve_path(base_dir=base_dir, raw_path=raw_value, key=key)
    raise ValidationError(f"Invalid value for {key} (must be a string path or empty)")

def _validate_levels(levels: Sequence[str]) -> None:
    bad = [x for x in levels if x not in ALLOWED_LEVELS]
    if bad:
        raise ValidationError(
            f"Invalid pipeline.levels: {bad}. "
            f"Allowed: {sorted(ALLOWED_LEVELS)}"
        )

def _validate_channels(channels: Sequence[str]) -> None:
    bad = [x for x in channels if x not in ALLOWED_CHANNELS]
    if bad:
        raise ValidationError(
            f"Invalid pipeline.channels: {bad}. "
            f"Allowed: {sorted(ALLOWED_CHANNELS)}"
        )