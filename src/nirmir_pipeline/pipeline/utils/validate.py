
import re
from pathlib import Path
from typing import Any, Literal, Sequence, get_args

from nirmir_pipeline.pipeline.utils.errors import ValidationError
from nirmir_pipeline.pipeline.utils.classes import Level, Channel, InputLayout


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

def _resolve_path(raw_path: str) -> Path:
    p = Path(raw_path).expanduser()
    if not p.is_absolute():
        project_root = Path.cwd()
        p = project_root / p
    return p.resolve()

def _resolve_optional_path(raw_value: Any) -> Path | None:
    if raw_value is None:
        return None
    if isinstance(raw_value, str):
        if raw_value.strip() == "":
            return None
        return _resolve_path(raw_path=raw_value)
    raise ValidationError(f"Resolving path failed, (must be a string, Path or empty)")

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

def _validate_output_dir(output_dir: Path, missphas: str) -> Path:

    if output_dir:
        output_path = output_dir.expanduser()
        if not output_path.is_dir():
            raise ValidationError(f"Output path exists but is not a directory: {output_path}")
        base_output = output_path
    else:
        project_root = Path.cwd()
        base_output = project_root / "outputs"
    
    final_output = base_output / missphas

    final_output.mkdir(parents=True, exist_ok=True)
    if not final_output.exists() or not final_output.is_dir():
        raise ValidationError(f"Failed to create output directory: {final_output}")
    
    return final_output
    
def _validate_level_0_input_dir(input_dir: Path) -> InputLayout:

    _ACQ_RE = re.compile(r"^acq_\d+.*$") # folder name for images. Has to start with acq_

    input_dir = input_dir.expanduser()

    if not input_dir.exists():
        raise ValidationError(f"Input directory does not exist: {input_dir}")
    if not input_dir.is_dir():
        raise ValidationError(f"Input path is not a directory: {input_dir}")
    
    root = input_dir
    
    meta_dir = root / "meta"
    if not meta_dir.exists():
        raise ValidationError(f"Missing required directory: {meta_dir}")
    if not meta_dir.is_dir():
        raise ValidationError(f"'meta' exists but its not a directory: {meta_dir}")
    
    telemetry_json = meta_dir / "telemetry.json"
    config_json = meta_dir / "config.json"

    missing = [p.name for p in (telemetry_json, config_json) if not p.exists()]
    if missing:
        raise ValidationError(f"Missing required file(s) in {meta_dir}: {', '.join(missing)}")

    for p in (telemetry_json, config_json):
        if not p.is_file():
            raise ValidationError(f"Required path exists but is not a file: {p}")
    
    candidates: list[Path] = [
        p for p in root.iterdir()
        if p.is_dir() and _ACQ_RE.match(p.name)
    ]
    if not candidates:
        raise ValidationError(f"No acquisition directory found. Expected folder like 'acq_000/' under: {root}")
    
    candidates.sort(key=lambda p: p.name)
    acq_dir = candidates[0]

    return InputLayout(
        root=root,
        meta_dir=meta_dir,
        telemetry_json=telemetry_json,
        config_json=config_json,
        acquisition_dir=acq_dir,
    )

    