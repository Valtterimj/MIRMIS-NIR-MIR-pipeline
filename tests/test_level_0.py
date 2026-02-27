import pytest
import yaml
from pathlib import Path

import nirmir_pipeline.pipeline.config as cfg
from nirmir_pipeline.pipeline.utils.errors import ValidationError


def _minimal_raw(tmp_path: Path) -> dict:
    input_dir = tmp_path / "input"
    spice_dir = tmp_path / "spice"
    output_dir = tmp_path / "output"
    input_dir.mkdir()
    spice_dir.mkdir()
    return {
        "run": {
            "input_dir" : str(input_dir),
            "output_dir": str(output_dir),
            "spice_dir" : str(spice_dir),
            "overwrite" : False
        },
        "data" : {
            "instrume" : "MIRMIS",
            "origin" : "TEST",
            "swcreate": "pytest",
            "missphas": "TEST",
            "observ": "TEST",
            "object": "TEST",
            "target": "TEST",
        },
        "pipeline": {
            "levels": ["0"],
            "channels": ["NIR"]
        }
    }

def test_load_config_valid(tmp_path: Path) -> None:

    raw = _minimal_raw(tmp_path)

    config_file = tmp_path / "pipeline.yaml"
    config_file.write_text(yaml.safe_dump(raw), encoding="utf-8")

    c = cfg.load_config(config_file)
    fields = c.data.__dataclass_fields__.keys()

    assert c.config_path == config_file.resolve()
    assert c.run.input_dir == c.run.input_dir.resolve()
    assert set(fields) == {
        "instrume",
        "origin",
        "swcreate",
        "missphas",
        "observ",
        "object",
        "target",
    }
    assert c.pipeline.levels == ("0",)
    assert c.pipeline.channels == ("NIR", )

def test_load_config_invalid_data(tmp_path: Path) -> None:
    raw = _minimal_raw(tmp_path)
    del raw["data"]["origin"]

    config_file = tmp_path / "pipeline.yaml"
    config_file.write_text(yaml.safe_dump(raw), encoding="utf-8")

    with pytest.raises(ValidationError, match=r"\borigin\b"):
        cfg.load_config(config_file)