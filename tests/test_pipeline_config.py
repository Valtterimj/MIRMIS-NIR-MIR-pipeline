import pytest
import yaml
import copy

from pathlib import Path
from astropy.io import fits
from collections.abc import Sequence

from nirmir_pipeline.pipeline.utils.errors import ValidationError, ConfigError, PipelineError
from nirmir_pipeline.pipeline.utils.classes import Config, RunConfig, PipelineConfig, CalibConfig, DataConfig
from nirmir_pipeline.pipeline.utils.validate import _resolve_path, _validate_output_dir


from nirmir_pipeline.pipeline.run import run_pipeline
from nirmir_pipeline.pipeline.config import load_config

from nirmir_pipeline.pipeline.levels.level_0.run import run_level_0

@pytest.fixture
def repo_root(pytestconfig) -> Path:
    return pytestconfig.rootpath

def read_yaml(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))

def write_yaml(path: Path, data: dict) -> None:
    path.write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8")

# Config file tests
def test_missing_config_file(repo_root: Path) -> None:
    invalid_config = repo_root / 'tests' / 'configs' / 'invalid.yaml'
    config_repo = repo_root / 'tests' / 'configs'
    with pytest.raises(ConfigError, match=r"\bConfig not found\b"):
        run_pipeline(invalid_config)
    with pytest.raises(ConfigError, match=r"^Config path is a directory, expected a file:"):
        run_pipeline(config_repo)

# Config file tests
def test_config_file_contains_everything(tmp_path: Path, repo_root: Path) -> None:
    config = repo_root / 'tests' / 'configs' / 'test.yaml'
    raw = read_yaml(config)
    modified = copy.deepcopy(raw)
    modified_config = tmp_path / "modified.yaml"
    modified["run"]["input_dir"] = str(repo_root / 'tests' / 'data' / 'binary') # Correct path for input dir
    modified["calibration"]["calibration_dir"] = str(repo_root / 'calibration') # Correct path for calibration
    write_yaml(modified_config, modified)

    cfg = load_config(modified_config)

    assert cfg.run is not None
    assert cfg.calib is not None
    assert cfg.data is not None
    assert cfg.pipeline is not None
    assert cfg.config_path is not None

    assert isinstance(cfg.run, RunConfig)
    assert isinstance(cfg.calib, CalibConfig)
    assert isinstance(cfg.data, DataConfig)
    assert isinstance(cfg.pipeline, PipelineConfig)
    assert isinstance(cfg.config_path, Path)

    run_cfg = cfg.run
    assert isinstance(run_cfg.input_dir, Path)
    assert isinstance(run_cfg.output_dir, Path | None)
    assert isinstance(run_cfg.spice_dir, Path | None)
    assert isinstance(run_cfg.overwrite, bool)

    calibration_cfg = cfg.calib
    assert isinstance(calibration_cfg.calibration_dir, Path)
    assert isinstance(calibration_cfg.dark, str | None)
    assert isinstance(calibration_cfg.flat, str | None)
    assert isinstance(calibration_cfg.badpixels, str | None)
    assert isinstance(calibration_cfg.mir_radiance, str | None)
    assert isinstance(calibration_cfg.nir_radiance, str | None)
    assert isinstance(calibration_cfg.solar_ssi, str | None)

    data_cfg = cfg.data
    assert isinstance(data_cfg.instrume, str)
    assert isinstance(data_cfg.origin, str)
    assert isinstance(data_cfg.swcreate, str)
    assert isinstance(data_cfg.missphas, str)
    assert isinstance(data_cfg.observ, str)
    assert isinstance(data_cfg.object, str)
    assert isinstance(data_cfg.target, str)
    assert isinstance(data_cfg.solar_d, str | None)

    pipeline_cfg = cfg.pipeline
    assert isinstance(pipeline_cfg.channels, Sequence)
    assert isinstance(pipeline_cfg.levels, Sequence)

def test_missing_config_subclass(tmp_path: Path, repo_root) -> None:
    config = repo_root / 'tests' / 'configs' / 'test.yaml'

    raw = read_yaml(config)
    modified = copy.deepcopy(raw)
    modified_config = tmp_path / "broken.yaml"
    modified["run"]["input_dir"] = str(repo_root / 'tests' / 'data' / 'binary') # Correct path for input dir
    modified["calibration"]["calibration_dir"] = str(repo_root / 'calibration') # Correct path for calibration

    del modified["run"]
    write_yaml(modified_config, modified)
    with pytest.raises(ValidationError, match=r"^Missing or invalid section 'run:'"):
        cfg = load_config(modified_config)
    
    modified = copy.deepcopy(raw)
    del modified["calibration"]
    write_yaml(modified_config, modified)
    with pytest.raises(ValidationError, match=r"^Missing or invalid section 'calibration:'"):
        cfg = load_config(modified_config)
    
    modified = copy.deepcopy(raw)
    del modified["data"]
    write_yaml(modified_config, modified)
    with pytest.raises(ValidationError, match=r"^Missing or invalid section 'data:'"):
        cfg = load_config(modified_config)
    
    modified = copy.deepcopy(raw)
    del modified["pipeline"]
    write_yaml(modified_config, modified)
    with pytest.raises(ValidationError, match=r"^Missing or invalid section 'pipeline:'"):
        cfg = load_config(modified_config)
    


def test_invalid_run_config(tmp_path: Path, repo_root: Path) -> None:
    config = repo_root / 'tests' / 'configs' / 'test.yaml'

    raw = read_yaml(config)
    modified = copy.deepcopy(raw)
    modified_config = tmp_path / "broken.yaml"
    modified["run"]["input_dir"] = str(repo_root / 'tests' / 'data' / 'binary') # Correct path for input dir
    modified["calibration"]["calibration_dir"] = str(repo_root / 'calibration') # Correct path for calibration

    # Missing input dir
    modified["run"]["input_dir"] = ""
    write_yaml(modified_config, modified)
    with pytest.raises(ValidationError, match=r"^Missing or invalid string:"):
        cfg = load_config(modified_config)
    
    # Invalid input dir
    modified["run"]["input_dir"] = "not_existing_path"
    write_yaml(modified_config, modified)
    with pytest.raises(ValidationError, match=r"^Path does not exists:"):
        cfg = load_config(modified_config)
    
    modified["run"]["input_dir"] = str(repo_root / 'tests' / 'data' / 'binary') # Correct path for input dir
    modified["run"]["output_dir"] = "not_exsisting_path"

    # Invalid output dir
    write_yaml(modified_config, modified)
    cfg = load_config(modified_config)
    with pytest.raises(ValidationError, match=r"^Output path does not exists:"):
        output = _validate_output_dir(cfg.run.output_dir, cfg.data.missphas)

    modified["run"]["output_dir"] = ""
    # Invalid overwrite
    modified["run"]["overwrite"] = "not_bool"
    write_yaml(modified_config, modified)
    with pytest.raises(ValidationError, match=r"^Missing or invalid boolean:"):
        cfg = load_config(modified_config)


def test_invalid_calibration_config(tmp_path: Path, repo_root: Path) -> None:
    config = repo_root / 'tests' / 'configs' / 'test.yaml'
    raw = read_yaml(config)
    modified = copy.deepcopy(raw)
    modified_config = tmp_path / "broken.yaml"
    modified["run"]["input_dir"] = str(repo_root / 'tests' / 'data' / 'binary') # Correct path for input dir

    modified["calibration"]["calibration_dir"] = str(repo_root / 'invalid_calibration')
    write_yaml(modified_config, modified)
    with pytest.raises(ValidationError, match=r"^Path does not exists:"):
        cfg = load_config(modified_config)

def test_invalid_data_config(tmp_path: Path, repo_root: Path) -> None:
    config = repo_root / 'tests' / 'configs' / 'test.yaml'
    raw = read_yaml(config)
    modified = copy.deepcopy(raw)
    modified_config = tmp_path / "broken.yaml"
    modified["run"]["input_dir"] = str(repo_root / 'tests' / 'data' / 'binary') # Correct path for input dir
    modified["calibration"]["calibration_dir"] = str(repo_root / 'calibration') # Correct path for calibration

    modified["data"]["origin"] = None
    write_yaml(modified_config, modified)
    with pytest.raises(ValidationError, match=r"^Missing or invalid string: origin"):
        cfg = load_config(modified_config)
    
    modified["data"]["origin"] = ""
    write_yaml(modified_config, modified)
    with pytest.raises(ValidationError, match=r"^Missing or invalid string: origin"):
        cfg = load_config(modified_config)
    
    del modified["data"]["origin"]
    write_yaml(modified_config, modified)
    with pytest.raises(ValidationError, match=r"^Missing or invalid string: origin"):
        cfg = load_config(modified_config)

def test_invalid_pipeline_config(tmp_path: Path, repo_root: Path) -> None:
    config = repo_root / 'tests' / 'configs' / 'test.yaml'
    raw = read_yaml(config)
    modified = copy.deepcopy(raw)
    modified_config = tmp_path / "broken.yaml"
    modified["run"]["input_dir"] = str(repo_root / 'tests' / 'data' / 'binary') # Correct path for input dir
    modified["calibration"]["calibration_dir"] = str(repo_root / 'calibration') # Correct path for calibration

    del modified["pipeline"]["levels"]
    write_yaml(modified_config, modified)
    with pytest.raises(ValidationError, match="Missing or invalid list: levels, must be non-empty list"):
        cfg = load_config(modified_config)
    
    modified["pipeline"]["levels"] = ""
    write_yaml(modified_config, modified)
    with pytest.raises(ValidationError, match="Missing or invalid list: levels, must be non-empty list"):
        cfg = load_config(modified_config)
    
    modified["pipeline"]["levels"] = [0, None, "2"]
    write_yaml(modified_config, modified)
    with pytest.raises(ValidationError, match="^Invalid pipeline.levels:"):
        cfg = load_config(modified_config)
    
    modified["pipeline"]["levels"] = [0, "1"]
    modified["pipeline"]["channels"] = ['NIR', "SWIR"]
    write_yaml(modified_config, modified)
    with pytest.raises(ValidationError, match="^Invalid pipeline.channels:"):
        cfg = load_config(modified_config)
    
    modified["pipeline"]["levels"] = [0, "1"]
    modified["pipeline"]["channels"] = ['nir']
    write_yaml(modified_config, modified)
    with pytest.raises(ValidationError, match="^Invalid pipeline.channels:"):
        cfg = load_config(modified_config)
    
    
def test_all_correct_config(tmp_path: Path, repo_root: Path) -> None:
    config = repo_root / 'tests' / 'configs' / 'test.yaml'
    raw = read_yaml(config)
    modified = copy.deepcopy(raw)
    modified_config = tmp_path / "broken.yaml"

    modified["run"]["input_dir"] = str(repo_root / 'tests' / 'data' / 'binary') # Correct path for input dir
    modified["run"]["output_dir"] = ""
    modified["run"]["spice_dir"] = ""
    modified["run"]['overwrite'] = True

    modified["calibration"]["calibration_dir"] = str(repo_root / 'calibration') # Correct path for calibration
    modified["calibration"]["dark"] = "DARKS/NIR_DARK.fits"
    modified["calibration"]["flat"] = "FLATS/NIR_FLAT.fits"
    modified["calibration"]["badpixels"] = "BADPIXELS/NIR_BADPIXELS.txt"
    modified["calibration"]["nir_radiance"] = "RADIANCE/NIR_RADIANCE.txt"
    modified["calibration"]["mir_radiance"] = "RADIANCE/MIR_RADIANCE.txt"
    modified["calibration"]["solar_ssi"] = "SOLAR/ssi_yearly_avg_e2024_c20250221.csv"

    modified["data"]["instrume"] = "NIRMIR"
    modified["data"]["origin"] = "ESA-COMET-INTERCEPTOR"
    modified["data"]["swcreate"] = "NIRMIRCAL"
    modified["data"]["missphas"] = "000_test"
    modified["data"]["observ"] = "pipeline-testing"
    modified["data"]["object"] = "test"
    modified["data"]["target"] = "TEST"

    modified["pipeline"]["levels"] = ["0", "1", "1A-extra"]
    modified["pipeline"]["channles"] = ["NIR", "MIR"]

    write_yaml(modified_config, modified)

    cfg = load_config(modified_config)

    assert cfg.run.input_dir == repo_root / 'tests' / 'data' / 'binary'
    assert cfg.run.output_dir == None
    assert cfg.run.spice_dir == None
    assert cfg.run.overwrite == True

    assert cfg.calib.calibration_dir == repo_root / 'calibration'
    assert cfg.calib.dark == "DARKS/NIR_DARK.fits"
    assert cfg.calib.flat == "FLATS/NIR_FLAT.fits"
    assert cfg.calib.badpixels == "BADPIXELS/NIR_BADPIXELS.txt"
    assert cfg.calib.nir_radiance == "RADIANCE/NIR_RADIANCE.txt"
    assert cfg.calib.mir_radiance == "RADIANCE/MIR_RADIANCE.txt"
    assert cfg.calib.solar_ssi == "SOLAR/ssi_yearly_avg_e2024_c20250221.csv"

    assert cfg.data.instrume == "NIRMIR"
    assert cfg.data.origin == "ESA-COMET-INTERCEPTOR"
    assert cfg.data.swcreate == "NIRMIRCAL"
    assert cfg.data.missphas == "000_test"
    assert cfg.data.observ == "pipeline-testing"
    assert cfg.data.object == "test"
    assert cfg.data.target == "TEST"

    assert cfg.pipeline.levels == ("0", "1", "1A-extra")
    assert cfg.pipeline.channels == ("NIR", "MIR")

