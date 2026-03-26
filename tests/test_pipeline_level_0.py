import pytest
import yaml
import copy

from pathlib import Path
from astropy.io import fits
from collections.abc import Sequence

from nirmir_pipeline.pipeline.utils.errors import ValidationError, ConfigError, PipelineError
from nirmir_pipeline.pipeline.utils.classes import Config, RunConfig, PipelineConfig, CalibConfig, DataConfig
from nirmir_pipeline.pipeline.utils.validate import _resolve_path, _validate_level_0_input_dir


from nirmir_pipeline.pipeline.run import run_pipeline
from nirmir_pipeline.pipeline.config import load_config

from nirmir_pipeline.pipeline.levels.level_0.metadata import collect_instrument_specific_metadata, collect_instrument_metadata, collect_spice_metadata
from nirmir_pipeline.pipeline.levels.level_0.run import run_level_0
from nirmir_pipeline.pipeline.levels.level_0.build_fits import build_fits

@pytest.fixture
def repo_root(pytestconfig) -> Path:
    return pytestconfig.rootpath

def read_yaml(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))

def write_yaml(path: Path, data: dict) -> None:
    path.write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8")

def test_validate_input_dir(tmp_path: Path, repo_root: Path) -> None:

    invalid_input_dir = tmp_path / 'tests' / 'data' / 'binary'
    missing_meta_dir = repo_root / 'tests' / 'data' / 'binary' / 'invalid_missing_meta'
    missing_acq_dir = repo_root / 'tests' / 'data' / 'binary' / 'invalid_missing_acq'
    missing_telemetry_dir = repo_root / 'tests' / 'data' / 'binary' / 'invalid_missing_telemetry'
    missing_config_dir = repo_root / 'tests' / 'data' / 'binary' / 'invalid_missing_config'

    with pytest.raises(ValidationError, match=r"^Input directory does not exist:"):
        _validate_level_0_input_dir(invalid_input_dir)

    with pytest.raises(ValidationError, match=r"^Missing required directory:"):
        _validate_level_0_input_dir(missing_meta_dir)
    
    with pytest.raises(ValidationError, match=r"^Missing required file\(s\) in"):
        _validate_level_0_input_dir(missing_telemetry_dir)

    with pytest.raises(ValidationError, match=r"^Missing required file\(s\) in"):
        _validate_level_0_input_dir(missing_config_dir)
    
    with pytest.raises(ValidationError, match=r"^No acquisition directory found."):
        _validate_level_0_input_dir(missing_acq_dir)


def test_corrupted_config(tmp_path: Path, repo_root: Path) -> None:

    invalid_input_dir = repo_root / 'tests' / 'data' / 'binary' / 'invalid'
    acq_dir = invalid_input_dir / 'acq_000'
    config = invalid_input_dir / 'meta' / 'config.json'

    results, issues = collect_instrument_specific_metadata(config, acq_dir, 'MIR')

    warnings = [i for i in issues if i.level == "warning"]
    assert len(warnings) == 1
    assert "Missing task file entry" in warnings[0].message

    results, issues = collect_instrument_specific_metadata(config, acq_dir, 'NIR')
    warnings = [i for i in issues if i.level == "warning"]
    assert len(warnings) == 1
    assert "The number of tasks is different to number of frames" in warnings[0].message
    assert len(results.fields.keys()) == 7
    assert results.fields["NIR_FRAMES"].value == '000,001,003,004'
    assert results.fields["NIR_TASK_000"].value == '0 0 0 100'
    assert results.fields["NIR_TASK_001"].value == '1 1 1 100'
    assert results.fields["NIR_TASK_002"].value == '2 2 2 100'
    assert results.fields["NIR_TASK_003"].value == '3 3 3 100'
    assert results.fields["NIR_TASK_004"].value == '4 4 4 100'

def test_corrupted_telemetry(tmp_path: Path, repo_root: Path) -> None:

    invalid_input_dir = repo_root / 'tests' / 'data' / 'binary' / 'invalid'
    telemetry = invalid_input_dir / 'meta' / 'telemetry.json'

    results, issues = collect_instrument_metadata(telemetry, 'NIR')
    warnings = [i for i in issues if i.level == "warning"]
    assert len(warnings) == 3
    assert results.CHANNELS.value == 'NIR'
    assert results.MIR_CCDTEMP.value == '0'
    assert results.NIR_CCDTEMP.value == 'UNK'
    assert results.NIR_FPI_TEMP1.value == 'UNK'
    assert results.NIR_FPI_TEMP2.value == 'UNK'
    assert results.MIR_FPI_TEMP2.value == '0'
    assert results.MIR_FPI_TEMP2.value == '0'

def test_spice_data_missing()-> None:

    result, issues = collect_spice_metadata("", 'test', '')
    warnings = [i for i in issues if i.level == "warning"]
    assert len(warnings) == 1
    assert "Failed to load meta kernel" in warnings[0].message

def test_nir_decoded_files(tmp_path: Path, repo_root) -> None:

    config = repo_root / 'tests' / 'configs' / 'test.yaml'

    raw = read_yaml(config)
    modified = copy.deepcopy(raw)
    modified_config = tmp_path / "modified.yaml"
    modified["run"]["input_dir"] = str(repo_root / 'tests' / 'data' / 'binary' / 'decoded') # Correct path for input dir
    modified["run"]["output_dir"] = str(tmp_path)
    modified["calibration"]["calibration_dir"] = str(repo_root / 'calibration') # Correct path for calibration
    write_yaml(modified_config, modified)

    cfg = load_config(modified_config)
    nir_result, issues = run_level_0(cfg, 'NIR')

    warnings = [i for i in issues if i.level == 'warning']
    assert len(warnings) == 1

    with fits.open(nir_result) as hdul:
        assert hdul[0].data.shape == (3, 518, 648)
        assert hdul[0].data.dtype == '<u2'

def test_nir_encoded_files(tmp_path: Path, repo_root) -> None:
    config = repo_root / 'tests' / 'configs' / 'test.yaml'

    raw = read_yaml(config)
    modified = copy.deepcopy(raw)
    modified_config = tmp_path / "modified.yaml"
    modified["run"]["input_dir"] = str(repo_root / 'tests' / 'data' / 'binary' / 'encoded') # Correct path for input dir
    modified["run"]["output_dir"] = str(tmp_path)
    modified["calibration"]["calibration_dir"] = str(repo_root / 'calibration') # Correct path for calibration
    write_yaml(modified_config, modified)

    cfg = load_config(modified_config)
    nir_result, issues = run_level_0(cfg, 'NIR')

    warnings = [i for i in issues if i.level == 'warning']
    assert len(warnings) == 1

    with fits.open(nir_result) as hdul:
        assert hdul[0].data.shape == (5, 518, 648)
        assert hdul[0].data.dtype == '<u2'

def test_mir_files(tmp_path: Path, repo_root: Path) -> None:
    config = repo_root / 'tests' / 'configs' / 'test.yaml'

    raw = read_yaml(config)
    modified = copy.deepcopy(raw)
    modified_config = tmp_path / "modified.yaml"
    modified["run"]["input_dir"] = str(repo_root / 'tests' / 'data' / 'binary' / 'encoded') # Correct path for input dir
    modified["run"]["output_dir"] = str(tmp_path)
    modified["calibration"]["calibration_dir"] = str(repo_root / 'calibration') # Correct path for calibration
    write_yaml(modified_config, modified)

    cfg = load_config(modified_config)
    mir_result, issues = run_level_0(cfg, 'MIR')

    warnings = [i for i in issues if i.level == 'warning']
    assert len(warnings) == 1

    with fits.open(mir_result) as hdul:
        assert hdul[0].data.shape == (6, )
        assert hdul[0].data.dtype == 'uint32'


def test_invalid_nir_shape(tmp_path: Path, repo_root: Path) -> None:

    config = repo_root / 'tests' / 'configs' / 'test.yaml'

    raw = read_yaml(config)
    modified = copy.deepcopy(raw)
    modified_config = tmp_path / "modified.yaml"
    modified["run"]["input_dir"] = str(repo_root / 'tests' / 'data' / 'binary' / 'invalid_nir_shape') # Correct path for input dir
    modified["run"]["output_dir"] = str(tmp_path)
    modified["calibration"]["calibration_dir"] = str(repo_root / 'calibration') # Correct path for calibration
    write_yaml(modified_config, modified)

    cfg = load_config(modified_config)
    with pytest.raises(PipelineError, match=r"Error creating fits data unit. List of frames seems to be empty."):
        mir_result, issues = run_level_0(cfg, 'NIR')

def test_level_0_works_correctly(tmp_path, repo_root: Path) -> None:

    config = repo_root / 'tests' / 'configs' / 'test.yaml'

    raw = read_yaml(config)
    modified = copy.deepcopy(raw)
    modified_config = tmp_path / "modified.yaml"
    modified["run"]["input_dir"] = str(repo_root / 'tests' / 'data' / 'binary' / 'decoded') # Correct path for input dir
    modified["run"]["output_dir"] = str(tmp_path)
    modified["calibration"]["calibration_dir"] = str(repo_root / 'calibration') # Correct path for calibration
    modified["pipeline"]["levels"] = ["0"]
    modified["pipeline"]["channels"] = ["NIR"]
    modified["data"]["missphas"] = 'pytest'
    write_yaml(modified_config, modified)
    run_pipeline(modified_config)

    fits_file = tmp_path / 'pytest' / 'NIR_000000_200101T015948_0A.fits'

    assert fits_file.exists()

    with fits.open(fits_file) as hdul:
        header = hdul[0].header
        data = hdul[0].data
    
    assert header.get("MISSPHAS") == 'pytest'
    assert header.get("NIR_FRAMES") == '000,001,002'

    assert data.shape == (3,518,648)
    assert data.dtype == '<u2'

