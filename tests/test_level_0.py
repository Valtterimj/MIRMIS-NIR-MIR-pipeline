import pytest
import yaml
import copy

from pathlib import Path
from astropy.io import fits

from nirmir_pipeline.pipeline.utils.errors import ValidationError, ConfigError, PipelineError
from nirmir_pipeline.pipeline.utils.validate import _resolve_path, _validate_output_dir

import nirmir_pipeline.pipeline.config as cfg
from nirmir_pipeline.pipeline.levels.level_0.run import run_level_0

@pytest.fixture
def repo_root(pytestconfig) -> Path:
    return pytestconfig.rootpath

def _minimal_raw(tmp_path: Path) -> dict:
    input_dir = "../tests/data/NIR_valid"
    spice_dir = tmp_path / "spice"
    output_dir = tmp_path / "output"
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
            "origin" : "pytest",
            "swcreate": "pytest",
            "missphas": "pytest",
            "observ": "pytest",
            "object": "pytest",
            "target": "pytest",
        },
        "pipeline": {
            "levels": ["0"],
            "channels": ["NIR"]
        }
    }

def test_load_config_valid(tmp_path: Path, repo_root: Path) -> None:

    config_file = repo_root / "tests" / "configs" / "test.yaml"

    # Test laoding config
    c = cfg.load_config(config_file)

    # Test data exists
    assert c.run.input_dir == repo_root / "tests" / "data" / "NIR_valid"
    assert c.config_path == config_file.resolve()

    fields = c.data.__dataclass_fields__.keys()
    assert set(fields) == {
        "instrume",
        "origin",
        "swcreate",
        "missphas",
        "observ",
        "object",
        "target",
    }
    assert c.data.swcreate == "pytest"
    assert c.pipeline.levels == ("0",)
    assert c.pipeline.channels == ("NIR", )

def test_load_config_valid(tmp_path: Path, repo_root: Path) -> None:

    config_file = repo_root / "tests" / "configs"

    # Test invalid config path leads to configError
    with pytest.raises(ConfigError, match=r"\bdirectory\b"):
        cfg.load_config(config_file)


def test_load_config_invalid_data(tmp_path: Path) -> None:
    raw = _minimal_raw(tmp_path)
    del raw["data"]["origin"]

    config_file = tmp_path / "pipeline.yaml"
    config_file.write_text(yaml.safe_dump(raw), encoding="utf-8")

    with pytest.raises(ValidationError, match=r"\borigin\b"):
        cfg.load_config(config_file)

def test_load_config_invalid_input(tmp_path: Path, repo_root: Path) -> None:

    config_file = repo_root / "tests" / "configs" / "test.yaml"

    conf_path = cfg._resolve_config_path(config_file)

    raw = cfg._read_yaml(conf_path)
    raw_invalid = copy.deepcopy(raw)

    raw_invalid["run"]["input_dir"] = ""

    with pytest.raises(ValidationError, match=r"\binput_dir\b"):
        cfg._parse_config_dict(raw_invalid, config_path=conf_path)
    

def test_load_config_invalid_pipeline(tmp_path: Path, repo_root: Path) -> None:

    config_file = repo_root / "tests" / "configs" / "test.yaml"
    conf_path = cfg._resolve_config_path(config_file)
    raw = cfg._read_yaml(conf_path)

    raw_invalid_levels = copy.deepcopy(raw)

    raw_invalid_levels["pipeline"]["levels"] = [""]
    with pytest.raises(ValidationError):
        cfg._parse_config_dict(raw_invalid_levels, config_path=conf_path)
    
    raw_invalid_levels["pipeline"]["levels"] = ["1", 2]
    with pytest.raises(ValidationError):
        cfg._parse_config_dict(raw_invalid_levels, config_path=conf_path)
    
    raw_invalid_levels["pipeline"]["levels"] = ["0", "1", "4"]
    with pytest.raises(ValidationError, match=r"\bpipeline.levels\b"):
        cfg._parse_config_dict(raw_invalid_levels, config_path=conf_path)
    
    raw_invalid_levels["pipeline"]["levels"] = ["0", "1"]
    raw_invalid_levels["pipeline"]["channels"] = ["SWIR"]
    with pytest.raises(ValidationError, match=r"\bpipeline.channels\b"):
        cfg._parse_config_dict(raw_invalid_levels, config_path=conf_path)

def test_output_dir_valid(tmp_path: Path, repo_root: Path) -> None:

    output_path = _validate_output_dir(tmp_path, missphas='testpy', base_dir=None)

    assert output_path == tmp_path / 'testpy'

def test_output_dir_invalid(tmp_path: Path, repo_root: Path) -> None:

    file = repo_root / "tests" / "configs" / "test.yaml"

    with pytest.raises(ValidationError, match=r"\bdirectory\b"):
        _validate_output_dir(file, missphas='testpy', base_dir=None)



def test_build_fits_NIR(tmp_path: Path, repo_root: Path) -> None:

    config_file = repo_root / "tests" / "configs" / "test.yaml"

    c = cfg.load_config(config_file)
    c.run.output_dir = tmp_path

    level0_result = run_level_0(c, 'NIR')
    assert str(level0_result[0]) == f'{tmp_path}/NIR_000000_200101T015948_0A.fits'

    with fits.open(level0_result[0]) as hdul:

        header = hdul[0].header
        data = hdul[0].data

        assert header['INSTRUME'] == 'NIRMIR'
        assert header['ORIGIN'] == "ESA-COMET-INTERCEPTOR"
        assert header['MISSPHAS'] == "pytest"
        assert header['OSERV_ID'] == "pytest"
        assert header['FILENAME'] == "NIR_000000_200101T015948_0A.fits"
        assert header['ORIGFILE'] == "dc_0_exp_000.bin.jp2"
        assert header['SWCREATE'] == "pytest"
        assert header['PROCLEVL'] == "0A"
        #Instrument data
        assert header['DATE_OBS'] == "2020-01-01T01:59:48.000"
        assert header['SC_CLK'] == 'UNK' # TODO: alter this once spacecraft clock count is available
        assert header['OBJECT'] == 'pytest'
        # Spice data
        # TODO: Implement spice data check when a spice kernel is available
        # Instrument Specific data
        assert header['CHANNELS'] == 'NIR'
        assert header['NIR_CCDTEMP'] == '0'
        assert header['NIR_FPI_TEMP1'] == '2143'
        assert header['NIR_FPI_TEMP2'] == '2168'
        assert header['MIR_CCDTEMP'] == '0'
        assert header['MIR_FPI_TEMP1'] == '0'
        assert header['MIR_FPI_TEMP2'] == '0'
        assert header['NIR_FRAMES'] == '000,001,002,003,004,005,006,007'
        assert header['NIR_TASK_NUMBER'] == '8'
        assert header['NIR_TASK_000'] == '17615 17879 17579 100'

        # Image data
        assert data.shape[0] == 8
        assert data.shape[1] == 518
        assert data.shape[2] == 648
        assert data[0].dtype == 'uint16'
        assert data[0][250][250] == 6030


def test_build_fits_MIR(tmp_path: Path, repo_root: Path) -> None:

    config_file = repo_root / "tests" / "configs" / "test.yaml"

    c = cfg.load_config(config_file)
    c.run.output_dir = tmp_path

    level0_result = run_level_0(c, 'MIR')
    assert str(level0_result[0]) == f'{tmp_path}/MIR_000000_200101T015948_0A.fits'

    with fits.open(level0_result[0]) as hdul:

        header = hdul[0].header
        data = hdul[0].data

        assert header['INSTRUME'] == 'NIRMIR'
        assert header['ORIGIN'] == "ESA-COMET-INTERCEPTOR"
        assert header['MISSPHAS'] == "pytest"
        assert header['OSERV_ID'] == "pytest"
        assert header['FILENAME'] == "MIR_000000_200101T015948_0A.fits"
        assert header['ORIGFILE'] == "dc_1_exp_000.bin"
        assert header['SWCREATE'] == "pytest"
        assert header['PROCLEVL'] == "0A"
        #Instrument data
        assert header['DATE_OBS'] == "2020-01-01T01:59:48.000"
        assert header['SC_CLK'] == 'UNK' # TODO: alter this once spacecraft clock count is available
        assert header['OBJECT'] == 'pytest'
        # Spice data
        # TODO: Implement spice data check when a spice kernel is available
        # Instrument Specific data
        assert header['CHANNELS'] == 'MIR'
        assert header['MIR_FRAMES'] == '000,001,002,003,004,005'
        assert header['MIR_TASK_NUMBER'] == '6'

        # Image data
        assert data.shape[0] == 6
        assert data[0].dtype == 'uint32'
        assert data[0] == 1194