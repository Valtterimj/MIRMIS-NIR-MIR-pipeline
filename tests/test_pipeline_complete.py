import pytest
import yaml
import copy

from pathlib import Path
from astropy.io import fits

from nirmir_pipeline.pipeline.run import run_pipeline

@pytest.fixture
def repo_root(pytestconfig) -> Path:
    return pytestconfig.rootpath

def read_yaml(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))

def write_yaml(path: Path, data: dict) -> None:
    path.write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8")

def test_full_pipeline_workflow(tmp_path: Path, repo_root: Path):
    config = repo_root / 'tests' / 'configs' / 'test.yaml'
    raw = read_yaml(config)
    modified = copy.deepcopy(raw)
    modified_config = tmp_path / "modified.yaml"
    modified["run"]["input_dir"] = str(repo_root / 'tests' / 'data' / 'binary' / 'decoded') # Correct path for input dir
    modified["run"]["output_dir"] = str(tmp_path)
    modified["calibration"]["calibration_dir"] = str(repo_root / 'tests' / 'data' / 'calib') # Correct path for calibration
    modified["calibration"]["dark"] = 'DARK_05.fits'
    modified["calibration"]["flat"] = 'FLAT_05.fits'
    modified["calibration"]["badpixels"] = 'BADPIXELS.txt'
    modified["calibration"]["nir_radiance"] = 'RADIANCE.txt'
    modified["calibration"]["mir_radiance"] = 'RADIANCE.txt'
    modified["calibration"]["solar_ssi"] = str(repo_root / 'calibration' / 'SOLAR' / 'ssi_yearly_avg_e2024_c20250221.csv')
    modified["calibration"]["solar_distance"] = "1"
    modified["pipeline"]["levels"] = ["0", "1"]
    modified["pipeline"]["channels"] = ["NIR", "MIR"]
    
    write_yaml(modified_config, modified)

    output_dir, warnings, errors = run_pipeline(modified_config)

    assert len(errors) == 0
    if len(warnings) > 0:
        assert len(warnings) == 2
        assert 'Failed to load meta kernel' in warnings[0].message
        assert 'Failed to load meta kernel' in warnings[1].message

    expected = {
        "MIR": {"0A", "1A", "1B"},
        "NIR": {"0A", "1A", "1B", "1C"},
    }
    assert output_dir.exists()

    files = list(output_dir.iterdir())

    assert files, "No .fits files found"

    found = {"MIR": set(), "NIR": set()}
    for f in files:
        name = f.stem
        parts = name.split("_")

        assert len(parts) >= 2, f"Invalid filename format: {f.name}"

        channel = parts[0]
        level = parts[-1]

        assert channel in expected, f"Unkown channel '{channel}' in {f.name}"

        found[channel].add(level)
    
    for channel, expected_levels in expected.items():
        missing = expected_levels - found[channel]
        extra = found[channel] - expected_levels

        assert not missing, (
            f"{channel}: missing levels {missing}, found {found[channel]}"
        )
        assert not extra, (
            f"{channel}: unexpected levels {extra}, expected {expected_levels}"
        )