import pytest

from pathlib import Path
from astropy.io import fits

from nirmir_pipeline.pipeline.levels.level_1.calibrate_header import calibrate_header
from nirmir_pipeline.pipeline.utils.classes import Issue
from nirmir_pipeline.pipeline.utils.utilities import kelvin


@pytest.fixture
def repo_root(pytestconfig) -> Path:
    return pytestconfig.rootpath


def test_temperature_calibration(tmp_path: Path, repo_root: Path) -> None:

    level_0A = repo_root / 'tests' / 'data' / 'fits' / 'NIR_0A.fits'

    result, issues = calibrate_header(level_0A, tmp_path, 'NIR')

    assert len(issues) == 1
    assert 'New fits file created:' in issues[0].message

    with fits.open(result) as hdul:
        header = hdul[0].header
        assert header['NIR_FPI_TEMP1'] == '311.22'
        assert header['NIR_FPI_TEMP2'] == '297.79'

def test_wl_exp_calibration(tmp_path: Path, repo_root: Path) -> None:

    level_0A = repo_root / 'tests' / 'data' / 'fits' / 'NIR_0A.fits'

    result, issues = calibrate_header(level_0A, tmp_path, 'NIR')

    with fits.open(result) as hdul:
        header = hdul[0].header
        assert header['NIR_WL_000'] == '521'
        assert header['NIR_WL_001'] == '521'
        assert header['NIR_WL_002'] == '521'
        assert header['NIR_WL_003'] == '521'
        assert header['NIR_WL_004'] == '582'
        assert header['NIR_WL_005'] == '582'
        assert header['NIR_WL_006'] == '582'
        assert header['NIR_WL_007'] == '582'

        assert header['NIR_EXP_000'] == '0.001'
        assert header['NIR_EXP_001'] == '0.001'
        assert header['NIR_EXP_002'] == '0.001'
        assert header['NIR_EXP_003'] == '0.001'
        assert header['NIR_EXP_004'] == '0.001'
        assert header['NIR_EXP_005'] == '0.001'
        assert header['NIR_EXP_006'] == '0.001'
        assert header['NIR_EXP_007'] == '0.001'



