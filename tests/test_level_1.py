import pytest
import numpy as np

from pathlib import Path
from astropy.io import fits
from astropy.io.fits import HDUList

from nirmir_pipeline.pipeline.levels.level_1.calibrate_header import calibrate_header
from nirmir_pipeline.pipeline.utils.classes import Issue
from nirmir_pipeline.pipeline.utils.utilities import convert_to_float32, convert_to_float64

from nirmir_pipeline.pipeline.levels.level_1.extract_cds import extract_cds_pixels
from nirmir_pipeline.pipeline.levels.level_1.dark_background import dark_subtraction
from nirmir_pipeline.pipeline.levels.level_1.flat_field import flat_field_calibration
from nirmir_pipeline.pipeline.levels.level_1.bad_pixels import replace_bad_pixels


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


# Lelvel 1b tests

def test_convert_to_64_32(tmp_path: Path) -> None:

    ny, nx = 518, 648
    data = np.ones((ny, nx), dtype=np.uint16)

    hdu = fits.PrimaryHDU(data=data)
    hdul = fits.HDUList([hdu])

    data_64, _ = convert_to_float64(hdul)
    assert data_64[0].data.dtype == np.dtype(np.float64)

    data_32, _ = convert_to_float32(data_64)
    assert data_32[0].data.dtype == np.dtype(np.float32)


def test_extract_cds(tmp_path: Path, repo_root: Path) -> None:

    level_1A = repo_root / 'tests' / 'data' / 'fits' / 'NIR_1A.fits'

    with fits.open(level_1A) as hdul:

        result, _ = extract_cds_pixels(hdul)

    assert result[0].data.shape == (2, 512, 640)

def test_dark_background(tmp_path: Path, repo_root: Path) -> None:

    ny, nx = 10, 10

    # single frame 
    data = np.full((ny, nx), 100, dtype=np.float64)
    hdu = fits.PrimaryHDU(data=data)
    hdul = fits.HDUList([hdu])
    ones = repo_root / 'tests' / 'data' / 'calib' / 'ones.fits'

    result, _ = dark_subtraction(hdul, ones)
    assert np.all(result[0].data == 99)

    # multi frame
    multi_data = np.full((2, ny, nx), 100, dtype=np.float64)
    hdu = fits.PrimaryHDU(data=multi_data)
    hdul = fits.HDUList([hdu])

    result, _ = dark_subtraction(hdul, ones)
    assert np.all(result[0].data == 99)
    assert np.all(result[0].data[1] == 99)

    # zeros 
    data_ones = np.full((ny, nx), 1, dtype=np.float64)
    hdu = fits.PrimaryHDU(data=data_ones)
    hdul = fits.HDUList([hdu])
    twos = repo_root / 'tests' / 'data' / 'calib' / 'twos.fits'

    result, _ = dark_subtraction(hdul, twos)
    assert np.all(result[0].data == 0)

    # range
    data = np.full((2, ny, nx), 100, dtype=np.float64)
    hdu = fits.PrimaryHDU(data=data)
    hdul = fits.HDUList([hdu])
    range = repo_root / 'tests' / 'data' / 'calib' / 'range.fits'
    reverse_range, _ = dark_subtraction(hdul, range)
    expected = np.arange(100, 0, -1).reshape(10, 10)
    assert np.array_equal(reverse_range[0].data[1], expected)

def test_flat_field(repo_root: Path) -> None:

    ny, nx = 10, 10
    
    # singel frame
    data = np.full((ny,nx), 2, dtype=np.float64)
    hdu = fits.PrimaryHDU(data)
    hdul = fits.HDUList([hdu])
    twos = repo_root / 'tests' / 'data' / 'calib' / 'twos.fits'
    result, _ = flat_field_calibration(hdul, twos)
    assert np.all(result[0].data == 1)

    # multiframe range
    data = np.arange(0, 100).reshape(10, 10)
    data = np.stack([data, data])
    hdu = fits.PrimaryHDU(data)
    hdul = fits.HDUList([hdu])
    twos = repo_root / 'tests' / 'data' / 'calib' / 'twos.fits'

    result, _ = flat_field_calibration(hdul, twos)
    assert result[0].data[0][0][1] == 0.5


def test_bad_pixel(repo_root: Path) -> None:
    
    ny, nx = 10, 10
    # single pixels
    data = np.arange(0, 100).reshape(ny, nx)
    data[0][0] = 0
    data[7][3] = 0
    data = np.stack([data, data])
    hdu = fits.PrimaryHDU(data)
    hdul = fits.HDUList([hdu])
    badpixels = repo_root / 'tests' / 'data' / 'calib' / 'pixels.txt'

    results, _ = replace_bad_pixels(hdul, badpixels)

    assert results[0].data[0][7][3] == 73.
    assert round(results[0].data[1][0][0], 2) == 6.00


    # single pixel single frame
    data = np.arange(0, 100).reshape(ny, nx)
    data[0][0] = 0
    data[7][3] = 0
    hdu = fits.PrimaryHDU(data)
    hdul = fits.HDUList([hdu])
    badpixels = repo_root / 'tests' / 'data' / 'calib' / 'pixels.txt'

    results, _ = replace_bad_pixels(hdul, badpixels)

    assert results[0].data[7, 3] == 73.

    # Rows and columns
    data = np.arange(0, 100).reshape(ny, nx)
    data[5, :] = 0
    data[:, 3] = 10000
    data = np.stack([data, data])
    hdu = fits.PrimaryHDU(data)
    hdul = fits.HDUList([hdu])
    badpixels = repo_root / 'tests' / 'data' / 'calib' / 'col_row.txt'

    results, _ = replace_bad_pixels(hdul, badpixels)

    assert results[0].data[0, 5, 3] == 53

    # region cluster
    data = np.arange(0, 100).reshape(ny, nx)
    data[3:6, 3:6] = 0
    data = np.stack([data, data])
    hdu = fits.PrimaryHDU(data)
    hdul = fits.HDUList([hdu])
    badpixels = repo_root / 'tests' / 'data' / 'calib' / 'cluster.txt'
    results, _ = replace_bad_pixels(hdul, badpixels)

    assert results[0].data[0, 4, 4] == 44






