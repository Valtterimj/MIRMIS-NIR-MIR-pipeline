import pytest
import numpy as np

from pathlib import Path
from astropy.io import fits
from astropy.io.fits import HDUList

from nirmir_pipeline.pipeline.levels.level_1.calibrate_header import calibrate_header
from nirmir_pipeline.pipeline.utils.classes import Issue, CalibConfig
from nirmir_pipeline.pipeline.utils.utilities import convert_to_float32, convert_to_float64

from nirmir_pipeline.pipeline.levels.level_1.extract_cds import extract_cds_pixels
from nirmir_pipeline.pipeline.levels.level_1.dark_background import dark_subtraction
from nirmir_pipeline.pipeline.levels.level_1.flat_field import flat_field_calibration
from nirmir_pipeline.pipeline.levels.level_1.bad_pixels import replace_bad_pixels
from nirmir_pipeline.pipeline.levels.level_1.radiometric import radiometric_calibration
from nirmir_pipeline.pipeline.levels.level_1.level_1b import run_level_1b

from nirmir_pipeline.pipeline.levels.level_1.reflectance import reflectance_calibration
from nirmir_pipeline.pipeline.levels.level_1.run import run_level_1


@pytest.fixture
def repo_root(pytestconfig) -> Path:
    return pytestconfig.rootpath


def test_temperature_calibration(tmp_path: Path, repo_root: Path) -> None:

    level_0A = repo_root / 'tests' / 'data' / 'fits' / 'NIR_0A.fits'

    result, issues = calibrate_header(level_0A, tmp_path, 'NIR')
    assert all(issue.level == "info" for issue in issues)
    assert 'New fits file created:' in issues[0].message

    with fits.open(result) as hdul:
        header = hdul[0].header
        assert header['NIR_FPI_TEMP1'] == '311.22'
        assert header['NIR_FPI_TEMP2'] == '297.79'

def test_wl_exp_calibration(tmp_path: Path, repo_root: Path) -> None:

    level_0A = repo_root / 'tests' / 'data' / 'fits' / 'NIR_0A.fits'

    result, issues = calibrate_header(level_0A, tmp_path, 'NIR')
    assert all(issue.level == "info" for issue in issues)
    with fits.open(result) as hdul:
        header = hdul[0].header
        assert header['NIR_WL_000'] == '521.46'
        assert header['NIR_WL_001'] == '521.46'
        assert header['NIR_WL_002'] == '521.46'
        assert header['NIR_WL_003'] == '521.46'
        assert header['NIR_WL_004'] == '582.28'
        assert header['NIR_WL_005'] == '582.28'
        assert header['NIR_WL_006'] == '582.28'
        assert header['NIR_WL_007'] == '582.28'

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

    data_64, issue = convert_to_float64(hdul)
    assert issue.level == "info" 
    assert data_64[0].data.dtype == np.dtype(np.float64)

    data_32, issue = convert_to_float32(data_64)
    assert issue.level == "info"
    assert data_32[0].data.dtype == np.dtype(np.float32)


def test_extract_cds(tmp_path: Path, repo_root: Path) -> None:

    level_1A = repo_root / 'tests' / 'data' / 'fits' / 'NIR_1A.fits'

    with fits.open(level_1A) as hdul:

        result, issues = extract_cds_pixels(hdul)
        assert all(issue.level == "info" for issue in issues)

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

    result, issues = dark_subtraction(hdul, twos)
    assert all(issue.level == "info" for issue in issues)
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

    result, issues = flat_field_calibration(hdul, twos)
    assert all(issue.level == "info" for issue in issues)
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

    results, issues = replace_bad_pixels(hdul, badpixels)
    assert all(issue.level == "info" for issue in issues)

    assert results[0].data[7, 3] == 73.

    # Rows and columns
    data = np.arange(0, 100).reshape(ny, nx)
    data[5, :] = 0
    data[:, 3] = 10000
    data = np.stack([data, data])
    hdu = fits.PrimaryHDU(data)
    hdul = fits.HDUList([hdu])
    badpixels = repo_root / 'tests' / 'data' / 'calib' / 'col_row.txt'

    results, issues = replace_bad_pixels(hdul, badpixels)
    assert all(issue.level == "info" for issue in issues)
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

def test_radiometric(tmp_path: Path, repo_root: Path) -> None:

    nir = repo_root / 'tests' / 'data' / 'fits' / 'NIR_1A.fits'
    nir_radiance = repo_root / 'calibration' / 'RADIANCE' / 'NIR_RADIANCE.txt'

    with fits.open(nir) as nir_hdul:
        converted, _ = convert_to_float64(nir_hdul)
        nir_radiometric, _ = radiometric_calibration(converted, nir_radiance)
        val_0 = nir_radiometric[0].data[0, 0, 0]
        val_1 = nir_radiometric[0].data[1, 0, 0]
        assert val_0 <= 1
        assert val_1 <= 1

    mir = repo_root / 'tests' / 'data' / 'fits' / 'MIR_1A.fits'
    mir_radiance = repo_root / 'calibration' / 'RADIANCE' / 'MIR_RADIANCE.txt'

    with fits.open(mir) as mir_hdul:
        val = mir_hdul[0].data
        print(val)
        mir_radiometric, issues = radiometric_calibration(mir_hdul, mir_radiance)
        assert all(issue.level == "info" for issue in issues)

        val_0 = mir_radiometric[0].data
        assert np.all(val_0 < 1)

def test_level_1b(tmp_path: Path, repo_root: Path) -> None:

    nir = repo_root / 'tests' / 'data' / 'fits' / 'NIR_1A.fits'
    mir = repo_root / 'tests' / 'data' / 'fits' / 'MIR_1A.fits'

    calib = CalibConfig(
        calibration_dir=repo_root / 'calibration',
        dark= "DARKS/NIR_DARK.fits",
        flat= "FLATS/NIR_FLAT.fits",
        badpixels= "BADPIXELS/NIR_BADPIXELS.txt",
        nir_radiance= "RADIANCE/NIR_RADIANCE.txt",
        mir_radiance= "RADIANCE/MIR_RADIANCE.txt",
        solar_ssi= "SOLAR/ssi_yearly_avg_e2024_c20250221.csv",
    )

    results_nir, nir_issues = run_level_1b(nir, tmp_path, calib, 'NIR')
    assert all(issue.level == "info" for issue in nir_issues)
    
    with fits.open(results_nir) as nir_hdul:
        assert nir_hdul[0].header['PROCLEVL'] == '1B'
        val_nir = nir_hdul[0].data[0, 0, 0]
        assert val_nir <= 1

    results_mir, mir_issues = run_level_1b(mir, tmp_path, calib, 'MIR')

    assert all(issue.level == "info" for issue in mir_issues)

    with fits.open(results_mir) as mir_hdul:
        assert mir_hdul[0].header['PROCLEVL'] == '1B'
        val_mir = mir_hdul[0].data[0]
        assert np.all(val_mir <= 1)


def test_reflectance_calibration(tmp_path: Path, repo_root: Path) -> None:

    nir = repo_root / 'tests' / 'data' / 'fits' / 'NIR_1B.fits'
    mir = repo_root / 'tests' / 'data' / 'fits' / 'MIR_1B.fits'

    with fits.open(nir, mode='update') as nir_hdul:
        header = nir_hdul[0].header
        header['SOLAR_D'] = '1'

    solar_ssi = repo_root / 'calibration' / 'SOLAR' / 'ssi_yearly_avg_e2024_c20250221.csv'
    results_nir, nir_issues = reflectance_calibration(nir, tmp_path, solar_ssi)

    assert all(issue.level == "info" for issue in nir_issues)

    with fits.open(mir, mode='update') as mir_hdul:
        header = mir_hdul[0].header
        header['SOLAR_D'] = '1'

    results_mir, mir_issues = reflectance_calibration(mir, tmp_path, solar_ssi, fwhm_nm=40)
    assert all(issue.level == "info" for issue in mir_issues)

def test_reflectance_calibration_2(tmp_path: Path, repo_root: Path) -> None:


    ones = np.ones(shape=(10,10,10), dtype=np.float64)
    dict= {
        'CHANNELS'   : 'NIR',
        'NIR_FRAMES' : '000,001,002,003,004,005,006,007,008,009',
        'NIR_WL_000' : '950',
        'NIR_WL_001' : '1000',
        'NIR_WL_002' : '1050',
        'NIR_WL_003' : '1100',
        'NIR_WL_004' : '1150',
        'NIR_WL_005' : '1200',
        'NIR_WL_006' : '1250',
        'NIR_WL_007' : '1300',
        'NIR_WL_008' : '1350',
        'NIR_WL_009' : '1400',
        'SOLAR_D'    : '1'
    }
    header = fits.Header(cards=dict)
    hdu = fits.PrimaryHDU(ones, header)
    hdul = fits.HDUList([hdu])
    file_path = tmp_path / 'NIR_000000_200101T015948_1B.fits'
    hdul.writeto(file_path, overwrite=True)

    solar_ssi = repo_root / 'calibration' / 'SOLAR' / 'ssi_yearly_avg_e2024_c20250221.csv'
    new_file, issues = reflectance_calibration(file_path, tmp_path, solar_ssi)

    with fits.open(new_file) as hdul:
        data = hdul[0].data

    assert all(issue.level == 'info' for issue in issues)
    assert np.all(data == data[:, 0:1, 0:1])
    assert np.all(data.min(axis=(1,2)) == data.max(axis=(1,2)))


def test_run_level_1_valid(tmp_path, repo_root) -> None:

    nir_level_0 = repo_root / 'tests' / 'data' / 'fits' / 'NIR_000000_0000000000000_0A.fits'
    mir_level_0 = repo_root / 'tests' / 'data' / 'fits' / 'MIR_000000_0000000000000_0A.fits'
    calib_dir = repo_root / 'calibration'

    calib = CalibConfig(
        calibration_dir=calib_dir,
        dark="DARKS/NIR_DARK.fits",
        flat="FLATS/NIR_FLAT.fits",
        badpixels="BADPIXELS/NIR_BADPIXELS.TXT",
        nir_radiance="RADIANCE/NIR_RADIANCE.txt",
        mir_radiance="RADIANCE/MIR_RADIANCE.txt",
        solar_ssi="SOLAR/ssi_yearly_avg_e2024_c20250221.csv"
    )

    results, issues = run_level_1(nir_level_0, tmp_path, calib, 'NIR')
    assert all(issue.level == 'info' for issue in issues)

    with fits.open(results) as nir_hdul:
        # assert np.all(nir_hdul[0].data < 1)
        n_data = nir_hdul[0].data
        shape = n_data.shape
        assert shape == (7, 512, 640)