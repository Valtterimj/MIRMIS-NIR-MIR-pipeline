import pytest
import yaml
import copy
import numpy as np

from pathlib import Path
from astropy.io import fits
from collections.abc import Sequence

from nirmir_pipeline.pipeline.utils.errors import PipelineError, ValidationError, CalibrationError
from nirmir_pipeline.pipeline.utils.classes import CalibConfig

from nirmir_pipeline.pipeline.config import load_config
from nirmir_pipeline.pipeline.utils.utilities import convert_to_float32, convert_to_float64
from nirmir_pipeline.pipeline.levels.level_1.run import run_level_1
from nirmir_pipeline.pipeline.levels.level_1.calibrate_header import calibrate_header
from nirmir_pipeline.pipeline.utils.calib_conversions import wavelength_conversion, exposure_conversion, det_temp_conversion, fpi_temp_conversion
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

def read_yaml(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))

def write_yaml(path: Path, data: dict) -> None:
    path.write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8")

def make_modified_fits_hdr(
        source_path: Path,
        tmp_path,
        output_name: str,
        updates: dict[str, tuple[object, str] | object] | None,
        deletes: list[str] | None = None,
    ) -> Path:
    
    out_path = tmp_path / output_name

    with fits.open(source_path) as hdul:
        hdul_copy = hdul.copy()
        hdr = hdul_copy[0].header

        if updates:
            for key, val in updates.items():
                hdr[key] = val
        
        if deletes:
            for key in deletes:
                if key in hdr:
                    del hdr[key]
        
        hdul_copy.writeto(out_path, overwrite=True)
    return out_path

def make_modified_fits_data(
        source_path: Path,
        tmp_path,
        output_name: str,
        pixel_updates: dict[tuple[int, int], int | float] | None = None,
        row_updates: dict[int, int | float] | None = None, 
        col_updates: dict[int, int | float] | None = None, 
    ) -> Path:

    out_path = tmp_path / output_name

    with fits.open(source_path) as hdul:
        hdul_copy = hdul.copy()
        data = hdul_copy[0].data
        n_frames, ny, nx = data.shape
    
        if pixel_updates:
            for (y, x), val in pixel_updates.items():
                assert 0 <= y < ny, f"y index {y} out of bounds for height {ny}"
                assert 0 <= x < nx, f"x index {x} out of bounds for width {nx}"
                data[:, y, x] = val

        if row_updates:
            for y, val in row_updates.items():
                assert 0 <= y < ny, f"y index {y} out of bounds for height {ny}"
                assert 0 <= x < nx, f"x index {x} out of bounds for width {nx}"
                data[:, y, :] = val
        
        if col_updates:
            for x, val in col_updates.items():
                assert 0 <= y < ny, f"y index {y} out of bounds for height {ny}"
                assert 0 <= x < nx, f"x index {x} out of bounds for width {nx}"
                data[:, :, x] = val

        hdul_copy.writeto(out_path, overwrite=True)
    return out_path


def assert_temp_string_equal(a: str, b: str, abs_tol: float= 0.1) -> None:
    special = {"N/A", "UNK"}

    if a in special or b in special:
        assert a == b
        return

    assert float(a) == pytest.approx(float(b), abs=abs_tol)

def test_invalid_input_dir_level_1(tmp_path: Path, repo_root: Path) -> None:
    config = repo_root / 'tests' / 'configs' / 'test.yaml'

    raw = read_yaml(config)
    modified = copy.deepcopy(raw)
    modified_config = tmp_path / "modified.yaml"
    modified["run"]["input_dir"] = str(repo_root / 'tests' / 'data' / 'binary') # Correct path for input dir
    modified["run"]["output_dir"] = str(tmp_path)
    modified["calibration"]["calibration_dir"] = str(repo_root / 'calibration')
    modified["pipeline"]["levels"] = ["1"]
    write_yaml(modified_config, modified)

    cfg = load_config(modified_config)

    with pytest.raises(ValidationError, match=r"^No level '0A' file for channel 'NIR'"):
        run_level_1(cfg, 'NIR')
    
    modified["pipeline"]["levels"] = ["1B"]
    write_yaml(modified_config, modified)

    cfg = load_config(modified_config)
    with pytest.raises(ValidationError, match=r"^No level '1A' file for channel 'MIR'"):
        run_level_1(cfg, 'MIR')

    modified["pipeline"]["levels"] = ["1C"]
    write_yaml(modified_config, modified)

    cfg = load_config(modified_config)
    with pytest.raises(ValidationError, match=r"^No level '1B' file for channel 'NIR'"):
        run_level_1(cfg, 'NIR')

def test_header_missing_temp(tmp_path: Path, repo_root: Path) -> None:

    level_0A_fits = repo_root / 'tests' / 'data' / 'fits' / 'lvl_0A' / 'NIR_000000_1111111111111_0A.fits'
    modified_file = make_modified_fits_hdr(
        source_path=level_0A_fits,
        tmp_path=tmp_path,
        output_name='NIR_000000_1111111111111_0A.fits',
        updates={
            "NIR_CCDTEMP" : ("1", "NIR detector temperature [DN]"),
            "NIR_FPI_TEMP1" : ("1", "NIR FPI 1 temperature [DN] "),
            "MIR_FPI_TEMP2" : ("not a number", "NIR FPI 2 temperature [DN] "),
        },
        deletes=["MIR_CCDTEMP"]
    )

    result, issues = calibrate_header(fits_path=modified_file, output_dir=tmp_path, channel='NIR')

    warnings = [i for i in issues if i.level == 'warning']

    assert len(warnings) == 2
    assert "Failed to compute detector temperature for MIR_CCDTEMP" in warnings[0].message
    assert "Failed to compute FPI 2 temperature for MIR_FPI_TEMP2" in warnings[1].message
    assert "New fits file created:" in issues[-1].message


def test_missing_task_number(tmp_path: Path, repo_root: Path) -> None:

    level_0A_fits = repo_root / 'tests' / 'data' / 'fits' / 'lvl_0A' / 'NIR_000000_1111111111111_0A.fits'
    modified_file = make_modified_fits_hdr(
        source_path=level_0A_fits,
        tmp_path=tmp_path,
        output_name='NIR_000000_1111111111111_0A.fits',
        updates=None,
        deletes=["NIR_TASK_NUMBER"]
    )

    with pytest.raises(CalibrationError, match=r"NIR_TASK_NUMBER is not found from header."):
        result, issues = calibrate_header(fits_path=modified_file, output_dir=tmp_path, channel='NIR')

def test_invalid_task_number(tmp_path: Path, repo_root: Path) -> None:

    level_0A_fits = repo_root / 'tests' / 'data' / 'fits' / 'lvl_0A' / 'NIR_000000_1111111111111_0A.fits'
    modified_file = make_modified_fits_hdr(
        source_path=level_0A_fits,
        tmp_path=tmp_path,
        output_name='NIR_000000_1111111111111_0A.fits',
        updates={
            "NIR_TASK_NUMBER" : ("not a number", "Number of tasks")
        },
        deletes=None
    )

    with pytest.raises(CalibrationError, match=r"NIR_TASK_NUMBER is not convertable to integer."):
        result, issues = calibrate_header(fits_path=modified_file, output_dir=tmp_path, channel='NIR')

def test_large_task_number(tmp_path: Path, repo_root: Path) -> None:

    level_0A_fits = repo_root / 'tests' / 'data' / 'fits' / 'lvl_0A' / 'NIR_000000_1111111111111_0A.fits'
    modified_file = make_modified_fits_hdr(
        source_path=level_0A_fits,
        tmp_path=tmp_path,
        output_name='NIR_000000_1111111111111_0A.fits',
        updates={
            "NIR_TASK_NUMBER" : ("15", "Number of tasks")
        },
        deletes=None
    )
    result, issues = calibrate_header(fits_path=modified_file, output_dir=tmp_path, channel='NIR')

    warnings = [i for i in issues if i.level == "warning"]
    assert len(warnings) == 0

def test_wl_conversion_nir(tmp_path: Path, repo_root: Path) -> None:

    level_0A_fits = repo_root / 'tests' / 'data' / 'fits' / 'lvl_0A' / 'NIR_000000_1111111111111_0A.fits'
    modified_file = make_modified_fits_hdr(
        source_path=level_0A_fits,
        tmp_path=tmp_path,
        output_name='NIR_000000_1111111111111_0A.fits',
        updates={
            "NIR_TASK_000" : ("10000 10000 10000 100", "SP1 SP2 SP3 ExpDn")
        },
        deletes=None
    )

    result, issues = calibrate_header(fits_path=modified_file, output_dir=tmp_path, channel='NIR')

    with fits.open(result) as hdul:
        hdr = hdul[0].header
        nir_wl = hdr.get("NIR_WL_000")
    
    wl_cal = wavelength_conversion(float(10000), 'NIR')

    assert_temp_string_equal(wl_cal, nir_wl)

def test_wl_conversion_mir(tmp_path: Path, repo_root: Path) -> None:

    level_0A_fits = repo_root / 'tests' / 'data' / 'fits' / 'lvl_0A' / 'MIR_000000_1111111111111_0A.fits'
    modified_file = make_modified_fits_hdr(
        source_path=level_0A_fits,
        tmp_path=tmp_path,
        output_name='MIR_000000_1111111111111_0A.fits',
        updates={
            "MIR_TASK_000" : ("10000 10000 10000 100", "SP1 SP2 SP3 ExpDn")
        },
        deletes=None
    )

    result, issues = calibrate_header(fits_path=modified_file, output_dir=tmp_path, channel='MIR')

    with fits.open(result) as hdul:
        hdr = hdul[0].header
        mir_wl = hdr.get("MIR_WL_000")
    
    wl_cal = wavelength_conversion(float(10000), 'MIR')

    assert_temp_string_equal(wl_cal, mir_wl)

def test_exposure_conversion_nir(tmp_path: Path, repo_root: Path) -> None:

    level_0A_fits = repo_root / 'tests' / 'data' / 'fits' / 'lvl_0A' / 'NIR_000000_1111111111111_0A.fits'
    modified_file = make_modified_fits_hdr(
        source_path=level_0A_fits,
        tmp_path=tmp_path,
        output_name='NIR_000000_1111111111111_0A.fits',
        updates={
            "NIR_TASK_000" : ("10000 10000 10000 100", "SP1 SP2 SP3 ExpDn")
        },
        deletes=None
    )

    result, issues = calibrate_header(fits_path=modified_file, output_dir=tmp_path, channel='NIR')

    with fits.open(result) as hdul:
        hdr = hdul[0].header
        nir_exp = hdr.get("NIR_EXP_000")
    
    exp_cal = exposure_conversion(float(100), 'NIR')

    assert_temp_string_equal(exp_cal, nir_exp)

def test_exposure_conversion_mir(tmp_path: Path, repo_root: Path) -> None:

    level_0A_fits = repo_root / 'tests' / 'data' / 'fits' / 'lvl_0A' / 'MIR_000000_1111111111111_0A.fits'
    modified_file = make_modified_fits_hdr(
        source_path=level_0A_fits,
        tmp_path=tmp_path,
        output_name='MIR_000000_1111111111111_0A.fits',
        updates={
            "MIR_TASK_000" : ("10000 10000 10000 100", "SP1 SP2 SP3 ExpDn")
        },
        deletes=None
    )

    result, issues = calibrate_header(fits_path=modified_file, output_dir=tmp_path, channel='MIR')

    with fits.open(result) as hdul:
        hdr = hdul[0].header
        mir_exp = hdr.get("MIR_EXP_000")
    
    exp_cal = exposure_conversion(float(100), 'MIR')

    assert_temp_string_equal(exp_cal, mir_exp)

def test_ccd_tmp_comversion(tmp_path: Path, repo_root: Path) -> None:

    level_0A_fits = repo_root / 'tests' / 'data' / 'fits' / 'lvl_0A' / 'NIR_000000_1111111111111_0A.fits'
    modified_file = make_modified_fits_hdr(
        source_path=level_0A_fits,
        tmp_path=tmp_path,
        output_name='NIR_000000_1111111111111_0A.fits',
        updates={
            "NIR_CCDTEMP" : ("100", ""),
            "MIR_CCDTEMP" : ("100", ""),
        },
        deletes=None
    )

    result, issues = calibrate_header(fits_path=modified_file, output_dir=tmp_path, channel='NIR')

    with fits.open(result) as hdul:
        hdr = hdul[0].header
        nir_ccd = hdr.get("NIR_CCDTEMP")
        mir_ccd = hdr.get("MIR_CCDTEMP")
    
    nir_det_temp = det_temp_conversion(float(100), 'NIR')[1]
    mir_det_temp = det_temp_conversion(float(100), 'MIR')[1]

    assert_temp_string_equal(nir_ccd, nir_det_temp)
    assert_temp_string_equal(mir_ccd, mir_det_temp)

def test_fpi_tmp_conversion(tmp_path: Path, repo_root: Path) -> None:

    level_0A_fits = repo_root / 'tests' / 'data' / 'fits' / 'lvl_0A' / 'NIR_000000_1111111111111_0A.fits'
    modified_file = make_modified_fits_hdr(
        source_path=level_0A_fits,
        tmp_path=tmp_path,
        output_name='NIR_000000_1111111111111_0A.fits',
        updates={
            "NIR_FPI_TEMP1" : ("100", ""),
            "MIR_FPI_TEMP1" : ("100", ""),
            "NIR_FPI_TEMP2" : ("100", ""),
            "MIR_FPI_TEMP2" : ("100", ""),
        },
        deletes=None
    )

    result, issues = calibrate_header(fits_path=modified_file, output_dir=tmp_path, channel='NIR')

    with fits.open(result) as hdul:
        hdr = hdul[0].header
        nir_fpi1 = hdr.get("NIR_FPI_TEMP1")
        nir_fpi2= hdr.get("NIR_FPI_TEMP2")
    
    nir_fpi1_temp = fpi_temp_conversion(float(100), 1)[1]
    nir_fpi2_temp = fpi_temp_conversion(float(100), 2)[1]

    assert_temp_string_equal(nir_fpi1, nir_fpi1_temp)
    assert_temp_string_equal(nir_fpi2, nir_fpi2_temp)

def test_complete_level_1A(tmp_path: Path, repo_root: Path) -> None:

    level_0A_fits = repo_root / 'tests' / 'data' / 'fits' / 'lvl_0A' / 'NIR_000000_1111111111111_0A.fits'
    result, issues = calibrate_header(fits_path=level_0A_fits, output_dir=tmp_path, channel='NIR')

    warnings = [i for i in issues if i.level == 'warning']
    assert len(warnings) == 0
    with fits.open(result) as hdul:
        hdr = hdul[0].header
    
    assert hdr.get('PROCLEVL') == '1A'
    assert hdr.get('FILENAME') == 'NIR_000000_1111111111111_1A.fits'


"""
Level 1B tests
"""

def test_extract_cds_pixels(tmp_path: Path, repo_root: Path) -> None:

    level_1A_fits = repo_root / 'tests' / 'data' / 'fits' / 'lvl_1A' / 'NIR_000000_1111111111111_1A.fits'

    with fits.open(level_1A_fits) as hdul:
        result, issues = extract_cds_pixels(hdul)
        data = result[0].data

    assert all(i.level == 'info' for i in issues)
    assert data.shape == (3, 512, 640)
    assert len(result) == 2

def test_dark_subtraction(tmp_path: Path, repo_root: Path) -> None:

    level_1A_fits = repo_root / 'tests' / 'data' / 'fits' / 'lvl_1A' / 'NIR_000000_1111111111111_1A.fits'
    dark = repo_root / 'tests' / 'data' / 'calib' / 'DARK_05.fits'
    with fits.open(level_1A_fits) as hdul:
        hdul, issues = convert_to_float64(hdul)
        hdul, issues = extract_cds_pixels(hdul)
        result, issues = dark_subtraction(hdul, dark)
        result_data = result[0].data
    
    assert all(i.level == 'info' for i in issues)
    assert np.all(result_data == 0.5)

def test_dark_subtrsction_invalid(tmp_path: Path, repo_root: Path) -> None:

    level_1A_fits = repo_root / 'tests' / 'data' / 'fits' / 'lvl_1A' / 'NIR_000000_1111111111111_1A.fits'
    dark = repo_root / 'tests' / 'data' / 'calib' / 'DARK_05.fits'
    with fits.open(level_1A_fits) as hdul:
        hdul, issues = convert_to_float32(hdul)
        result, issues = dark_subtraction(hdul, dark)
        result_data = result[0].data
    
    warnings = [i for i in issues if i.level == 'warning']
    assert len(warnings) == 1
    assert "Dark frame subtraction failed for " in warnings[0].message
    assert np.all(result_data == 1.0)

def test_flat_field(repo_root: Path) -> None:
    level_1A_fits = repo_root / 'tests' / 'data' / 'fits' / 'lvl_1A' / 'NIR_000000_1111111111111_1A.fits'
    dark = repo_root / 'tests' / 'data' / 'calib' / 'DARK_05.fits'
    flat = repo_root / 'tests' / 'data' / 'calib' / 'FLAT_05.fits'
    with fits.open(level_1A_fits) as hdul:
        hdul, issues = convert_to_float64(hdul)
        hdul, issues = extract_cds_pixels(hdul)
        hdul, issues = dark_subtraction(hdul, dark)
        result, issues = flat_field_calibration(hdul, flat)
        result_data = result[0].data
    
    assert all(i.level == 'info' for i in issues)
    assert np.all(result_data == 1.0)

def test_flat_field_failed(repo_root: Path) -> None:
    level_1A_fits = repo_root / 'tests' / 'data' / 'fits' / 'lvl_1A' / 'NIR_000000_1111111111111_1A.fits'
    flat = repo_root / 'tests' / 'data' / 'calib' / 'FLAT_05.fits'
    with fits.open(level_1A_fits) as hdul:
        hdul, issues = convert_to_float64(hdul)
        result, issues = flat_field_calibration(hdul, flat)
        result_data = result[0].data
    
    warnings = [i for i in issues if i.level == 'warning']
    assert len(warnings) == 1
    assert "Flat field correction failed for" in warnings[0].message
    assert np.all(result_data == 1.0)

def test_invalid_flat_file(repo_root: Path) -> None:

    level_1A_fits = repo_root / 'tests' / 'data' / 'fits' / 'lvl_1A' / 'NIR_000000_1111111111111_1A.fits'
    flat = repo_root / 'tests' / 'data' / 'calib' / 'non_existing_flat.fits'

    with fits.open(level_1A_fits) as hdul:
        result, issues = flat_field_calibration(hdul, flat)
        errors = [i for i in issues if i.level == 'error']
        assert len(errors) == 1
        assert "Caught Exception while reading flat field for channel" in errors[0].message

def test_bad_pixel_correction(tmp_path: Path, repo_root: Path) -> None:

    level_1A_fits = repo_root / 'tests' / 'data' / 'fits' / 'lvl_1A' / 'NIR_000000_1111111111111_1A.fits'
    dark = repo_root / 'tests' / 'data' / 'calib' / 'DARK_05.fits'
    flat = repo_root / 'tests' / 'data' / 'calib' / 'FLAT_05.fits'
    badpixels = repo_root / 'tests' / 'data' / 'calib' / 'BADPIXELS.txt'

    modified_file = make_modified_fits_data(
        source_path=level_1A_fits,
        tmp_path=tmp_path,
        output_name='NIR_000000_1111111111111_1A.fits',
        pixel_updates={
            (105, 104) : 1000, # Single pixel
            (405, 404) : 1000, # Cluster from (400, 400) to (403, 402)
            (406, 404) : 1000,
            (407, 404) : 1000,
            (408, 404) : 1000,
            (405, 405) : 1000,
            (406, 405) : 1000,
            (407, 405) : 1000,
            (408, 405) : 1000,
            (405, 406) : 1000,
            (406, 406) : 1000,
            (407, 406) : 1000,
            (408, 406) : 1000,
            
        },
        row_updates={
            255 : 1000 # Row at height 250 
        },
        col_updates={
            24 : 1000 # Column at width 20
        }
    )
    with fits.open(modified_file) as hdul:
        hdul, issues = convert_to_float64(hdul)
        hdul, issues = extract_cds_pixels(hdul)
        hdul, issues = dark_subtraction(hdul, dark)
        hdul, issues = flat_field_calibration(hdul, flat)
        result, issues = replace_bad_pixels(hdul, badpixels)

        result_data = result[0].data

        mask = result_data != 1.0
        indices = np.where(mask)
        print(indices)
        assert all(i.level == 'info' for i in issues)

        assert np.all(result_data == 1.0)

def test_invalid_bad_pixel_file(repo_root: Path) -> None:

    level_1A_fits = repo_root / 'tests' / 'data' / 'fits' / 'lvl_1A' / 'NIR_000000_1111111111111_1A.fits'
    badpixels = repo_root / 'tests' / 'data' / 'calib' / 'non_existing_BADPIXELS.txt'

    with fits.open(level_1A_fits) as hdul:
        result, issues = replace_bad_pixels(hdul, badpixels)
        warnings = [i for i in issues if i.level == 'warning']
        assert len(warnings) == 1
        assert "Parsing bad pixel list failed;" in warnings[0].message

def test_radiometric_calibration_nir(tmp_path: Path, repo_root: Path) -> None:

    level_1A_fits = repo_root / 'tests' / 'data' / 'fits' / 'lvl_1A' / 'NIR_000000_1111111111111_1A.fits'
    radiance_file = repo_root / 'tests' / 'data' / 'calib' / 'RADIANCE.txt'
    modified_file = make_modified_fits_hdr(
        source_path=level_1A_fits,
        tmp_path=tmp_path,
        output_name='NIR_000000_1111111111111_1A.fits',
        updates={
            "NIR_EXP_000": ("0.001", "NIR TASK 000 exposure [s] "),
            "NIR_EXP_001": ("0.001", "NIR TASK 001 exposure [s] "),
            "NIR_EXP_002": ("0.001", "NIR TASK 002 exposure [s] "),
            "NIR_WL_000" : ("900", "NIR TASK 000 wavelength [nm]"),
            "NIR_WL_001" : ("950", "NIR TASK 001 wavelength [nm]"),
            "NIR_WL_002" : ("1025", "NIR TASK 002 wavelength [nm]"),
        },
        deletes=None
    )

    with fits.open(modified_file) as hdul:
        hdul, issues = convert_to_float64(hdul)
        hdul, issues = extract_cds_pixels(hdul=hdul)
        hdul, issues = radiometric_calibration(hdul, radiance_file)
        data = hdul[0].data

        assert len(issues) == 1
        assert issues[0].level == 'info'

        assert np.all(data[0] == 1000)
        assert np.all(data[1] == 1000)
        assert np.all(data[2] == 250)

        
def test_radiance_calibration_mir(tmp_path: Path, repo_root: Path) -> None:

    level_1A_fits = repo_root / 'tests' / 'data' / 'fits' / 'lvl_1A' / 'MIR_000000_1111111111111_1A.fits'
    radiance_file = repo_root / 'tests' / 'data' / 'calib' / 'RADIANCE.txt'
    modified_file = make_modified_fits_hdr(
        source_path=level_1A_fits,
        tmp_path=tmp_path,
        output_name='MIR_000000_1111111111111_1A.fits',
        updates={
            "MIR_EXP_000": ("0.001", "MIR TASK 000 exposure [s] "),
            "MIR_EXP_001": ("0.001", "MIR TASK 001 exposure [s] "),
            "MIR_EXP_002": ("0.001", "MIR TASK 002 exposure [s] "),
            "MIR_WL_000" : ("2000", "MIR TASK 000 wavelength [nm]"),
            "MIR_WL_001" : ("2020", "MIR TASK 001 wavelength [nm]"),
            "MIR_WL_002" : ("2050", "MIR TASK 002 wavelength [nm]"),
        },
        deletes=None
    )

    with fits.open(modified_file) as hdul:
        hdul, issues = convert_to_float64(hdul)
        hdul, issues = radiometric_calibration(hdul, radiance_file)
        data = hdul[0].data

        assert len(issues) == 1
        assert issues[0].level == 'info'

        assert np.all(data[0] == 1000)
        assert np.all(data[1] == 1000)
        assert np.all(data[2] == 250)

def test_radiance_mismatch_frames_wl_exp_nir(tmp_path: Path, repo_root: Path) -> None:

    level_1A_fits = repo_root / 'tests' / 'data' / 'fits' / 'lvl_1A' / 'NIR_000000_1111111111111_1A.fits'
    radiance_file = repo_root / 'tests' / 'data' / 'calib' / 'RADIANCE.txt'
    modified_file = make_modified_fits_hdr(
        source_path=level_1A_fits,
        tmp_path=tmp_path,
        output_name='NIR_000000_1111111111111_1A.fits',
        updates={
            "NIR_FRAMES" : ("000,002,003", "NIR frames"),
            "NIR_EXP_001": ("0.001", "NIR TASK 000 exposure [s] "),
            "NIR_EXP_002": ("0.001", "NIR TASK 001 exposure [s] "),
            "NIR_EXP_003": ("0.001", "NIR TASK 002 exposure [s] "),
            "NIR_WL_001" : ("900", "NIR TASK 000 wavelength [nm]"),
            "NIR_WL_002" : ("950", "NIR TASK 001 wavelength [nm]"),
            "NIR_WL_003" : ("1025", "NIR TASK 002 wavelength [nm]"),
        },
        deletes=['NIR_EXP_000', 'NIR_WL_000']
    )
    with fits.open(modified_file) as hdul:
        hdul, issues = convert_to_float64(hdul)
        hdul, issues = radiometric_calibration(hdul, radiance_file)
        data = hdul[0].data

        warnings = [i for i in issues if i.level == 'warning']

        assert len(warnings) == 1
        assert 'No wavelength data found for NIR frame 000' in warnings[0].message
        assert np.all(data[0] == 1)
        assert np.all(data[1] == 1000)


def test_radiance_mismatch_frames_wl_exp_mir(tmp_path: Path, repo_root: Path) -> None:

    level_1A_fits = repo_root / 'tests' / 'data' / 'fits' / 'lvl_1A' / 'MIR_000000_1111111111111_1A.fits'
    radiance_file = repo_root / 'tests' / 'data' / 'calib' / 'RADIANCE.txt'
    modified_file = make_modified_fits_hdr(
        source_path=level_1A_fits,
        tmp_path=tmp_path,
        output_name='MIR_000000_1111111111111_1A.fits',
        updates={
            "MIR_FRAMES" : ("000,002,003", "MIR frames"),
            "MIR_EXP_001": ("0.001", "MIR TASK 000 exposure [s] "),
            "MIR_EXP_002": ("0.001", "MIR TASK 001 exposure [s] "),
            "MIR_EXP_003": ("0.001", "MIR TASK 002 exposure [s] "),
            "MIR_WL_001" : ("2000", "MIR TASK 000 wavelength [nm]"),
            "MIR_WL_002" : ("2020", "MIR TASK 001 wavelength [nm]"),
            "MIR_WL_003" : ("2050", "MIR TASK 002 wavelength [nm]"),
        },
        deletes=['MIR_EXP_000', 'MIR_WL_000']
    )
    with fits.open(modified_file) as hdul:
        hdul, issues = convert_to_float64(hdul)
        hdul, issues = radiometric_calibration(hdul, radiance_file)
        data = hdul[0].data

        warnings = [i for i in issues if i.level == 'warning']

        assert len(warnings) == 1
        assert 'No wavelength data found for MIR frame 000' in warnings[0].message
        assert np.all(data[0] == 1)
        assert np.all(data[1] == 1000)

def test_convert_dtypes(tmp_path: Path, repo_root: Path) -> None:

    level_1A_fits = repo_root / 'tests' / 'data' / 'fits' / 'lvl_1A' / 'MIR_000000_1111111111111_1A.fits'

    with fits.open(level_1A_fits) as hdul:
        float_64, issue = convert_to_float64(hdul)
        assert issue.level == 'info'
        assert issue.message == 'HDU data converted to float64'
        assert float_64[0].header['BITPIX'] == -64
        assert float_64[0].data.dtype == np.float64

        float_32, issue = convert_to_float32(float_64)
        assert issue.level == 'info'
        assert issue.message == 'HDU data converted to float32'
        assert float_32[0].header['BITPIX'] == -32
        assert float_32[0].data.dtype == np.float32

def test_level_1b_nir(tmp_path: Path, repo_root: Path) -> None:

    level_1A_fits = repo_root / 'tests' / 'data' / 'fits' / 'lvl_1A' / 'NIR_000000_1111111111111_1A.fits'
    output_dir = tmp_path
    modified_file = make_modified_fits_hdr(
        source_path=level_1A_fits,
        tmp_path=tmp_path,
        output_name='NIR_000000_1111111111111_1A.fits',
        updates={
            "NIR_TASK_NUMBER" : ("3", "Number of tasks"),
            "NIR_FRAMES" : ("000,001,002", ""),
            "NIR_EXP_000": ("0.001", "NIR TASK 000 exposure [s] "),
            "NIR_EXP_001": ("0.001", "NIR TASK 001 exposure [s] "),
            "NIR_EXP_002": ("0.001", "NIR TASK 002 exposure [s] "),
            "NIR_WL_000" : ("900", "NIR TASK 000 wavelength [nm]"),
            "NIR_WL_001" : ("950", "NIR TASK 001 wavelength [nm]"),
            "NIR_WL_002" : ("1025", "NIR TASK 002 wavelength [nm]"),
        },
        deletes=None
    )
    calib_dir = repo_root / 'tests' / 'data' / 'calib'

    calib_cfg = CalibConfig(
        calibration_dir=calib_dir,
        dark='DARK_05.fits',
        flat='FLAT_05.fits',
        badpixels='BADPIXELS.txt',
        nir_radiance='RADIANCE.txt',
        mir_radiance=None,
        solar_ssi=None
    )
    result, issues = run_level_1b(modified_file, output_dir, calib_cfg, 'NIR', extra=True)

    warnings_errors = [i for i in issues if i.level=='warning'  or i.level=='error']
    assert len(warnings_errors) == 0

    with fits.open(result) as hdul:
        data = hdul[0].data
        assert hdul[0].header['BITPIX'] == -32
        assert data.dtype == '>f4'
        assert np.all(data[0] == 1000)
        assert np.all(data[1] == 1000)
        assert np.all(data[2] == 250)
        assert hdul[0].header['PROCLEVL'] == '1B'

def test_level_1b_mir(tmp_path: Path, repo_root: Path) -> None:

    level_1A_fits = repo_root / 'tests' / 'data' / 'fits' / 'lvl_1A' / 'MIR_000000_1111111111111_1A.fits'
    output_dir = tmp_path
    modified_file = make_modified_fits_hdr(
        source_path=level_1A_fits,
        tmp_path=tmp_path,
        output_name='MIR_000000_1111111111111_1A.fits',
        updates={
            "MIR_EXP_000": ("0.001", "MIR TASK 000 exposure [s] "),
            "MIR_EXP_001": ("0.001", "MIR TASK 001 exposure [s] "),
            "MIR_EXP_002": ("0.001", "MIR TASK 002 exposure [s] "),
            "MIR_WL_000" : ("2000", "MIR TASK 000 wavelength [nm]"),
            "MIR_WL_001" : ("2020", "MIR TASK 001 wavelength [nm]"),
            "MIR_WL_002" : ("2050", "MIR TASK 002 wavelength [nm]"),
        },
        deletes=None
    )
    calib_dir = repo_root / 'tests' / 'data' / 'calib'

    calib_cfg = CalibConfig(
        calibration_dir=calib_dir,
        dark='DARK_05.fits',
        flat='FLAT_05.fits',
        badpixels='BADPIXELS.txt',
        nir_radiance='RADIANCE.txt',
        mir_radiance='RADIANCE.txt',
        solar_ssi=None
    )
    result, issues = run_level_1b(modified_file, output_dir, calib_cfg, 'MIR', extra=True)

    warnings_errors = [i for i in issues if i.level=='warning'  or i.level=='error']
    assert len(warnings_errors) == 0

    with fits.open(result) as hdul:
        data = hdul[0].data
        assert hdul[0].header['BITPIX'] == -32
        assert data.dtype == '>f4'
        assert data[0] == 1000
        assert data[1] == 1000
        assert data[2] == 250
        assert hdul[0].header['PROCLEVL'] == '1B'


"""
Level 1C
"""

def test_reflectance(tmp_path: Path, repo_root: Path) -> None:

    level_1B_fits = repo_root / 'tests' / 'data' / 'fits' / 'lvl_1B' / 'NIR_000000_1111111111111_1B.fits'
    output_dir = tmp_path
    modified_file = make_modified_fits_hdr(
        source_path=level_1B_fits,
        tmp_path=tmp_path,
        output_name='NIR_000000_1111111111111_1B.fits',
        updates={
            "NIR_TASK_NUMBER" : ("3", "Number of tasks"),
            "NIR_FRAMES" : ("000,001,002", ""),
            "NIR_EXP_000": ("0.001", "NIR TASK 000 exposure [s] "),
            "NIR_EXP_001": ("0.001", "NIR TASK 001 exposure [s] "),
            "NIR_EXP_002": ("0.001", "NIR TASK 002 exposure [s] "),
            "NIR_WL_000" : ("900", "NIR TASK 000 wavelength [nm]"),
            "NIR_WL_001" : ("950", "NIR TASK 001 wavelength [nm]"),
            "NIR_WL_002" : ("1025", "NIR TASK 002 wavelength [nm]"),
        },
        deletes=None
    )
    solar_ssi = repo_root / 'calibration' / 'SOLAR' / 'ssi_yearly_avg_e2024_c20250221.csv'

    result, issues = reflectance_calibration(modified_file, output_dir, solar_ssi)
    
    warnings = [i for i in issues if i.level == 'warning']
    assert len(warnings) == 1
    assert "Using default 1AU" in warnings[0].message

    with fits.open(result) as hdul:
        data = hdul[0].data
        hdr = hdul[0].header

        assert hdr['PROCLEVL'] == '1C'
        assert np.all(data == data[:, 0:1, 0:1]), "Not all frames are constant"
        frame_values = data[:, 0, 0]
        assert np.all(frame_values[:-1] < frame_values[1:]), "Frame values are not strictly increasing"


def test_reflectance_missing_frames(tmp_path: Path, repo_root: Path) -> None:
    level_1B_fits = repo_root / 'tests' / 'data' / 'fits' / 'lvl_1B' / 'NIR_000000_1111111111111_1B.fits'
    output_dir = tmp_path
    modified_file = make_modified_fits_hdr(
        source_path=level_1B_fits,
        tmp_path=tmp_path,
        output_name='NIR_000000_1111111111111_1B.fits',
        updates={
            "SOLAR_D"    : ("1", "Solar distance [AU]"),
            "NIR_TASK_NUMBER" : ("3", "Number of tasks"),
            "NIR_FRAMES" : ("000,002,003", ""),
            "NIR_EXP_000": ("0.001", "NIR TASK 000 exposure [s] "),
            "NIR_EXP_001": ("0.001", "NIR TASK 001 exposure [s] "),
            "NIR_EXP_002": ("0.001", "NIR TASK 002 exposure [s] "),
            "NIR_WL_000" : ("900", "NIR TASK 000 wavelength [nm]"),
            "NIR_WL_001" : ("950", "NIR TASK 001 wavelength [nm]"),
            "NIR_WL_002" : ("1025", "NIR TASK 002 wavelength [nm]"),
        },
        deletes=["NIR_WL_003", "NIR_EXP_003"]
    )
    solar_ssi = repo_root / 'calibration' / 'SOLAR' / 'ssi_yearly_avg_e2024_c20250221.csv'

    result, issues = reflectance_calibration(modified_file, output_dir, solar_ssi)

    warning = [i for i in issues if i.level=='warning']

    assert len(warning) == 1

def test_reflectance_missing_wl(tmp_path: Path, repo_root: Path) -> None:
    level_1B_fits = repo_root / 'tests' / 'data' / 'fits' / 'lvl_1B' / 'NIR_000000_1111111111111_1B.fits'
    output_dir = tmp_path
    modified_file = make_modified_fits_hdr(
        source_path=level_1B_fits,
        tmp_path=tmp_path,
        output_name='NIR_000000_1111111111111_1B.fits',
        updates={
            "SOLAR_D"    : ("1", "Solar distance [AU]"),
            "NIR_TASK_NUMBER" : ("3", "Number of tasks"),
            "NIR_FRAMES" : ("000,001,002", ""),
            "NIR_EXP_000": ("0.001", "NIR TASK 000 exposure [s] "),
            "NIR_EXP_001": ("0.001", "NIR TASK 001 exposure [s] "),
            "NIR_EXP_002": ("0.001", "NIR TASK 002 exposure [s] "),
            "NIR_WL_000" : ("900", "NIR TASK 000 wavelength [nm]"),
            "NIR_WL_002" : ("1025", "NIR TASK 002 wavelength [nm]"),
        },
        deletes=["NIR_WL_001"]
    )
    solar_ssi = repo_root / 'calibration' / 'SOLAR' / 'ssi_yearly_avg_e2024_c20250221.csv'

    result, issues = reflectance_calibration(modified_file, output_dir, solar_ssi)

    warning = [i for i in issues if i.level=='warning']

    assert len(warning) == 1

def test_reflectance_mismatch_frames_wl_exp(tmp_path: Path, repo_root: Path) -> None:

    level_1B_fits = repo_root / 'tests' / 'data' / 'fits' / 'lvl_1B' / 'NIR_000000_1111111111111_1B.fits'
    radiance_file = repo_root / 'tests' / 'data' / 'calib' / 'RADIANCE.txt'
    modified_file = make_modified_fits_hdr(
        source_path=level_1B_fits,
        tmp_path=tmp_path,
        output_name='NIR_000000_1111111111111_1B.fits',
        updates={
            "SOLAR_D"    : ("1", "Solar distance [AU]"),
            "NIR_FRAMES" : ("000,002,003", "NIR frames"),
            "NIR_EXP_001": ("0.001", "NIR TASK 000 exposure [s] "),
            "NIR_EXP_002": ("0.001", "NIR TASK 001 exposure [s] "),
            "NIR_EXP_003": ("0.001", "NIR TASK 002 exposure [s] "),
            "NIR_WL_001" : ("900", "NIR TASK 000 wavelength [nm]"),
            "NIR_WL_002" : ("950", "NIR TASK 001 wavelength [nm]"),
            "NIR_WL_003" : ("1025", "NIR TASK 002 wavelength [nm]"),
        },
        deletes=['NIR_EXP_000', 'NIR_WL_000']
    )
    solar_ssi = repo_root / 'calibration' / 'SOLAR' / 'ssi_yearly_avg_e2024_c20250221.csv'
    result, issues = reflectance_calibration(modified_file, tmp_path, solar_ssi)

    warnings = [i for i in issues if i.level == 'warning']

    assert len(warnings) == 1
    assert 'No wavelength data found for NIR frame 000' in warnings[0].message
    with fits.open(result) as hdul:
        data = hdul[0].data
        assert np.all(data[0] == 1)


def pipeline_level_1_works(tmp_path: Path, repo_root: Path) -> None:
    level_0A_fits = repo_root / 'tests' / 'data' / 'fits' / 'lvl_0A' / 'NIR_000000_1111111111111_0A.fits'
    modified_file = make_modified_fits_hdr(
        source_path=level_0A_fits,
        tmp_path=tmp_path,
        output_name='NIR_000000_1111111111111_0A.fits',
        updates={
            "NIR_TASK_NUMBER" : ("3", "Number of tasks"),
            "NIR_FRAMES" : ("000,001,002", ""),
        },
        deletes=[]
    )

    config = repo_root / 'tests' / 'configs' / 'test.yaml'

    raw = read_yaml(config)
    modified = copy.deepcopy(raw)
    modified_config = tmp_path / "modified.yaml"
    modified["run"]["input_dir"] = str(tmp_path) # Correct path for input dir
    modified["run"]["output_dir"] = str(tmp_path)
    modified["calibration"]["calibration_dir"] = str(repo_root / 'tests' / 'data' / 'calib')
    modified["pipeline"]["levels"] = ["1"]
    modified["calibration"]["solar_d"] = ["1"]
    write_yaml(modified_config, modified)

    cfg = load_config(modified_config)

    result, issues = run_level_1(cfg, 'NIR')

    warnings_errors = [i for i in issues if i.level == 'warning' or i.level == 'error']

    assert len(warnings_errors) == 0
    