
from pathlib import Path
from astropy.io import fits 

from nirmir_pipeline.pipeline.utils.utilities import convert_to_float64, convert_to_float32
from nirmir_pipeline.pipeline.utils.classes import Issue
from nirmir_pipeline.pipeline.levels.level_1.extract_cds import extract_cds_pixels
from nirmir_pipeline.pipeline.levels.level_1.dark_background import dark_subtraction
from nirmir_pipeline.pipeline.levels.level_1.flat_field import flat_field_calibration
from nirmir_pipeline.pipeline.levels.level_1.bad_pixels import replace_bad_pixels
from nirmir_pipeline.pipeline.levels.level_1.radiometric import radiometric_calibration

def run_level_1b(fits_file: Path, output_dir: Path, calibration_dir: Path, channel: str) -> tuple[Path, list[Issue]]:

    all_issues: list[Issue] = []

    with fits.open(fits_file, memmap=False) as hdul:


        # Convert the data to float64 for calibration
        hdul, issue = convert_to_float64(hdul)
        all_issues.append(issue)

        # Extract diagnostic pixels from NIR. Convert the values to float64
        hdul, issues = extract_cds_pixels(hdul)
        all_issues.extend(issues)

        # Subtrack the dark frame from each image
        dark = calibration_dir / 'DARKS' / f'{channel}_DARK.fits'
        hdul, issues = dark_subtraction(hdul, dark)
        all_issues.extend(issues)

        # Apply flatfield correction
        flat = calibration_dir / 'FLATS' / f'{channel}_FLAT.fits'
        hdul, issues = flat_field_calibration(hdul, flat)
        all_issues.extend(issues)

        # Replace bad pixels with neigbours
        badpixels = calibration_dir / 'BADPIXELS' / f'{channel}_BADPIXELS.txt'
        hdul, issues = replace_bad_pixels(hdul, bp_file=badpixels)
        all_issues.extend(issues)

        # Apply radiometric calibration
        radiance_coefs = calibration_dir / 'RADIANCE' / f'{channel}_RADIANCE.txt'
        hdul, issues = radiometric_calibration(hdul, radiance_file=radiance_coefs)
        all_issues.extend(issues)

        hdul, issue = convert_to_float32(hdul)
        all_issues.append(issue)

        stem = fits_file.stem
        suffix = fits_file.suffix
        new_calibration_level = '1B'
        file_name = stem[:25] + new_calibration_level + suffix
        primary_header = hdul[0].header
        primary_header['FILENAME'] = file_name
        primary_header['PROCLEVL'] = new_calibration_level

    # create the new fits
    fits_file = Path(output_dir) / file_name
    hdul.writeto(fits_file, overwrite=True)

    return fits_file, all_issues