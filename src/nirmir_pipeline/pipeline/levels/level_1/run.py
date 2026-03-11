from pathlib import Path

from nirmir_pipeline.pipeline.utils.classes import Issue
from nirmir_pipeline.pipeline.levels.level_1.calibrate_header import calibrate_header
from nirmir_pipeline.pipeline.levels.level_1.level_1b import run_level_1b

def run_level_1(fits: Path, output_dir: Path, calibration_dir: Path, channel: str) -> tuple[Path, list[Issue]]:

    all_issues: list[Issue] = []
    
    fits_path, issues = calibrate_header(fits_path=fits, output_dir=output_dir, channel=channel)
    all_issues.extend(issues)
    all_issues.append(
                    Issue(
                        level="info",
                        message=(f"Level 1A completed."),
                        source=__name__,
                    )
                )
    
    fits_path, issues = run_level_1b(fits_file=fits_path, output_dir=output_dir, calibration_dir=calibration_dir, channel=channel)
    all_issues.extend(issues)
    all_issues.append(
                    Issue(
                        level="info",
                        message=(f"Level 1b completed."),
                        source=__name__,
                    )
                )

    return fits_path, all_issues 