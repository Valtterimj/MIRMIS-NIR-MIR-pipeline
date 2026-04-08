
import logging
from pathlib import Path

from nirmir_pipeline.pipeline.config import load_config
from nirmir_pipeline.pipeline.utils.classes import Issue
from nirmir_pipeline.pipeline.utils.errors import PipelineError, ValidationError, ConfigError, format_exeption_chain

from nirmir_pipeline.pipeline.levels.level_0.run import run_level_0
from nirmir_pipeline.pipeline.utils.validate import _validate_output_dir
from nirmir_pipeline.pipeline.visualise import visualise_fits
from nirmir_pipeline.pipeline.utils.utilities import fits_in_dir, log_issue
from nirmir_pipeline.pipeline.levels.level_1.run import run_level_1
logger = logging.getLogger(__name__)

def run_pipeline(config_path: Path) -> tuple[Path, list[Issue], list[Issue]]:
    """
    Main funciton to run data calibration and processing pipeline for MIRMIS instrument's NIR and MIR channels. 
    Returns the output directory containing the processed FITS files and lists of potential warning / error messages.
    """
    warnings: list[Issue] = []
    errors: list[Issue] = []

    logger.info("Staring pipeline function: run_pipeline")

    try:
        cfg = load_config(config_path=config_path)
    except ConfigError as e: 
        logger.error(f"[%s] Loading config failed.", type(e).__name__)
        raise

    cfg_path = cfg.config_path
    levels = cfg.pipeline.levels
    channels = cfg.pipeline.channels
    logger.info(f"Configations loaded from {cfg_path}")
    
    missphas = cfg.data.missphas
    output_dir = cfg.run.output_dir
    try:
        output_path = _validate_output_dir(output_dir=output_dir, missphas=missphas)
        cfg.run.output_dir = output_path
        logger.info(f"Output directory resolved: {output_path}")
    except ValidationError as e:
        logger.error(f"[%s] Resolving output directory failed.", type(e).__name__)
        raise
    
    logger.info(f"Running pipeline levels: {levels} for channels: {channels}")

    if "0" in levels:
        for channel in cfg.pipeline.channels:
            try:
                logger.info(f"Running level 0 for channel {channel}.")
                fits_file, all_issues = run_level_0(cfg=cfg, channel=channel)
                for issue in all_issues:
                    log_issue(issue)
                    if issue.level == 'warning':
                        warnings.append(issue)
                    if issue.level == 'error':
                        errors.append(issue)
                logger.info(f"Level 0 run finished for channel {channel}.")
            except PipelineError as e:
                logger.error(
                    f"Error running pipeline for channel %s. Continuing. %s",
                    channel, 
                    format_exeption_chain(e)
                    )
                continue
        cfg.run.input_dir = cfg.run.output_dir


    if any(x in levels for x in ["1", "1A", "1A-extra", "1B", "1C"]):
        for channel in cfg.pipeline.channels:
            try:
                logger.info(f"Running level 1 for channel {channel}.")
                fits_file, all_issues = run_level_1(cfg=cfg, channel=channel)
                for issue in all_issues:
                    log_issue(issue)
                    if issue.level == 'warning':
                        warnings.append(issue)
                    if issue.level == 'error':
                        errors.append(issue)
                logger.info(f"Level 1 finished for channel {channel}.")  
            except PipelineError as e:
                logger.error(
                    f"Error running pipeline for channel %s. Continuing. %s",
                    channel, 
                    format_exeption_chain(e)
                    )
                continue
    
    return cfg.run.output_dir, warnings, errors


def view_fits(path: Path, level: str | None = None) -> None:

    if path.is_file():
        visualise_fits(path)
        return
    
    if not path.is_dir():
        raise PipelineError(f"Path does not exist: {path}")

    allowed_levels = {"0A", "1A", "1B", "1C", "2A", "2B"}

    if level is None:
        raise PipelineError(f"when --path is a directory, you must provide --level "
                            f"Allowed values: {', '.join(sorted(allowed_levels))}")

    if level not in allowed_levels:
        raise PipelineError( f"Invalid --level '{level}'. "
                            f"Allowed values: {', '.join(sorted(allowed_levels))}")
    
    fits_files = fits_in_dir(folder=path)

    if not fits_files:
        raise PipelineError(f"No FITS files found in: {path}")
    
    suffix = f"_{level}.fits"
    selected = [fp for fp in fits_files if fp.name.endswith(suffix)]

    if not selected: 
        raise PipelineError(f"No FITS files matched level {level} in {path}.")
    
    for f in selected:
        visualise_fits(file=f)