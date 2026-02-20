import logging

from pathlib import Path

from nirmir_pipeline.pipeline.levels.level_0.build_fits import build_fits

from nirmir_pipeline.pipeline.utils.classes import Config, InputLayout
from nirmir_pipeline.pipeline.utils.validate import _validate_level_0_input_dir
from nirmir_pipeline.pipeline.utils.errors import ValidationError

logger = logging.getLogger(__name__)

def run_level_0(cfg: Config, channel: str) -> Path:

    input_dir = cfg.run.input_dir
    try:
        input_layout = _validate_level_0_input_dir(input_dir=input_dir)
        fits_path = build_fits(input=input_layout)

    except ValidationError:
        logger.error(f"Error in validating the input directory.")
        raise
    
