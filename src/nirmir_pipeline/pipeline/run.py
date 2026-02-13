
import logging
from pathlib import Path

from nirmir_pipeline.pipeline.config import load_config
from nirmir_pipeline.pipeline.utils.error import PipelineError

logger = logging.getLogger(__name__)

def test_run(config_path: Path) -> None:
    logger.info("Staring pipeline function: test_run")

    try:
        cfg = load_config(config_path=config_path)
    except PipelineError as e: 
        logger.error(f"[%s] Loading config failed.", type(e).__name__)
        raise
    cfg_path = cfg.config_path
    levels = cfg.pipeline.levels
    channels = cfg.pipeline.channels
    logger.info(f"Configations loaded from {cfg_path}")
    logger.info(f"Running pipeline levels: {levels} for channels: {channels}")

