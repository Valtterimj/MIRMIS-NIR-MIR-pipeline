
import logging
from pathlib import Path

from nirmir_pipeline.pipeline.config import load_config
from nirmir_pipeline.pipeline.utils.errors import PipelineError

from nirmir_pipeline.pipeline.levels.level_0.run import run_level_0
from nirmir_pipeline.pipeline.utils.validate import _validate_output_dir

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
    
    missphas = cfg.data.missphas
    output_dir = cfg.run.output_dir
    try:
        output_path = _validate_output_dir(output_dir=output_dir, missphas=missphas)
        cfg.run.output_dir = output_path
        logger.info(f"Output directory resolved: {output_path}")
    except PipelineError as e:
        logger.error(f"[%s] Resolving output directory failed.", type(e).__name__)
        raise
    
    logger.info(f"Running pipeline levels: {levels} for channels: {channels}")
    for channel in cfg.pipeline.channels:
        logger.info(f"Channel: {channel}")
        if "0" in levels:
            run_level_0(cfg=cfg, channel=channel)

