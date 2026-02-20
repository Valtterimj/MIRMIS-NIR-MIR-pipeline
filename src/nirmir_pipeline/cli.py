import argparse

from pathlib import Path

from nirmir_pipeline.utils.logging_config import setup_logging

from nirmir_pipeline.pipeline.run import test_run
from nirmir_pipeline.pipeline.utils.errors import PipelineError


def main() -> None:
    parser = argparse.ArgumentParser(prog="mirmis")
    sub = parser.add_subparsers(dest="cdm", required=True)

    test = sub.add_parser("test", help="Run a minimal pipeline test")
    test.add_argument(
        "--config",
        default=None,
        help="Path to YAML config. If omitted, searches default options"
    )

    args = parser.parse_args()

    setup_logging("INFO")
    
    try:
        if args.cdm == "test":
            config_path = Path(args.config).expanduser() if args.config else None
            test_run(config_path)
    except PipelineError as e:
        raise SystemExit(str(e)) from e

if __name__ == "__main__":
    main()