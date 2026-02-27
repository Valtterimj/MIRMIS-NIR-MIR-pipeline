import argparse

from pathlib import Path

from nirmir_pipeline.utils.logging_config import setup_logging

from nirmir_pipeline.pipeline.run import run_pipeline, view_fits
from nirmir_pipeline.pipeline.utils.errors import PipelineError


def main() -> None:
    parser = argparse.ArgumentParser(prog="mirmis")
    sub = parser.add_subparsers(dest="cdm", required=True)

    run = sub.add_parser("run", help="Run the pipeline")
    run.add_argument(
        "--config",
        default=None,
        help="Path to YAML config. If omitted, searches default options"
    )

    view = sub.add_parser("view", help="Visualise produced FITS files")
    view.add_argument(
        "--path",
        required=True,
        help="Path to a FITS file or a dirctory containing FITS files"
    )
    view.add_argument(
        "--level",
        default=None,
        help="If --path is a directory, which level product to view (e.g. '0A', '1A', '1B') "
    )

    args = parser.parse_args()

    setup_logging("INFO")
    
    try:
        if args.cdm == "run":
            config_path = Path(args.config).expanduser() if args.config else None
            run_pipeline(config_path)
        elif args.cdm == "view":
            view_fits(
                path=Path(args.path).expanduser(),
                level=args.level,
            )
    except PipelineError as e:
        raise SystemExit(str(e)) from e

if __name__ == "__main__":
    main()