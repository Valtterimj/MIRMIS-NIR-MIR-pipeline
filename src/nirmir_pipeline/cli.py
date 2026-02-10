import argparse

from pathlib import Path

from nirmir_pipeline.pipeline.run import test_run

DEFAULT_CANDIDATES = [
    Path("configs/pipeline.yaml"),
    Path("configs/pipeline.example.yaml"),
]

def resolve_config_path(cli_value: str | None) -> Path:
    if cli_value:
        p = Path(cli_value)
        if not p.exists():
            raise SystemExit(f"Config not found: {p}")
        return p
    
    for p in DEFAULT_CANDIDATES:
        if p.exists():
            return p
    
    raise SystemExit("No config found")


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

    if args.cdm == "test":
        config_path = resolve_config_path(args.config)
        test_run(config_path)

if __name__ == "__main__":
    main()