import argparse

from nir_mir_pipeline.pipeline.run import test_run

def main() -> None:
    parser = argparse.ArgumentParser(prog="mirmis")
    sub = parser.add_subparsers(dest="cdm", required=True)

    sub.add_parser("test", help="Run a minimal pipeline test")

    args = parser.parse_args()

    if args.cdm == "test":
        test_run()

if __name__ == "__main__":
    main()