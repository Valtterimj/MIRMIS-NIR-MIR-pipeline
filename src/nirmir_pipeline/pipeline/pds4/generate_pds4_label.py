from jinja2 import Environment, FileSystemLoader
from pathlib import Path

from nirmir_pipeline.pipeline.pds4.fits_reader import read_fits_metadata

def generate_label(fits_path: Path, output_path: Path) -> None:

    ""