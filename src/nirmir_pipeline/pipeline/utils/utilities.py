import re
import subprocess
import numbers
import numpy as np

from datetime import datetime, timezone
from pathlib import Path
from astropy.io.fits import Header, PrimaryHDU, ImageHDU, BinTableHDU, HDUList

from nirmir_pipeline.pipeline.utils.classes import Issue, IssueLevel

import logging
logger = logging.getLogger(__name__)

kelvin: float = 273.15

channel_to_id = {
    "NIR" : 0,
    "MIR" : 1
}

id_to_channel = {
    0 : "NIR",
    1 : "MIR"
}

def parse_levels_to_run(levels: list[str]) -> list[str]:
    levels_to_run: list[str] = []
    seen: set[str] = set()

    def add(level: str) -> None:
        if level not in seen:
            levels_to_run.append(level)
            seen.add(level)
    
    if "1" in levels:
        for level in ("1A", "1B", "1C"):
            add(level)
    for level in levels:
        if level == "1":
            continue
        if level.startswith("1"):
            add(level)
    
    return levels_to_run

def get_current_utc_time_str():
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')

def extract_frames(filenames: list[str]) -> list[str]:

    pattern = re.compile(r"_exp_(\d+)\.bin")

    frames = []
    for name in filenames:
        match = pattern.search(name)
        if not match:
            continue
        frames.append(match.group(1))
    
    frames.sort(key=int)
    return frames
    
def list_channel_frames(acq_dir: str | Path, channel: str) -> tuple[int, list[str]]:

    channel_id = channel_to_id[channel]
    if channel_id not in (0, 1):
        raise ValueError(f"Channel must be 0 or 1, got {channel_id}")
    
    p = Path(acq_dir)
    if not p.is_dir():
        raise NotADirectoryError(f"Not a directory: {p}")
    
    pattern = re.compile(rf"dc_{channel_id}_exp_(\d+)\.bin(?:\.jp2)?$")

    matched_files = []
    for file in p.iterdir():
        if not file.is_file():
            continue
        match = pattern.match(file.name)
        if match:
            frame_number = int(match.group(1))
            matched_files.append((frame_number, file.name))
        
    matched_files.sort(key=lambda x: x[0])

    filenames = [name for _, name in matched_files]

    return (len(filenames), filenames)

def decompress_jp2(input_path: str | Path, output_dir: str | Path) -> Path:
    """
    Decompress a JPEG2000 .jp2 image using the C-based './decompress' program.

    Parameters:
        input_path (str | Path): Path object to the input .jp2 file.
        output_dir (str | Path): Directory to store the output .bin file.
    
    Returns:
        Path (Path): Path object to the decompressed .bin file.
    """
    input_path = Path(input_path)
    output_dir = Path(output_dir)

    output_dir.mkdir(parents=True, exist_ok=True) # Cretate the output directory if does not exist
    if input_path.suffix == '.jp2':
        output_filename = input_path.stem 
    else:
        raise ValueError("Input file does not end with .jp2")

    output_path = Path(output_dir) / output_filename 

    # Resolve absolute path to decompress binary (relative to this script's location)
    project_root = Path(__file__).resolve().parents[4]
    level0_dir = project_root / "src" / "nirmir_pipeline" / "pipeline" /  "levels" / "level_0"

    decompress_path = level0_dir / "decompress"

    with open(input_path, "rb") as f_in, open(output_path, "wb") as f_out:
        subprocess.run([str(decompress_path)], stdin=f_in, stdout=f_out, stderr=subprocess.DEVNULL, check=True)
    
    return output_path

def sc_clock_to_base32(sc_seconds: int, offset: int = 0) -> str:
    """
    Convert a spacecraft time (in seconds) to an unique 6 character image number, increasing by the acquisition time.
    It generated as the base 32 coding of the image capture SC clock seconds. In case of clock counter restart, 
    the time of the restart event will be added to result a continuously increasing number.

    Parameters: 
        sc_seconds (int): The spacecraft clock time in seconds.
        offset (int): Offset to add to clock time (e.g. after clock reset). Default is 0.

    Returns:
        str: A 6-character base-32 string representing the unique image number
    """

    alphabet = '0123456789ABCDEFGHIJKLMNOPQRSTUV'
    base = 32
    value = sc_seconds + offset
    if value < 0:
        raise ValueError("Clock time + offset must be non-negative")
    result = ''
    for _ in range(6):
        result = alphabet[value % base] + result
        value //= base
    
    return result


def form_fits_name(channel: str, image_number: str, utc_time: str, calib_lvl: str) -> str:
    try:
        utc_format = datetime.strptime(utc_time, "%Y-%m-%dT%H:%M:%S.%f").strftime("%y%m%dT%H%M%S")
    except Exception as e:
        utc_format = 'XXXXXXXXXXXXX'
    if image_number == '':
        image_number = 'XXXXXX'
    file_name = f'{channel}_{image_number}_{utc_format}_{calib_lvl}.fits'
    return file_name

def form_fits_header_val(key: str, value: str, comment: str, hierarch: bool) -> tuple[str, str]:
    try: 
        key = str(key)
        value = str(value)
        comment = str(comment)
        k = f"HIERARCH {key}" if hierarch else key
        card_length = len(k) + len(value) + len(comment) + 7
        if card_length <= 80:
            return (value, comment)
        else: 
            return (value, "")
    except Exception as e:
        raise ValueError(f"Fits header value and comment should be stings. (key: {type(key)}, value: {type(value)}, comment: {type(comment)})") from e

def log_issue(issue: Issue) -> None:
    issue_logger = logging.getLogger(issue.source)
    if issue.level == "info":
        issue_logger.info(issue.message)
    elif issue.level == "warning":
        issue_logger.warning(issue.message)
    elif issue.level == "error":
        issue_logger.warning(issue.message)

def fits_in_dir(folder: Path) -> list[Path]:
    files = []
    for pat in ("*.fits", "*.fit", "*fts"):
        files.extend(folder.glob(pat))
    return sorted(set(files))

def convert_to_float64(hdul: HDUList, index: int = 0) -> tuple[HDUList, Issue]:
    # Replace the .data with a float64
    hdu = hdul[index]
    if hdu.data is not None and np.issubdtype(hdu.data.dtype, np.number):
        hdu.data = hdu.data.astype(np.float64)
        if 'BITPIX' in hdu.header:
            hdu.header['BITPIX'] = -64
        issue = Issue(
            level='info',
            message='HDU data converted to float64',
            source=__name__,
        )
    else:
        issue = Issue(
            level='warning',
            message='HDU data is None or not convertable to float64',
            source=__name__,
        )
    return hdul, issue

def convert_to_float32(hdul: HDUList, index: int = 0) -> tuple[HDUList, Issue]:
    # Replace the data with a float32 
    hdu = hdul[index]
    if hdu.data is not None and np.issubdtype(hdu.data.dtype, np.number):
        hdu.data = hdu.data.astype(np.float32)
        if 'BITPIX' in hdu.header:
            hdu.header['BITPIX'] = -32
        issue = Issue(
            level='info',
            message='HDU data converted to float32',
            source=__name__,
        )
    else:
        issue = Issue(
            level='warning',
            message='HDU data is None or not convertable to float32',
            source=__name__,
        )
    return hdul, issue








