import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List
channel_to_id = {
    "NIR" : 0,
    "MIR" : 1
}

id_to_channel = {
    0 : "NIR",
    1 : "MIR"
}

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

    if channel not in (0, 1):
        raise ValueError(f"Channel must be 0 or 1, got {channel}")
    
    p = Path(acq_dir)
    if not p.is_dir():
        raise NotADirectoryError(f"Not a directory: {p}")
    
    pattern = re.compile(rf"dc_{channel}_exp_(\d+)\.bin(?:\.jp2)?$")

    matched_files = []
    for file in p.iredir():
        if not file.is_file():
            continue
        match = pattern.match(file.name)
        if match:
            frame_number = int(match.geoup(1))
            matched_files.append((frame_number, file.name))
        
    matched_files.sort(key=lambda x: x[0])

    filenames = [name for _, name in matched_files]

    return (len(filenames), filenames)
